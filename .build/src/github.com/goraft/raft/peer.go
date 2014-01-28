package raft

import (
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A peer is a reference to another server involved in the consensus protocol.
type Peer struct {
	server           *server
	Name             string `json:"name"`
	ConnectionString string `json:"connectionString"`
	prevLogIndex     uint64
	mutex            sync.RWMutex
	stopChan         chan bool
	heartbeatTimeout time.Duration
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new peer.
func newPeer(server *server, name string, connectionString string, heartbeatTimeout time.Duration) *Peer {
	return &Peer{
		server:           server,
		Name:             name,
		ConnectionString: connectionString,
		heartbeatTimeout: heartbeatTimeout,
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

// Sets the heartbeat timeout.
func (p *Peer) setHeartbeatTimeout(duration time.Duration) {
	p.heartbeatTimeout = duration
}

//--------------------------------------
// Prev log index
//--------------------------------------

// Retrieves the previous log index.
func (p *Peer) getPrevLogIndex() uint64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.prevLogIndex
}

// Sets the previous log index.
func (p *Peer) setPrevLogIndex(value uint64) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.prevLogIndex = value
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Heartbeat
//--------------------------------------

// Starts the peer heartbeat.
func (p *Peer) startHeartbeat() {
	p.stopChan = make(chan bool)
	c := make(chan bool)
	go p.heartbeat(c)
	<-c
}

// Stops the peer heartbeat.
func (p *Peer) stopHeartbeat(flush bool) {
	p.stopChan <- flush
}

//--------------------------------------
// Copying
//--------------------------------------

// Clones the state of the peer. The clone is not attached to a server and
// the heartbeat timer will not exist.
func (p *Peer) clone() *Peer {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return &Peer{
		Name:             p.Name,
		ConnectionString: p.ConnectionString,
		prevLogIndex:     p.prevLogIndex,
	}
}

//--------------------------------------
// Heartbeat
//--------------------------------------

// Listens to the heartbeat timeout and flushes an AppendEntries RPC.
func (p *Peer) heartbeat(c chan bool) {
	stopChan := p.stopChan

	c <- true

	ticker := time.Tick(p.heartbeatTimeout)

	debugln("peer.heartbeat: ", p.Name, p.heartbeatTimeout)

	for {
		select {
		case flush := <-stopChan:
			if !flush {
				debugln("peer.heartbeat.stop: ", p.Name)
				return
			} else {
				// before we can safely remove a node
				// we must flush the remove command to the node first
				p.flush()
				debugln("peer.heartbeat.stop.with.flush: ", p.Name)
				return
			}

		case <-ticker:
			start := time.Now()
			p.flush()
			duration := time.Now().Sub(start)
			p.server.DispatchEvent(newEvent(HeartbeatEventType, duration, nil))
		}
	}
}

func (p *Peer) flush() {
	debugln("peer.heartbeat.flush: ", p.Name)
	prevLogIndex := p.getPrevLogIndex()
	entries, prevLogTerm := p.server.log.getEntriesAfter(prevLogIndex, p.server.maxLogEntriesPerRequest)

	if p.server.State() != Leader {
		return
	}

	if entries != nil {
		p.sendAppendEntriesRequest(newAppendEntriesRequest(p.server.currentTerm, prevLogIndex, prevLogTerm, p.server.log.CommitIndex(), p.server.name, entries))
	} else {
		p.sendSnapshotRequest(newSnapshotRequest(p.server.name, p.server.lastSnapshot))
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Sends an AppendEntries request to the peer through the transport.
func (p *Peer) sendAppendEntriesRequest(req *AppendEntriesRequest) {
	tracef("peer.append.send: %s->%s [prevLog:%v length: %v]\n",
		p.server.Name(), p.Name, req.PrevLogIndex, len(req.Entries))

	resp := p.server.Transporter().SendAppendEntriesRequest(p.server, p, req)
	if resp == nil {
		p.server.DispatchEvent(newEvent(HeartbeatTimeoutEventType, p, nil))
		debugln("peer.append.timeout: ", p.server.Name(), "->", p.Name)
		return
	}
	traceln("peer.append.resp: ", p.server.Name(), "<-", p.Name)

	// If successful then update the previous log index.
	p.mutex.Lock()
	if resp.Success() {
		if len(req.Entries) > 0 {
			p.prevLogIndex = req.Entries[len(req.Entries)-1].GetIndex()

			// if peer append a log entry from the current term
			// we set append to true
			if req.Entries[len(req.Entries)-1].GetTerm() == p.server.currentTerm {
				resp.append = true
			}
		}
		traceln("peer.append.resp.success: ", p.Name, "; idx =", p.prevLogIndex)
		// If it was unsuccessful then decrement the previous log index and
		// we'll try again next time.
	} else {
		if resp.Term() > p.server.Term() || resp.CommitIndex() > p.server.CommitIndex() {
			// this happens when there is a new leader comes up that this *leader* has not
			// known yet.
			// this server can know until the new leader send a ae with higher term
			// or this server finish processing this response.
			debugln("peer.append.resp.not.update: new.leader.found")
		} else if resp.CommitIndex() >= p.prevLogIndex {
			// we may miss a response from peer
			// so maybe the peer has committed the logs we just sent
			// but we did not receive the successful reply and did not increase
			// the prevLogIndex

			// peer failed to truncate the log and sent a fail reply at this time
			// we just need to update peer's prevLog index to commitIndex

			p.prevLogIndex = resp.CommitIndex()
			debugln("peer.append.resp.update: ", p.Name, "; idx =", p.prevLogIndex)

		} else if p.prevLogIndex > 0 {
			// Decrement the previous log index down until we find a match. Don't
			// let it go below where the peer's commit index is though. That's a
			// problem.
			p.prevLogIndex--
			// if it not enough, we directly decrease to the index of the
			if p.prevLogIndex > resp.Index() {
				p.prevLogIndex = resp.Index()
			}

			debugln("peer.append.resp.decrement: ", p.Name, "; idx =", p.prevLogIndex)
		}
	}
	p.mutex.Unlock()

	// Attach the peer to resp, thus server can know where it comes from
	resp.peer = p.Name
	// Send response to server for processing.
	p.server.sendAsync(resp)
}

// Sends an Snapshot request to the peer through the transport.
func (p *Peer) sendSnapshotRequest(req *SnapshotRequest) {
	debugln("peer.snap.send: ", p.Name)

	resp := p.server.Transporter().SendSnapshotRequest(p.server, p, req)
	if resp == nil {
		debugln("peer.snap.timeout: ", p.Name)
		return
	}

	debugln("peer.snap.recv: ", p.Name)

	// If successful, the peer should have been to snapshot state
	// Send it the snapshot!
	if resp.Success {
		p.sendSnapshotRecoveryRequest()
	} else {
		debugln("peer.snap.failed: ", p.Name)
		return
	}

}

// Sends an Snapshot Recovery request to the peer through the transport.
func (p *Peer) sendSnapshotRecoveryRequest() {
	req := newSnapshotRecoveryRequest(p.server.name, p.server.lastSnapshot)
	debugln("peer.snap.recovery.send: ", p.Name)
	resp := p.server.Transporter().SendSnapshotRecoveryRequest(p.server, p, req)

	if resp == nil {
		debugln("peer.snap.recovery.timeout: ", p.Name)
		return
	}

	if resp.Success {
		p.prevLogIndex = req.LastIndex
	} else {
		debugln("peer.snap.recovery.failed: ", p.Name)
		return
	}

	p.server.sendAsync(resp)
}

//--------------------------------------
// Vote Requests
//--------------------------------------

// send VoteRequest Request
func (p *Peer) sendVoteRequest(req *RequestVoteRequest, c chan *RequestVoteResponse) {
	debugln("peer.vote: ", p.server.Name(), "->", p.Name)
	req.peer = p
	if resp := p.server.Transporter().SendVoteRequest(p.server, p, req); resp != nil {
		debugln("peer.vote.recv: ", p.server.Name(), "<-", p.Name)
		resp.peer = p
		c <- resp
	} else {
		debugln("peer.vote.failed: ", p.server.Name(), "<-", p.Name)
	}
}
