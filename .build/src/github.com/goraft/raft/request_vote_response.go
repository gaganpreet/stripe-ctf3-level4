package raft

import (
	"io"
	"io/ioutil"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/goraft/raft/protobuf"
)

// The response returned from a server after a vote for a candidate to become a leader.
type RequestVoteResponse struct {
	peer        *Peer
	Term        uint64
	VoteGranted bool
}

// Creates a new RequestVote response.
func newRequestVoteResponse(term uint64, voteGranted bool) *RequestVoteResponse {
	return &RequestVoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
	}
}

// Encodes the RequestVoteResponse to a buffer. Returns the number of bytes
// written and any error that may have occurred.
func (resp *RequestVoteResponse) Encode(w io.Writer) (int, error) {
	pb := &protobuf.RequestVoteResponse{
		Term:        proto.Uint64(resp.Term),
		VoteGranted: proto.Bool(resp.VoteGranted),
	}

	p, err := proto.Marshal(pb)
	if err != nil {
		return -1, err
	}

	return w.Write(p)
}

// Decodes the RequestVoteResponse from a buffer. Returns the number of bytes read and
// any error that occurs.
func (resp *RequestVoteResponse) Decode(r io.Reader) (int, error) {
	data, err := ioutil.ReadAll(r)

	if err != nil {
		return 0, err
	}

	totalBytes := len(data)

	pb := &protobuf.RequestVoteResponse{}
	if err = proto.Unmarshal(data, pb); err != nil {
		return -1, err
	}

	resp.Term = pb.GetTerm()
	resp.VoteGranted = pb.GetVoteGranted()

	return totalBytes, nil
}
