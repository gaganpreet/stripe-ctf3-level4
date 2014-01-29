package server

import (
//	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/sql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"stripe-ctf.com/sqlcluster/command"
	"github.com/goraft/raft"
	"time"
    "math/rand"
    "bytes"
    "encoding/json"
    "sync"
    "strings"
)

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
    raftServer  raft.Server
    cs          string
    mutex       sync.RWMutex
}

// Creates a new server.
func New(path, listen string) (*Server, error) {
	cs, err := transport.Encode(listen)
	if err != nil {
		return nil, err
	}

	sqlPath := filepath.Join(path, "storage.sql")
	util.EnsureAbsent(sqlPath)

	s := &Server{
		path:    path,
		listen:  listen,
		sql:     sql.NewSQL(sqlPath),
		router:  mux.NewRouter(),
		client:  transport.NewClient(),
        cs:      cs,
	}
    s.name = fmt.Sprintf("%s-%07x", listen, rand.Int())[0:7]

	return s, nil
}

func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
    s.router.HandleFunc(pattern, handler)
}

func (s *Server) connectionString() string {
    cs, _ := transport.Encode(s.path + "/" + s.listen)
    return cs
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
	var err error
	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}

    transporter := raft.NewHTTPTransporter("/raft")
    transporter.Transport.Dial = transport.UnixDialer
    s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
    if err != nil {
        log.Fatal(err)
    }
    transporter.Install(s.raftServer, s)
    s.raftServer.Start()
    s.raftServer.SetElectionTimeout(1 * time.Second)
    s.raftServer.SetHeartbeatTimeout(time.Millisecond * 300)

    if leader != "" {
        // Join to leader if specified.

        log.Println("Attempting to join leader:", leader)

        if !s.raftServer.IsLogEmpty() {
            log.Fatal("Cannot join with an existing log")
        }
        if err := s.Join(leader); err != nil {
            log.Fatal(err)
        }

    } else if s.raftServer.IsLogEmpty() {
        // Initialize the server by joining itself.

        log.Println("Initializing new cluster")

        _, err := s.raftServer.Do(&raft.DefaultJoinCommand{
            Name: s.raftServer.Name(),
            ConnectionString: s.connectionString(),
        })
        if err != nil {
            log.Fatal(err)
        }

    } else {
        log.Println("Recovered from log")
    }


    // Initialize and start HTTP server.
    s.httpServer = &http.Server{
        Handler: s.router,
    }


	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	//s.router.HandleFunc("/replicate", s.replicationHandler).Methods("POST")
	//s.router.HandleFunc("/healthcheck", s.healthcheckHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	return s.httpServer.Serve(l)
}

// Client operations

// Join an existing cluster
func (s *Server) Join(leader string) error {
    time.Sleep(5 * time.Second)
    tried := 0
    for {
        command := &raft.DefaultJoinCommand{
            Name: s.raftServer.Name(),
            ConnectionString: s.connectionString(),
        }

        var b bytes.Buffer
        json.NewEncoder(&b).Encode(command)
        cs, err := transport.Encode(leader)
        if err != nil {
            log.Printf("Unable to encode transport %s", leader)
        }

        log.Printf("Contacting leader %s", cs)
        body, err := s.client.SafePost(cs, "/join", &b)

        if err != nil || body == nil {
            log.Printf("Couldn't join cluster %s %s", err, body)
            if tried == 20 {
                break
            }
            time.Sleep(200 * time.Millisecond)
            continue
        }
        log.Printf("Got this while joining: %v", body)
        log.Printf("Member count: %d", s.raftServer.MemberCount())
        return nil
    }
    log.Printf("Giving up")
    return nil
/*
    resp.Body.Close()
    if err != nil {
        return err
    }

    return nil

	cs, err := transport.Encode(primary)
	if err != nil {
		return err
	}

	for {
		body, err := s.client.SafePost(cs, "/join", b)
		if err != nil {
			log.Printf("Unable to join cluster: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		resp := &JoinResponse{}
		if err = util.JSONDecode(body, &resp); err != nil {
			return err
		}

		s.cluster.Join(resp.Self, resp.Members)
		return nil
	}
*/
}

func (s *Server) forwardRequest(w http.ResponseWriter, req *http.Request) {
    log.Printf("Received a query for forwarding")
    for {
        leaderPeer, _ := s.raftServer.Peers()[s.raftServer.Leader()]
        log.Printf("Leader peer: %s", leaderPeer)
        if leaderPeer == nil {
            log.Printf("Leader peer continuing", leaderPeer)
            time.Sleep(1 * time.Second)
            continue
        }
        cs := leaderPeer.ConnectionString

        log.Printf("Forwarding cs: ", cs)
        buf := new(bytes.Buffer)
        body, err := s.client.SafePost(cs, "/sql", req.Body)
        if body != nil {
            buf.ReadFrom(body)
            s := buf.String()
            byteArray := []byte(s)
            w.Write(byteArray)
            return
        }
        if body == nil || err != nil {
            http.Error(w, "something went wrong", http.StatusBadRequest)
        }
    }
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    state := s.raftServer.State()
    command := &raft.DefaultJoinCommand{}

    log.Printf("Received join request, my state is: %s", state)
    /*
    if state != "leader" {
        s.forwardRequest(w, req)
        return
    }
    */


    if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		log.Printf("Invalid join request: %s", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    log.Printf("Received join request from %s with cs %s. My current state is: %s", command.Name, command.ConnectionString, state)
    t, err := s.raftServer.Do(command)
    if err != nil {
		log.Printf("Tried to add to cluster, but got this: %s", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    log.Printf("While adding to cluster, got this; %s", t)

    log.Printf("Member count: %d", s.raftServer.MemberCount())
    b := util.JSONEncode("in")
    w.Write(b.Bytes())


/*
	j := &Join{}
	if err := util.JSONDecode(req.Body, j); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling join request: %#v", j)

	// Add node to the cluster if err := s.cluster.AddMember(j.Self); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Respond with the current cluster description
	resp := &JoinResponse{
		s.cluster.self,
		s.cluster.members,
	}
	b := util.JSONEncode(resp)
	w.Write(b.Bytes())
*/
}

// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
    log.Printf("Hello world")
    for {
        log.Printf("Leader is: ", s.raftServer.MemberCount(), s.raftServer.Leader())
        if s.raftServer.Leader() == "" {
            log.Printf("Sleeping: %d", s.raftServer.State())
            time.Sleep(1 * time.Second)
            continue
        }
        break
    }

    log.Printf("Leader is: %d %s", s.raftServer.MemberCount(), s.raftServer.Leader())
    state := s.raftServer.State()
    log.Printf("state %s", state)
    query, _ := ioutil.ReadAll(req.Body)

    filename := util.Sha1(string(query))
    filename = s.sql.Path + filename

    log.Printf("filename: ", filename)
    if util.Exists(filename) == true {
        contents, _ := ioutil.ReadFile(filename)
        if len(contents) > 0 && strings.Contains(string(contents), "no such table") == false {
            // log.Printf("returning for %s -> %s", query, string(contents))
            w.Write([]byte(contents))
            return
        }
    }
    if state == "leader" {
        log.Printf("Starting query: ", string(query))
        output_t, _ := s.raftServer.Do(command.NewWriteCommand(string(query)))

        if output_t == nil {
            http.Error(w, "something went wrong", http.StatusBadRequest)
            return
        }

        formatted := output_t.(string)
        log.Printf("Received output: ", formatted)

        w.Write([]byte(formatted))
        return
    } else {
        for {
            log.Printf("Forwarding")
            leader := s.raftServer.Leader()
            cs, err := transport.Encode(leader + ".sock")
            body, err := s.client.SafePost(cs, "/sql", bytes.NewReader(query))
            if err != nil {
                http.Error(w, " went wrong", http.StatusBadRequest)
                time.Sleep(1000 * time.Millisecond)
                return
                continue
            } else {
                r, _ := ioutil.ReadAll(body)
            //    log.Printf("Writing ", r)
                w.Write([]byte(r))
                return
            }
        }
        return
    }
    /*else {
        leader := s.raftServer.Peers()[s.raftServer.Leader()]
        log.Printf("Trying to forward to %s", leader)
        http.Error(w, "a", http.StatusBadRequest)
        return
    }
        log.Printf("Leader is: ", s.raftServer.MemberCount(), s.raftServer.Leader())
        http.Error(w, "a", http.StatusInternalServerError)
        return
    log.Printf("Received an sql query")
    log.Printf("Member count: %d %s", s.raftServer.MemberCount(), s.raftServer.Peers()[s.raftServer.Leader()])

    if state != "leader" {
        log.Printf("Forwarded an sql query")
    //    s.forwardRequest(w, req)
        http.Error(w, " went wrong", http.StatusBadRequest)
		return
    }


    */
    /*
	state := s.cluster.State()
	if state != "primary" {
		http.Error(w, "Only the primary can service queries, but this is a "+state, http.StatusBadRequest)
		return
	}

	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Debugf("[%s] Received query: %#v", s.cluster.State(), string(query))
	resp, err := s.execute(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	r := &Replicate{
		Self:  s.cluster.self,
		Query: query,
	}
	for _, member := range s.cluster.members {
		b := util.JSONEncode(r)
		_, err := s.client.SafePost(member.ConnectionString, "/replicate", b)
		if err != nil {
			log.Printf("Couldn't replicate query to %v: %s", member, err)
		}
	}

	log.Debugf("[%s] Returning response to %#v: %#v", s.cluster.State(), string(query), string(resp))
	w.Write(resp)
    */
}

/*
func (s *Server) replicationHandler(w http.ResponseWriter, req *http.Request) {
	r := &Replicate{}
	if err := util.JSONDecode(req.Body, r); err != nil {
		log.Printf("Invalid replication request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling replication request from %v", r.Self)

	_, err := s.execute(r.Query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	resp := &ReplicateResponse{
		s.cluster.self,
	}
	b := util.JSONEncode(resp)
	w.Write(b.Bytes())
}

func (s *Server) healthcheckHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) execute(query []byte) ([]byte, error) {
	output, err := s.sql.Execute(s.cluster.State(), string(query))

	if err != nil {
		var msg string
		if output != nil && len(output.Stderr) > 0 {
			template := `Error executing %#v (%s)

SQLite error: %s`
			msg = fmt.Sprintf(template, query, err.Error(), util.FmtOutput(output.Stderr))
		} else {
			msg = err.Error()
		}

		return nil, errors.New(msg)
	}

	formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
		output.SequenceNumber, output.Stdout)
	return []byte(formatted), nil
}
*/
