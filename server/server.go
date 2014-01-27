package server

import (
	"errors"
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
    "bytes"
    "encoding/json"
    "time"
)

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *sql.SQL
	client     *transport.Client
	cluster    *Cluster
    raftServer raft.Server
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
		cluster: NewCluster(path, cs),
	}
    s.name = listen

	return s, nil
}

// Starts the server.
func (s *Server) ListenAndServe(leader string) error {
    var err error

    log.Printf("Initializing Raft Server: %s", s.path)

    // Initialize and start Raft server.
    transporter := raft.NewHTTPTransporter("/raft")
    transporter.Transport.Dial = transport.UnixDialer
    s.raftServer, err = raft.NewServer(s.name, s.path, transporter, nil, s.sql, "")
    if err != nil {
        log.Fatal(err)
    }
    transporter.Install(s.raftServer, s)
    s.raftServer.SetTransporter(transporter)
    s.raftServer.Start()

    log.Println("<<<%s>>>", s.raftServer.LogPath())
    if leader != "" {
        // Join to leader if specified.

        log.Println("Attempting to join leader:", leader)

        if !s.raftServer.IsLogEmpty() {
            log.Fatal("Cannot join with an existing log")
        }
        if err := s.Join(leader); err != nil {
            log.Fatal(err)
        }
        log.Printf("Joined to leader: ", leader)

    } else if s.raftServer.IsLogEmpty() {
        // Initialize the server by joining itself.

        log.Println("Initializing new cluster")

        cs, err := transport.Encode(s.path + "/" + s.listen)

        _, err = s.raftServer.Do(&raft.DefaultJoinCommand{
            Name:             s.raftServer.Name(),
            ConnectionString: cs,
        })
        if err != nil {
            log.Fatal(err)
        }
        log.Printf("Created a leader: ", leader)

    } else {
        log.Println("Recovered from log")
    }

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Handler: s.router,
	}

	s.router.HandleFunc("/sql", s.sqlHandler).Methods("POST")
	s.router.HandleFunc("/healthcheck", s.healthcheckHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	// Start Unix transport
	l, err := transport.Listen(s.listen)
	if err != nil {
		log.Fatal(err)
	}
	return s.httpServer.Serve(l)
}

func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
    s.router.HandleFunc(pattern, handler)
}

// Client operations

// Join an existing cluster
func (s *Server) Join(leader string) error {
    cs, _ := transport.Encode(s.path + "/" + s.listen)
    command := &raft.DefaultJoinCommand{
        Name:             s.raftServer.Name(),
        ConnectionString: /*s.path+ "/" +*/ cs,
    }
    var b bytes.Buffer
    json.NewEncoder(&b).Encode(command)


	for {
        log.Printf(">>>>>>>>>>INITIATING JOIN REQUEST to %s from %s<<<<<<<<<<,", leader, s.listen)
        cs, err := transport.Encode(leader)
        if err != nil {
            log.Printf("Error, transport.encode")
            return err
        }
		_, err = s.client.SafePost(cs, "/join", &b)
		if err != nil {
			log.Printf("Unable to join cluster: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
        log.Printf("Member count: ", s.raftServer.MemberCount())
        return nil
	}
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
    command := &raft.DefaultJoinCommand{}

    log.Printf("Handling join request: %#v", req.Body)
    if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    log.Printf("Handling join request: %#v", command.ConnectionString)
    if _, err := s.raftServer.Do(command); err != nil {
        log.Printf("%s err", err)
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    log.Printf("%s count", s.raftServer.MemberCount())
	b := util.JSONEncode("")
    w.Write(b.Bytes())

/*
	j := &Join{}
	if err := util.JSONDecode(req.Body, j); err != nil {
		log.Printf("Invalid join request: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Handling join request: %#v", j)

	// Add node to the cluster
	if err := s.cluster.AddMember(j.Self); err != nil {
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
    log.Printf("Received an sql query")
    if s.raftServer.MemberCount() < 3 {
            http.Error(w, "not ready yet", http.StatusBadRequest)
        return
    }
	state := s.raftServer.State()

    leaderPeer, ok := s.raftServer.Peers()[s.raftServer.Leader()]

    if ok {
        log.Printf("Error while getting leader")
            http.Error(w, "not ready yet", http.StatusBadRequest)
        return
    }

	if state != "leader" && leaderPeer != nil {
        cs, err := transport.Encode(leaderPeer.ConnectionString)
        if err != nil || cs == ""  {
            log.Printf("Error, transport.encode")
            http.Error(w, "sorry", http.StatusBadRequest)
            return
        }
        buf := new(bytes.Buffer)
        log.Printf(cs)
        body, err := s.client.SafePost(cs, "/sql", req.Body)
        if body != nil {
            buf.ReadFrom(body)
            s := buf.String()
            byteArray := []byte(s)
            w.Write(byteArray)
        } else {
            http.Error(w, "sorry", http.StatusBadRequest)
        }
		return
	}

	query, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Couldn't read body: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Debugf("[%s] Received query: %#v", s.cluster.State(), string(query))
    output_t, err := s.raftServer.Do(command.NewWriteCommand(query))


    log.Printf(">>>>>>>>>>> %s %s", err, s.raftServer.State())

    if output_t == nil {
        http.Error(w, "sorry", http.StatusBadRequest)
        return
    }
    output := output_t.(*sql.Output)
    formatted := fmt.Sprintf("SequenceNumber: %d\n%s",
    output.SequenceNumber, output.Stdout)

	w.Write([]byte(formatted))
    /*
	resp, err := s.execute(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	log.Debugf("[%s] Returning response to %#v: %#v", s.cluster.State(), string(query), string(resp))
	w.Write(resp)
    */
}

func (s *Server) healthcheckHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *Server) execute(query []byte) ([]byte, error) {
	output, err := s.sql.Execute(string(query))

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
