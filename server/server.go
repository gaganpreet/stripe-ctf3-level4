package server

import (
//	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"stripe-ctf.com/sqlcluster/log"
	"stripe-ctf.com/sqlcluster/minesql"
	"stripe-ctf.com/sqlcluster/transport"
	"stripe-ctf.com/sqlcluster/util"
	"stripe-ctf.com/sqlcluster/command"
	"github.com/goraft/raft"
	"time"
    "math/rand"
    "bytes"
    "encoding/json"
    "sync"
)

type Server struct {
	name       string
	path       string
	listen     string
	router     *mux.Router
	httpServer *http.Server
	sql        *minesql.MineSQL
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
		sql:     minesql.NewSQL(sqlPath),
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
    cs, _ := transport.Encode(s.listen)
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
    s.raftServer.SetHeartbeatTimeout(time.Millisecond * 50)

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
        Addr: s.listen,
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
    tried := 0
    for {
        command := &raft.DefaultJoinCommand{
            Name: s.raftServer.Name(),
            ConnectionString: s.connectionString(),
        }

        var b bytes.Buffer
        json.NewEncoder(&b).Encode(command)
        cs, err := transport.Encode(leader)

        body, err := s.client.SafePost(cs, "/join", &b)

        if err != nil || body == nil {
            if tried == 20 {
                break
            }
            time.Sleep(200 * time.Millisecond)
            continue
        }
        return nil
    }
    return nil
}

// Server handlers
func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
    s.mutex.Lock()
    defer s.mutex.Unlock()
    command := &raft.DefaultJoinCommand{}

    if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    _, err := s.raftServer.Do(command)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }

    b := util.JSONEncode("in")
    w.Write(b.Bytes())
}

// This is the only user-facing function, and accordingly the body is
// a raw string rather than JSON.
func (s *Server) sqlHandler(w http.ResponseWriter, req *http.Request) {
    if s.raftServer.Leader() == "" {
        for {
            if s.raftServer.Leader() == "" {
                time.Sleep(20 * time.Millisecond)
                continue
            }
            break
        }
    }

    state := s.raftServer.State()
    query, _ := ioutil.ReadAll(req.Body)

    if state == "leader" {
        //log.Printf("Starting query: ", string(query))
        output_t, _ := s.raftServer.Do(command.NewWriteCommand(string(query)))

        if output_t == nil {
            http.Error(w, "something went wrong", http.StatusBadRequest)
            return
        }

        formatted := output_t.(string)
        //log.Printf("Received output: ", formatted)

        w.Write([]byte(formatted))
        return
    } else {
        go func() {
            leader := s.raftServer.Leader()
            cs, _ := transport.Encode(leader + ".sock")
            _, _ = s.client.SafePost(cs, "/sql", bytes.NewReader(query))
        }()

        for {
            cached_val, ok := s.sql.Cache[string(query)]
            if ok {
                w.Write([]byte(cached_val))
                return
            }
            time.Sleep(1 * time.Millisecond)
        }
        return
    }
}
