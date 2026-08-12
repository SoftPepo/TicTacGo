package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/coder/websocket"
)

type Client struct {
	conn  *websocket.Conn
	send  chan string
	nick  string
	hub   *Hub
	board *Board
}

type Hub struct {
	clients    map[*Client]bool
	boards     map[string]*Board
	register   chan *Client
	unregister chan *Client
	command    chan Command
	closeBoard chan *Board
}

type Board struct {
	playerA   *Client
	playerB   *Client
	name      string
	password  string
	gameState GameState
	command   chan GameCommand
	done      chan struct{}
}

type GameState struct {
	boardState    [9]Cell
	currentPlayer string
	status        Status
	result        Result
}

type Command struct {
	kind     CommandKind
	client   *Client
	name     string
	password string
	reply    chan Response
}

type CommandKind string

const (
	Create CommandKind = "Create"
	Join   CommandKind = "Join"
)

type Response struct {
	ok    bool
	code  string
	msg   string
	board *Board
}

type Ack struct {
	Kind string `json:"kind"`
	Ok   bool   `json:"ok"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

type GameCommand struct {
	kind   GameCommandKind
	client *Client
	cell   int
}

type GameCommandKind string

const (
	Move  GameCommandKind = "Move"
	Leave GameCommandKind = "Leave"
)

type Status string

const (
	Waiting  Status = "Waiting"
	Ongoing  Status = "Ongoing"
	Finished Status = "Finished"
	Aborted  Status = "Aborted"
)

type Result string

const (
	A         Result = "A"
	B         Result = "B"
	Stalemate Result = "Stalemate"
	None      Result = "None"
)

type Cell string

const (
	Empty   Cell = "E"
	PlayerA Cell = "A"
	PlayerB Cell = "B"
)

type ClientMsg struct {
	Kind     string `json:"kind"`
	Name     string `json:"name"`
	Password string `json:"password"`
	Cell     int    `json:"cell"`
}

func (c *Client) readPump(ctx context.Context) {
	defer func() { c.hub.unregister <- c }()
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		var msg ClientMsg
		err = json.Unmarshal(data, &msg)
		fmt.Println(msg.Name)
	}
}

func (c *Client) writePump(ctx context.Context) {
	defer c.conn.Close(websocket.StatusNormalClosure, "")
	for msg := range c.send {
		err := c.conn.Write(ctx, websocket.MessageText, []byte(msg))
		if err != nil {
			return
		}
	}
}

func handleWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ctx := context.Background()

	c := &Client{
		conn: conn,
		send: make(chan string),
		nick: "Anon",
		hub:  hub,
	}

	c.readPump(ctx)
}

func main() {
	hub := &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		command:    make(chan Command),
		closeBoard: make(chan *Board),
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWS(hub, w, r)
	})
	http.ListenAndServe(":8080", nil)
}
