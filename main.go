package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/coder/websocket"
)

type Client struct {
	conn  *websocket.Conn
	send  chan []byte
	nick  string
	hub   *Hub
	board *Board
}

type Hub struct {
	clients map[*Client]bool
	boards  map[string]*Board
	command chan Command
}

type Board struct {
	players   [2]*Client
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
	board    *Board
	name     string
	password string
	reply    chan Response
}

type CommandKind string

const (
	Register   CommandKind = "Register"
	Unregister CommandKind = "Unregister"
	Create     CommandKind = "Create"
	Join       CommandKind = "Join"
	CloseBoard CommandKind = "CloseBoard"
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
	Nick     string `json:"nick"`
	RoomName string `json:"room_name"`
	Password string `json:"password"`
	Cell     int    `json:"cell"`
}

func (c *Client) readPump(ctx context.Context) {
	defer func() { c.hub.command <- Command{kind: Unregister, client: c} }()
	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}
		var msg ClientMsg
		err = json.Unmarshal(data, &msg)
		slog.Info("Message received", "Kind", msg.Kind)
		switch msg.Kind {
		case "Create", "Join":
			fmt.Println("Lmao")
			reply := make(chan Response, 1)
			command := Command{kind: CommandKind(msg.Kind), client: c, name: msg.RoomName, password: msg.Password, reply: reply}
			c.hub.command <- command
			resp := <-reply
			if !resp.ok {
				ack, _ := json.Marshal(Ack{Kind: "Ack", Ok: false, Code: resp.code, Msg: resp.msg})
				c.send <- ack
			}
		}
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

func (h *Hub) run() {
	for {
		cmd := <-h.command
		switch cmd.kind {
		case Register:
			h.clients[cmd.client] = true
			slog.Info("User registered")
		case Unregister:
			delete(h.clients, cmd.client)
		case Create:
			if _, ok := h.boards[cmd.name]; !ok {
				h.boards[cmd.name] = &Board{
					name:     cmd.name,
					password: cmd.password,
					done:     make(chan struct{}),
				}
				h.boards[cmd.name].players[0] = cmd.client
				cmd.reply <- Response{ok: true, board: h.boards[cmd.name]}
				slog.Info("Room created", "name", cmd.name)
			}
		case Join:
			if board, ok := h.boards[cmd.name]; !ok {
				cmd.reply <- Response{ok: false, code: "no_such_room", msg: "This room does not exist"}
			} else if board.password != cmd.password {
				cmd.reply <- Response{ok: false, code: "wrong_password", msg: "Invalid room password"}
			} else if board.players[1] != nil {
				cmd.reply <- Response{ok: false, code: "room_full", msg: "This room is full"}
			} else {
				board.players[1] = cmd.client
				cmd.reply <- Response{ok: true, board: board}
			}
		case CloseBoard:
			if h.boards[cmd.name] == cmd.board {
				delete(h.boards, cmd.name)
			}
		}
	}
}

func handleWS(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ctx := context.Background()
	var msg ClientMsg
	_, nick, err := conn.Read(ctx)
	err = json.Unmarshal(nick, &msg)
	c := &Client{
		conn: conn,
		send: make(chan []byte, 16),
		nick: msg.Nick,
		hub:  hub,
	}

	c.hub.command <- Command{kind: Register, client: c}
	c.readPump(ctx)
}

func main() {
	hub := &Hub{
		clients: make(map[*Client]bool),
		boards:  make(map[string]*Board),
		command: make(chan Command),
	}

	go hub.run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		handleWS(hub, w, r)
	})
	http.ListenAndServe(":8080", nil)
}
