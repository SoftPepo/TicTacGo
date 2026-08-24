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
	boardState [9]Cell
	currentIdx int
	status     Status
	result     Cell
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
	reply  chan Response
}

type GameCommandKind string

const (
	Move      GameCommandKind = "Move"
	Leave     GameCommandKind = "Leave"
	AddPlayer GameCommandKind = "AddPlayer"
)

type Status string

const (
	Finished Status = "Finished"
	Aborted  Status = "Aborted"
)

type Cell string

const (
	Empty Cell = ""
	X     Cell = "X"
	O     Cell = "O"
)

type ClientMsg struct {
	Kind     string `json:"kind"`
	Nick     string `json:"nick"`
	RoomName string `json:"room_name"`
	Password string `json:"password"`
	Cell     int    `json:"cell"`
}

var winLines = [8][3]int{
	{0, 1, 2}, {3, 4, 5}, {6, 7, 8},
	{0, 3, 6}, {1, 4, 7}, {2, 5, 8},
	{0, 4, 8}, {2, 4, 6},
}

var symbols = [2]Cell{X, O}

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
			reply := make(chan Response, 1)
			cmd := Command{kind: CommandKind(msg.Kind), client: c, name: msg.RoomName, password: msg.Password, reply: reply}
			c.hub.command <- cmd
			resp := <-reply
			if !resp.ok {
				ack, _ := json.Marshal(Ack{Kind: "Ack", Ok: false, Code: resp.code, Msg: resp.msg})
				c.send <- ack
			} else {
				c.board = resp.board
			}
		case "Move":
			if c.board == nil {
				ack, _ := json.Marshal(Ack{Kind: "Ack", Ok: false, Code: "no_board", Msg: "You have not joined a board yet"})
				c.send <- ack
			}
			reply := make(chan Response, 1)
			gamecmd := GameCommand{kind: Move, client: c, cell: msg.Cell, reply: reply}
			c.board.command <- gamecmd
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
		slog.Info("Command received ", "kind", cmd.kind)
		switch cmd.kind {
		case Register:
			h.clients[cmd.client] = true
			slog.Info("User registered")
		case Unregister:
			delete(h.clients, cmd.client)
		case Create:
			if h.playerInGame(cmd.client) {
				cmd.reply <- Response{ok: false, code: "player_in_game", msg: "Player already in game"}
				continue
			}
			if _, ok := h.boards[cmd.name]; !ok {
				h.boards[cmd.name] = &Board{
					name:     cmd.name,
					password: cmd.password,
					command:  make(chan GameCommand, 16),
					done:     make(chan struct{}),
				}
				go h.boards[cmd.name].run()
				h.boards[cmd.name].command <- GameCommand{kind: AddPlayer, client: cmd.client, reply: cmd.reply}
				slog.Info("Room created", "name", cmd.name)
			} else {
				cmd.reply <- Response{ok: false, code: "room_name_exists", msg: "Room with this name already exists"}
			}

		case Join:
			if h.playerInGame(cmd.client) {
				cmd.reply <- Response{ok: false, code: "player_in_game", msg: "Player already in game"}
			}
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

func (h *Hub) playerInGame(c *Client) bool {
	for _, board := range h.boards {
		if board.players[0] == c || board.players[1] == c {
			return true
		}
	}
	return false
}

func (b *Board) run() {
	for {
		cmd := <-b.command
		slog.Info("Received game command ", "kind", cmd.kind)
		switch cmd.kind {
		case "Move":
			if b.players[1] == nil {
				cmd.reply <- Response{ok: false, code: "game_not_started", msg: "Game has not yet started"}
				continue
			}
			if cmd.cell < 0 || cmd.cell > 8 {
				cmd.reply <- Response{ok: false, code: "invalid_cell", msg: "Cell our of range (0-8)"}
				continue
			}
			if b.indexOf(cmd.client) != b.gameState.currentIdx {
				cmd.reply <- Response{ok: false, code: "invalid_turn", msg: "This is not your turn"}
				continue
			}
			if b.gameState.boardState[cmd.cell] != "" {
				cmd.reply <- Response{ok: false, code: "invalid_cell", msg: "This cell is not empty"}
				continue
			}
			slog.Info("Making move", "Current inx", b.gameState.currentIdx, "Symbol", symbols[b.indexOf(cmd.client)])
			b.gameState.boardState[cmd.cell] = symbols[b.indexOf(cmd.client)]
			fmt.Println(b.checkWinner())
			if b.checkWinner() != Empty {
				b.gameState.result = b.checkWinner()
				cmd.reply <- Response{ok: false, code: "win", msg: "Game won"}
			}
			b.gameState.currentIdx = 1 - b.gameState.currentIdx
			cmd.reply <- Response{ok: true}
		case "AddPlayer":
			if b.players[0] == nil {
				b.players[0] = cmd.client
				cmd.reply <- Response{ok: true, board: b}
			} else if b.players[1] == nil {
				b.players[1] = cmd.client
				cmd.reply <- Response{ok: true, board: b}
			} else {
				cmd.reply <- Response{ok: false, code: "room_full", msg: "Room you are  trying to join is full"}
			}
		}
	}
}

func (b *Board) indexOf(c *Client) int {
	if b.players[0] == c {
		return 0
	} else if b.players[1] == c {
		return 1
	} else {
		return -1
	}
}

func (b *Board) checkWinner() Cell {
	for _, line := range winLines {
		x := b.gameState.boardState[line[0]]
		y := b.gameState.boardState[line[1]]
		z := b.gameState.boardState[line[2]]
		if x != Empty && x == y && y == z {
			return x
		}
	}
	return Empty
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
	go c.writePump(ctx)
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
