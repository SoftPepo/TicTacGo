package main

import (
	"context"
	"net/http"

	"github.com/coder/websocket"
)

type Client struct {
	conn  *websocket.Conn
	reply chan string
	nick  string
	hub   *Hub
	board *Board
}

type Hub struct {
	clients    map[*Client]bool
	boards     map[*Board]bool
	register   chan *Client
	unregister chan *Client
	requests   chan Command
}

type Board struct {
	playerA    *Client
	playerB    *Client
	name       string
	password   string
	gameState  GameState
	move       chan string
	broadcast  chan GameState
	closeBoard chan struct{}
}

type GameState struct {
	boardState    [9]Cell
	currentPlayer string
	status        Status
	result        Result
}

type Command struct {
	Kind   CommandKind
	Client *Client
}

type Status string

const (
	Lobby    Status = "Lobby"
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

func (c *Client) readPump(ctx context.Context) {
	defer func() { c.hub.unregister <- c }()

	_, data, err := c.conn.Read(ctx)
	if err != nil {
		return
	}
	c.hub.requests <- string(data)
}

func (c *Client) writePump(ctx context.Context) {
	defer c.conn.Close(websocket.StatusNormalClosure, "")
	for msg := range c.reply {
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

	c := &Client{conn: conn, reply: make(chan string), nick: "Anon", hub: hub}
	hub.register <- c

}

func main() {
}
