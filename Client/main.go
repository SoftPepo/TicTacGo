package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/coder/websocket"
)

type ClientMsg struct {
	kind     Kind
	nick     string
	roomName string
	password string
	cell     int
}

type Client struct {
	conn *websocket.Conn
	send chan string
	nick string
}

type Kind string

const (
	Hello  Kind = "Hello"
	Create Kind = "Create"
	Join   Kind = "Join"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Input your nickname: ")
	scanner.Scan()
	nick, _ := json.Marshal(ClientMsg{kind: Hello, nick: scanner.Text()})

	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, "ws://localhost:8080/ws", nil)
	if err != nil {
		panic(err)
	}
	defer conn.CloseNow()

	conn.Write(ctx, websocket.MessageText, []byte(nick))

	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			fmt.Println(string(data))
		}
	}()

	for scanner.Scan() {
		msg, _ := json.Marshal(ClientMsg{kind: Create, roomName: "testroom", password: "1234", cell: 1})
		conn.Write(ctx, websocket.MessageText, []byte(msg))
	}
}
