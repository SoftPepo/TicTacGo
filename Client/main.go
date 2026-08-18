package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/coder/websocket"
)

type ClientMsg struct {
	Kind     Kind
	Nick     string
	RoomName string
	Password string
	Cell     string
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
	Move   Kind = "Move"
	Leave  Kind = "Leave"
	Exit   Kind = "Exit"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Input your nickname: ")
	scanner.Scan()
	nick, _ := json.Marshal(ClientMsg{Kind: Hello, Nick: scanner.Text()})

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
		var msg ClientMsg
		input := strings.Fields(scanner.Text())
		switch input[0] {
		case "Create":
			if len(input) != 3 {
				fmt.Println("Invalid format, Create <Room> <Password>")
				continue
			}
			msg = ClientMsg{Kind: Create, RoomName: input[1], Password: input[2]}
		case "Join":
			if len(input) != 3 {
				fmt.Println("Invalid format, Join <Room> <Password>")
				continue
			}
			msg = ClientMsg{Kind: Join, RoomName: input[1], Password: input[2]}
		case "Move":
			if len(input) != 3 {
				fmt.Println("Invalid format, Move <Cell>")
				continue
			}
			msg = ClientMsg{Kind: Move, Cell: input[1]}
		case "Leave":
			if len(input) != 1 {
				fmt.Println("Leave command takes no parameters")
				continue
			}
			msg = ClientMsg{Kind: Leave}
		case "Exit":
			if len(input) != 1 {
				fmt.Println("Exit command takes no parameters")
				continue
			}
			msg = ClientMsg{Kind: Exit}
		default:
			fmt.Println("Unknown command")
			continue
		}
		data, _ := json.Marshal(msg)
		conn.Write(ctx, websocket.MessageText, []byte(data))
		slog.Info("Message sent", "Kind", msg.Kind)
	}
}
