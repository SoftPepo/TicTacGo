package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/coder/websocket"
)

type ClientMsg struct {
	Kind     Kind   `json:"kind"`
	Nick     string `json:"nick"`
	RoomName string `json:"room_name"`
	Password string `json:"password"`
	Cell     int    `json:"cell"`
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

type Ack struct {
	Kind string `json:"kind"`
	Ok   bool   `json:"ok"`
	Code string `json:"code"`
	Msg  string `json:"msg"`
}

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
				slog.Error("Connection read error")
				continue
			}
			var ack Ack
			err = json.Unmarshal(data, &ack)
			if err != nil {
				slog.Error("Inbound json conversion error")
				continue
			}
			fmt.Println(string(data))
		}
	}()

	for scanner.Scan() {
		var msg ClientMsg
		input := strings.Fields(scanner.Text())
		if len(input) == 0 {
			fmt.Println("Please input a command: ")
			continue
		}
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
			if len(input) != 2 {
				fmt.Println("Invalid format, Move <Cell>")
				continue
			}
			cell, err := strconv.Atoi(input[1])
			if input[0] == "Move" && err != nil {
				fmt.Println("Cell number should be a number")
				continue
			}
			msg = ClientMsg{Kind: Move, Cell: cell}
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
