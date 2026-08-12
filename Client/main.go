package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/coder/websocket"
)

type Command struct {
	kind     CommandKind
	name     string
	password string
	cell     int
}

type CommandKind string

const (
	Create CommandKind = "Create"
	Join   CommandKind = "Join"
)

func main() {
	ctx := context.Background()
	conn, _, err := websocket.Dial(ctx, "ws://localhost:8080/ws", nil)
	if err != nil {
		panic(err)
	}
	defer conn.CloseNow()

	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			fmt.Println(string(data))
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		command, _ := json.Marshal(Command{kind: Create, name: "testroom", password: "1234", cell: 1})
		conn.Write(ctx, websocket.MessageText, []byte(command))
	}
}
