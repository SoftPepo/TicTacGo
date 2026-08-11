package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/coder/websocket"
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
		conn.Write(ctx, websocket.MessageText, []byte(scanner.Text()))
	}
}
