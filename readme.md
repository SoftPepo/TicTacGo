# TicTacGo

Multiplayer tic-tac-toe server built with Go and WebSockets. Two players
join a password-protected room and play in real time.

## Running

```bash
go run .
```

Server listens on `:8080`, WebSocket endpoint at `/ws`.

Client:

```bash
go run ./client
```

## Architecture

State is owned by goroutines rather than guarded by locks. Every piece of
mutable state has exactly one writer, and goroutines communicate through
channels instead of shared memory.

- **Hub** — a single goroutine owning the room registry. Handles client
  registration, room creation and lookup.
- **Board** — one goroutine per game, owning the board state and the player
  slots. Broadcasts state directly to both players; the hub is not a relay.
- **Client** — two goroutines: a reader pulling frames off the socket and a
  writer draining a buffered outbound channel.

Requests that need a return value (room creation, joining) carry a
single-use reply channel, so the hub never blocks waiting for anyone.

There is one mutex in the codebase, on `Board.players`. It exists because
the hub reads player slots when validating whether a client is already in
a game, which is the only place two goroutines touch the same field.

## Protocol

### Client to server

| kind          | fields |
|---------------|---|
| (first_frame) | `nick` |
| `Create`      | `room_name`, `password` |
| `Join`        | `room_name`, `password` |
| `Move`        | `cell` (0–8) |
| `Leave`       | — |

### Server to client

| kind | fields |
|---|---|
| `Gamestate` | `board_state`, `current_idx`, `status`, `result` |
| `Ack` | `ok`, `code`, `msg` |

Client identity is derived from the connection, never from message
contents — a client cannot claim to be someone else.

## Design decisions

**Room name as ID** — Allows for sharing and finding rooms simply by name.

**Plain text password** — Rooms are ephemeral and passwords are never reused elsewhere, so hashing would defend against a threat that doesn't exist here.

## Known limitations

**No reconnect** — This functionality would require many new elements for very low gain, so I left it out of the scope.

**No nickname validation** — Nicks are passed through unchecked; an empty or very long nick will look broken in the other player's client.

**No persistence after restart** — Both registration and games are quick enough not to require persistence.
