package main

import "testing"

func TestCheckWinner(t *testing.T) {
	b := &Board{}
	b.gameState.BoardState = [9]Cell{X, X, X}
	if b.checkWinner() != X {
		t.Errorf("X should win, %v won instead", b.checkWinner())
	}
}
