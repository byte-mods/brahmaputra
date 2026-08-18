package main

import (
	"fmt"
	"log"
)

func main() {
	player := Player{
		Username:  "Hero",
		Level:     10,
		Score:     5000,
		Inventory: []string{"Sword", "Shield", "Potion"},
	}

	game := &GameState{
		Id:       1,
		IsActive: true,
		Players:  []Player{player},
	}

	// Encode
	data := game.Encode()
	fmt.Printf("Encoded size: %d bytes\n", len(data))

	// Decode
	decoded, err := DecodeGameState(data)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Decoded Game ID: %d\n", decoded.Id)
	fmt.Printf("Decoded Player: %s (Level %d)\n", decoded.Players[0].Username, decoded.Players[0].Level)
}
