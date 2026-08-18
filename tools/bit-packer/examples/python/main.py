from game import Player, GameState

def main():
    p = Player()
    p.username = "Hero"
    p.level = 10
    p.score = 5000
    p.inventory = ["Sword", "Shield", "Potion"]

    g = GameState()
    g.id = 1
    g.isActive = True
    g.players = [p]

    # Encode
    data = g.Encode()
    print(f"Encoded size: {len(data)} bytes")

    # Decode
    decoded = GameState.Decode(data)
    print(f"Decoded Game ID: {decoded.id}")
    if decoded.players:
        print(f"Decoded Player: {decoded.players[0].username} (Level {decoded.players[0].level})")

if __name__ == "__main__":
    main()
