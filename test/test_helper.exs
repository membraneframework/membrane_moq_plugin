ExUnit.start(capture_log: true)

# Integration tests talk to a real MoQ relay; opt in with
# `mix test --include integration` (and `RELAY_URL=...` to point at it).
ExUnit.configure(exclude: [:integration])
