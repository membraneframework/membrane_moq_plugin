ExUnit.start(capture_log: true)

# Integration tests talk to a real MoQ relay.
# Opt in with `mix test --include integration`.
ExUnit.configure(exclude: [:integration])
