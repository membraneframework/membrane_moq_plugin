ExUnit.start(capture_log: true)

# Integration tests talk to a real MoQ relay; opt in with
# `mix test --include integration` (see `Membrane.MoQ.Test.Relay` for how the
# relay is provided). Interop tests additionally need ffmpeg and the moq CLI;
# opt in with `mix test --include interop`.
ExUnit.configure(exclude: [:integration, :interop])
