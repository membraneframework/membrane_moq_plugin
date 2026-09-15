ExUnit.start(capture_log: true)

IO.puts("""
Some tests assume a moq-relay binary is available in your PATH.
To disable them, run:

  mix test --exclude integration

""")
