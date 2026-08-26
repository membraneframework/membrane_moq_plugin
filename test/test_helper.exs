ExUnit.start(capture_log: true)

cond do
  ExMoQ.Test.Relay.find_binary() ->
    :ok

  System.get_env("CI") == "true" ->
    raise "moq-relay not found — integration tests must not be skipped in CI"

  true ->
    IO.puts("moq-relay not found — excluding :integration tests")
    ExUnit.configure(exclude: [:integration])
end

if System.get_env("CI") == "true" do
  ExUnit.configure(exclude: [:flaky])
end
