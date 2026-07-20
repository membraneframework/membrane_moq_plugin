defmodule Membrane.MoQ.Test.Relay do
  @moduledoc """
  NOTE: This test support module was LLM-generated.

  Provides a MoQ relay for integration tests.

  `ensure!/0` returns `%{url: url, disable_tls_verify?: boolean}`:

    * If `RELAY_URL` is set, that relay is used as-is and nothing is spawned.
    * Otherwise a `moq-relay` binary (`MOQ_RELAY` env var, or found on `$PATH`)
      is spawned on a random free port with a self-signed certificate and
      anonymous auth. The instance is shared by all test modules in the run.

  Cleanup of the spawned relay is layered:

    * `ExUnit.after_suite/1` (registered on spawn) stops the server once the
      suite finishes, closing the port and deleting the generated config.
    * Should the VM die without running that callback (e.g. SIGKILL), the
      spawn wrapper still reaps the relay: it watches our end of the stdin
      pipe and kills the relay when the pipe closes (see `init/1`).
  """

  use GenServer

  @localhost_hosts ["localhost", "127.0.0.1", "::1"]
  @ready_timeout_ms 15_000
  @probe_interval_ms 100

  @type relay :: %{url: String.t(), disable_tls_verify?: boolean()}

  @spec ensure!() :: relay()
  def ensure!() do
    case System.get_env("RELAY_URL") do
      nil ->
        spawned_relay!()

      url ->
        # A local relay presents a self-signed certificate; a remote one is
        # expected to present a valid certificate.
        %{url: url, disable_tls_verify?: URI.parse(url).host in @localhost_hosts}
    end
  end

  defp spawned_relay!() do
    # Unlinked and named, so one relay serves every test module in the run.
    pid =
      case GenServer.start(__MODULE__, nil, name: __MODULE__) do
        {:ok, pid} ->
          pid

        {:error, {:already_started, pid}} ->
          pid

        {:error, reason} ->
          raise "failed to start the test relay: #{Exception.format_exit(reason)}"
      end

    GenServer.call(pid, :relay, :infinity)
  end

  @doc "Stops the spawned relay, if any. Registered as an `ExUnit.after_suite/1` callback."
  @spec stop() :: :ok
  def stop() do
    case GenServer.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.stop(pid)
    end
  end

  @impl true
  def init(nil) do
    binary = find_binary!()
    port_number = free_port!()
    config_path = write_config!(port_number)

    # Closing a port (or the whole VM exiting) only closes
    # the child's stdin/stdout pipes; it does not kill the child, and the
    # relay never reads stdin, so spawned bare it would linger as an orphan.
    # The shell wrapper below runs the relay in the background with a watcher:
    # `cat` returns EOF the moment our end of the stdin pipe goes away — and
    # then kills the relay. Meanwhile the foreground `wait` ties the wrapper's
    # lifetime to the relay's, so a relay crash reaches us as the port's
    # `:exit_status`. Expanded, the invocation is:
    #
    #   /bin/sh -c 'exec 3<&0; "$1" "$2" & pid=$!;
    #               { cat <&3; kill $pid; } > /dev/null 2>&1 & wait $pid' \
    #     wrapper <binary> <config>
    #
    # `sh -c` maps the arguments after the script to $0 ("wrapper", unused),
    # $1 (the relay binary) and $2 (the config path); passing the paths as
    # positional parameters keeps them safe from quoting issues.
    # The fd shuffling makes the watcher hold only the stdin pipe:
    # a background job's stdin is /dev/null, so fd 3 smuggles in the real stdin for `cat`,
    # and the group's stdout/stderr must not point at the port's pipe,
    # or Erlang would withhold `:exit_status` until the watcher too is gone.
    cleanup_wrapper =
      ~S(exec 3<&0; "$1" "$2" & pid=$!; { cat <&3; kill $pid; } > /dev/null 2>&1 & wait $pid)

    port =
      Port.open({:spawn_executable, "/bin/sh"}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["-c", cleanup_wrapper, "wrapper", binary, config_path]
      ])

    await_ready!(port_number, port)

    ExUnit.after_suite(fn _stats -> stop() end)

    {:ok,
     %{
       relay: %{url: "https://localhost:#{port_number}", disable_tls_verify?: true},
       port: port,
       config_path: config_path
     }}
  end

  @impl true
  def terminate(_reason, state) do
    # Closing our end of the stdin pipe makes the wrapper's `cat` return EOF
    # and kill the relay.
    if Port.info(state.port) != nil, do: Port.close(state.port)
    File.rm(state.config_path)
    :ok
  end

  @impl true
  def handle_call(:relay, _from, state), do: {:reply, state.relay, state}

  @impl true
  def handle_info({port, {:data, _output}}, %{port: port} = state), do: {:noreply, state}

  def handle_info({port, {:exit_status, status}}, %{port: port} = state) do
    {:stop, {:moq_relay_exited, status}, state}
  end

  defp find_binary!() do
    System.get_env("MOQ_RELAY") || System.find_executable("moq-relay") ||
      raise """
      no MoQ relay available for the integration tests; provide one of:
        * RELAY_URL — URL of an already-running relay
        * MOQ_RELAY — path to a moq-relay binary
        * moq-relay on $PATH (e.g. installed with `cargo install moq-relay`)
      """
  end

  # The relay binds the same port number twice: QUIC over UDP (`[server]`) and
  # the plain-HTTP readiness endpoint over TCP (`[web.http]`). Take a UDP port
  # from the OS and keep it only if the same number is also free on TCP.
  # The sockets are closed before the relay spawns, so another process could
  # still grab the port in between; unlikely enough for a test helper.
  defp free_port!(attempts \\ 10)

  defp free_port!(0) do
    raise "could not find a port free on both UDP and TCP for the test relay"
  end

  defp free_port!(attempts) do
    {:ok, udp_socket} = :gen_udp.open(0, reuseaddr: true)
    {:ok, port_number} = :inet.port(udp_socket)

    case :gen_tcp.listen(port_number, reuseaddr: true) do
      {:ok, tcp_socket} ->
        :ok = :gen_tcp.close(tcp_socket)
        :ok = :gen_udp.close(udp_socket)
        port_number

      {:error, _reason} ->
        :ok = :gen_udp.close(udp_socket)
        free_port!(attempts - 1)
    end
  end

  defp write_config!(port_number) do
    path = Path.join(System.tmp_dir!(), "membrane-moq-test-relay-#{port_number}.toml")

    File.write!(path, """
    # Generated by #{inspect(__MODULE__)}; safe to delete.
    [log]
    level = "info"

    [server]
    listen = "[::]:#{port_number}"
    # Self-signed certificate; clients connect with TLS verification disabled.
    tls.generate = ["localhost"]

    # Serves /certificate.sha256 over plain HTTP, used as the readiness probe.
    [web.http]
    listen = "[::]:#{port_number}"

    [auth]
    public = ""
    """)

    path
  end

  defp await_ready!(port_number, port) do
    deadline = System.monotonic_time(:millisecond) + @ready_timeout_ms
    await_ready_loop(port_number, deadline, port)
  end

  defp await_ready_loop(port_number, deadline, port) do
    cond do
      probe(port_number) ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        raise """
        moq-relay did not become ready within #{@ready_timeout_ms} ms; its output so far:

        #{drain_output(port)}
        """

      true ->
        Process.sleep(@probe_interval_ms)
        await_ready_loop(port_number, deadline, port)
    end
  end

  # Fetches /certificate.sha256, which the relay serves over plain HTTP once
  # it is up. Hand-rolled over :gen_tcp because Mix prunes the code paths of
  # undeclared OTP apps like :inets in the test env.
  defp probe(port_number) do
    case :gen_tcp.connect(~c"127.0.0.1", port_number, [:binary, active: false], 1_000) do
      {:ok, socket} ->
        request =
          "GET /certificate.sha256 HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n"

        :ok = :gen_tcp.send(socket, request)
        response = :gen_tcp.recv(socket, 0, 1_000)
        :gen_tcp.close(socket)
        match?({:ok, "HTTP/1.1 200" <> _rest}, response)

      {:error, _reason} ->
        false
    end
  end

  defp drain_output(port, acc \\ []) do
    receive do
      {^port, {:data, data}} ->
        drain_output(port, [acc, data])

      {^port, {:exit_status, status}} ->
        IO.iodata_to_binary([acc, "\n(exited with status #{status})"])
    after
      0 -> IO.iodata_to_binary(acc)
    end
  end
end
