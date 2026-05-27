defmodule Membrane.MoQ.IntegrationTest do
  use ExUnit.Case, async: true
  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions
  alias Membrane.Testing.Pipeline

  # Integration tests require a running MoQ relay server.
  # Set the RELAY_URL environment variable to its address, e.g.:
  #   RELAY_URL=https://localhost:4443 mix test --include integration
  #
  # If RELAY_URL is not set, these tests are skipped.

  @moduletag :integration

  @relay_url System.get_env("RELAY_URL", "https://localhost:4443")
  @broadcast "membrane/test"
  @track "data"

  defmodule TimestampsGenerator do
    use Membrane.Filter

    def_input_pad :input, accepted_format: _any
    def_output_pad :output, accepted_format: _any

    @impl true
    def handle_init(_ctx, _opts) do
      {[], %{i: 0}}
    end

    @impl true
    def handle_buffer(:input, %Membrane.Buffer{} = buffer, _ctx, state) do
      buffer = %{buffer | pts: Membrane.Time.milliseconds(state.i)}
      {[buffer: {:output, buffer}], %{state | i: state.i + 1}}
    end
  end

  @tag :tmp_dir
  test "sink publishes a stream that can be received by the source", ctx do
    output = Path.join(ctx.tmp_dir, "out.bin")
    input = "test/fixtures/bbb.ts"

    receiver = Pipeline.start_link_supervised!()

    receiver_spec =
      child(:source, %Membrane.MoQ.Source{
        url: @relay_url,
        broadcast: @broadcast,
        track: @track
      })
      |> child(:sink, %Membrane.File.Sink{location: output})

    Pipeline.execute_actions(receiver, spec: receiver_spec)
    assert_child_playing(receiver, :source)

    sender = Pipeline.start_link_supervised!()

    sender_spec =
      child(:source, %Membrane.File.Source{location: input})
      |> child(:timestamps_generator, TimestampsGenerator)
      |> child(:realtimer, Membrane.Realtimer)
      |> child(:sink, %Membrane.MoQ.Sink{
        url: @relay_url,
        broadcast: @broadcast,
        track: @track
      })

    Pipeline.execute_actions(sender, spec: sender_spec)

    assert_end_of_stream(receiver, :sink, :input, 10_000)
    :ok = Membrane.Pipeline.terminate(sender)
    :ok = Membrane.Pipeline.terminate(receiver)

    assert File.read!(input) == File.read!(output)
  end

  test "source handles relay disconnect gracefully" do
    receiver = Pipeline.start_link_supervised!()

    receiver_spec =
      child(:source, %Membrane.MoQ.Source{
        url: @relay_url,
        broadcast: "membrane/nonexistent",
        track: @track
      })
      |> child(:fake, Membrane.Fake.Sink)

    Pipeline.execute_actions(receiver, spec: receiver_spec)
    assert_child_playing(receiver, :source)

    # When the relay closes the subscription (e.g. broadcast not found),
    # the source should emit end_of_stream.
    assert_end_of_stream(receiver, :source, :output, 5_000)

    :ok = Membrane.Pipeline.terminate(receiver)
  end
end
