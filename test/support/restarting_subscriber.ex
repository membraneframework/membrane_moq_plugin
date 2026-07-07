defmodule Membrane.MoQ.Test.RestartingSubscriber do
  @moduledoc """
  NOTE: This test support module was LLM-generated.

  Notification-driven subscriber pipeline: wires an H26x parser directly onto
  the Source pad from `{:new_track, info}` (the pad carries a full stream
  format, no filter in between), and on `{:disconnected, _}` tears the
  generation down and starts a fresh Source to resubscribe.

  Run it through `Membrane.Testing.Pipeline` (`module:` + `custom_args:`), so
  the test process observes everything via `Membrane.Testing.Assertions` —
  notifications land through `assert_pipeline_notified` and each generation's
  `Membrane.Testing.Sink` (named `{:sink, gen}`) through `assert_sink_*`.
  """

  use Membrane.Pipeline

  require Membrane.Pad

  alias Membrane.MoQ.Source.TrackInfo
  alias Membrane.Pad
  alias Membrane.Testing

  @impl true
  def handle_init(_ctx, opts) do
    state = %{
      source_spec: %Membrane.MoQ.Source{
        url: opts[:url],
        broadcast: opts[:broadcast],
        disable_tls_verify?: opts[:disable_tls_verify?],
        latency: Membrane.Time.milliseconds(200)
      },
      gen: 0,
      current: nil
    }

    {[spec: source_spec(state)], state}
  end

  @impl true
  def handle_child_notification(
        {:new_track, %TrackInfo{} = info},
        {:source, gen},
        _ctx,
        %{gen: gen, current: nil} = state
      )
      when info.type == :video do
    spec =
      get_child({:source, gen})
      |> via_out(Pad.ref(:output, info.track), options: [track: info.track])
      |> child({:parser, gen}, parser_for(info.stream_format))
      |> child({:sink, gen}, Testing.Sink)

    {[spec: spec], %{state | current: info.track}}
  end

  def handle_child_notification(
        {:disconnected, _reason},
        {:source, gen},
        _ctx,
        %{gen: gen} = state
      ) do
    teardown =
      case state.current do
        nil -> [{:source, gen}]
        _track -> [{:source, gen}, {:parser, gen}, {:sink, gen}]
      end

    state = %{state | gen: gen + 1, current: nil}
    {[remove_children: teardown, spec: source_spec(state)], state}
  end

  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}

  defp source_spec(state) do
    child({:source, state.gen}, state.source_spec)
  end

  defp parser_for(%Membrane.H264{}), do: %Membrane.H264.Parser{}
  defp parser_for(%Membrane.H265{}), do: %Membrane.H265.Parser{}
end
