defmodule Membrane.MoQ.Test.RestartingSubscriber do
  @moduledoc """
  Notification-driven subscriber pipeline.

  Wires an H26x parser directly onto the Source pad from `{:new_track, {track, stream_format}}`
  On `{:disconnected, _}`, tears the generation down and starts a fresh Source to resubscribe.
  """

  use Membrane.Pipeline

  require Membrane.Pad

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
      generation: 0,
      track: nil
    }

    {[spec: child({:source, state.generation}, state.source_spec)], state}
  end

  @impl true
  def handle_child_notification(
        {:new_track, {track, %module{} = stream_format}},
        {:source, gen},
        _ctx,
        %{generation: gen, track: nil} = state
      )
      when module in [Membrane.H264, Membrane.H265] do
    spec =
      get_child({:source, gen})
      |> via_out(Pad.ref(:output, track), options: [track: track])
      |> child({:parser, gen}, parser_for(stream_format))
      |> child({:sink, gen}, Testing.Sink)

    send(self(), {:generation_landed, gen, track})

    {[spec: spec], %{state | track: track}}
  end

  def handle_child_notification(
        {:disconnected, _reason},
        {:source, gen},
        ctx,
        %{generation: gen} = state
      ) do
    teardown =
      case state.track do
        nil -> [{:source, gen}]
        _track -> for {child, _spec} <- ctx.children, do: child
      end

    state = %{state | generation: gen + 1, track: nil}
    {[remove_children: teardown, spec: child({:source, state.generation}, state.source_spec)], state}
  end

  def handle_child_notification(_notification, _child, _ctx, state), do: {[], state}

  defp parser_for(%Membrane.H264{}), do: %Membrane.H264.Parser{}
  defp parser_for(%Membrane.H265{}), do: %Membrane.H265.Parser{}
end
