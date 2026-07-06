defmodule Membrane.MoQ.CmafInteropTest do
  @moduledoc """
  NOTE: This test module was LLM-generated.

  Cross-implementation test: ffmpeg publishes a CMAF (fragmented-MP4) stream
  through the `moq` CLI, and `Membrane.MoQ.Source` consumes it. This exercises
  the consume-side per-rendition container selection (CMAF instead of
  `:legacy`) against a publisher we don't control.

  Requires `ffmpeg` and the `moq` CLI on `$PATH` (or `MOQ_CLI` pointing at the
  binary, e.g. built with `cargo install moq-cli`); without them the test is
  skipped with a warning at compile time. Tagged `:interop`; opt in with:

      mix test --include interop
  """

  use ExUnit.Case, async: false

  import Membrane.ChildrenSpec
  import Membrane.Testing.Assertions

  require Logger
  require Membrane.Pad

  alias Membrane.MoQ.Source.TrackInfo
  alias Membrane.MoQ.Test.Relay
  alias Membrane.Pad
  alias Membrane.Testing.{Pipeline, Sink}

  @moduletag :interop

  # Seconds of test pattern ffmpeg publishes (paced in real time by `-re`).
  @publish_duration 4

  ffmpeg = System.find_executable("ffmpeg")
  moq_cli = System.get_env("MOQ_CLI") || System.find_executable("moq")

  if ffmpeg && moq_cli do
    @ffmpeg ffmpeg
    @moq_cli moq_cli

    setup_all do
      [relay: Relay.ensure!()]
    end

    setup do
      [broadcast: "membrane/interop-#{System.unique_integer([:positive])}"]
    end

    test "Source consumes a CMAF broadcast published by ffmpeg | moq", %{
      relay: relay,
      broadcast: broadcast
    } do
      # The Source starts with no pads; the CMAF track's name is chosen by the
      # publisher, so the pad is linked from the `:new_track` notification.
      receiver =
        Pipeline.start_link_supervised!(
          spec:
            child(:source, %Membrane.MoQ.Source{
              url: relay.url,
              broadcast: broadcast,
              disable_tls_verify?: relay.disable_tls_verify?,
              latency: Membrane.Time.milliseconds(500)
            })
        )

      start_publisher!(relay, broadcast)

      assert_pipeline_notified(
        receiver,
        :source,
        {:new_track, %TrackInfo{type: :video} = info},
        15_000
      )

      Pipeline.execute_actions(receiver,
        spec:
          get_child(:source)
          |> via_out(Pad.ref(:output, :video), options: [track: info.track, priority: 60])
          |> child(:sink, Sink)
      )

      assert_sink_stream_format(receiver, :sink, %Membrane.H264{}, 10_000)

      # One second of steady frames proves the CMAF wire format is parsed
      # correctly. Deliberately NO end_of_stream assertion: when a publisher
      # exits abruptly the relay leaves subscriptions dangling (moq-cli's own
      # `export` hangs the same way), so whether the track-finish beats the
      # session teardown is a coin toss. See TODO.md (broadcast unannounce).
      for _i <- 1..30 do
        assert_sink_buffer(receiver, :sink, %Membrane.Buffer{}, 10_000)
      end
    end

    defp start_publisher!(relay, broadcast) do
      # `http://` makes the moq CLI fetch /certificate.sha256 first and trust
      # the relay's self-signed certificate by fingerprint.
      url =
        if relay.disable_tls_verify?,
          do: String.replace_prefix(relay.url, "https://", "http://"),
          else: relay.url

      script = ~S"""
      "$1" -hide_banner -loglevel error -re \
        -f lavfi -i "testsrc2=duration=$5:size=320x240:rate=30" \
        -pix_fmt yuv420p -c:v libx264 -preset ultrafast -tune zerolatency \
        -x264-params keyint=30:min-keyint=30:scenecut=0 \
        -f mp4 -movflags cmaf+frag_keyframe - \
      | "$2" --client-connect "$3" --broadcast "$4" import fmp4
      """

      # ffmpeg self-terminates after $5 seconds, so unlike the relay this
      # pipeline needs no stdin-watching kill wrapper.
      Port.open({:spawn_executable, "/bin/sh"}, [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: [
          "-c",
          script,
          "publisher",
          @ffmpeg,
          @moq_cli,
          url,
          broadcast,
          Integer.to_string(@publish_duration)
        ]
      ])
    end
  else
    Logger.warning(
      "Skipping #{inspect(__MODULE__)}: needs ffmpeg and the moq CLI " <>
        "(MOQ_CLI env var or `moq` on $PATH, e.g. `cargo install moq-cli`)"
    )
  end
end
