defmodule Membrane.MoQ.Source.TracksTest do
  @moduledoc "Pure tests of the Source's subscription/catalog bookkeeping."

  use ExUnit.Case, async: true

  alias Membrane.MoQ.Source.Tracks

  @pad_a {:pad, :a}
  @pad_b {:pad, :b}

  test "pads get consecutive tokens and removal returns them once" do
    tracks = %Tracks{}

    assert {0, tracks} = Tracks.add_pad(tracks, @pad_a, "video")
    assert {1, tracks} = Tracks.add_pad(tracks, @pad_b, "audio")
    assert Tracks.pad_for(tracks, 0) == @pad_a

    assert {0, tracks} = Tracks.remove_pad(tracks, @pad_a, "video")
    assert {nil, ^tracks} = Tracks.remove_pad(tracks, @pad_a, "video")
    assert Tracks.pad_for(tracks, 0) == nil

    # Tokens are never reused, even after the highest pad is removed.
    assert {2, _tracks} = Tracks.add_pad(tracks, @pad_a, "video")
  end

  test "waiting lists only tokens without a live subscription" do
    tracks = %Tracks{}
    {a, tracks} = Tracks.add_pad(tracks, @pad_a, "video")
    {b, tracks} = Tracks.add_pad(tracks, @pad_b, "audio")

    tracks = Tracks.activate(tracks, a)
    assert Tracks.waiting(tracks) == [{b, @pad_b, "audio"}]

    # A dead subscription retires its token binding entirely.
    assert {@pad_a, tracks} = Tracks.remove_token(tracks, a)
    assert {nil, ^tracks} = Tracks.remove_token(tracks, a)
    assert Tracks.waiting(tracks) == [{b, @pad_b, "audio"}]
  end

  test "apply_snapshot categorizes the diff and stores the renditions" do
    {diff, tracks} = Tracks.apply_snapshot(%Tracks{}, [{"video", :r1}, {"audio", :r1}])

    assert %{removed: [], changed: [], ended: []} = diff
    assert Enum.sort(diff.added) == ["audio", "video"]
    assert Tracks.rendition(tracks, "video") == :r1

    {diff, tracks} = Tracks.apply_snapshot(tracks, [{"video", :r2}, {"text", :r1}])

    assert %{removed: ["audio"], added: ["text"], changed: ["video"], ended: []} = diff
    assert Tracks.rendition(tracks, "video") == :r2
    assert Tracks.rendition(tracks, "audio") == nil
  end

  test "an identical snapshot diffs to nothing" do
    renditions = [{"video", :r1}]

    {_diff, tracks} = Tracks.apply_snapshot(%Tracks{}, renditions)
    {diff, _tracks} = Tracks.apply_snapshot(tracks, renditions)

    assert diff == %{removed: [], added: [], changed: [], ended: []}
  end

  test "a changed rendition ends and retires its live subscription; waiting ones stay" do
    {_diff, tracks} = Tracks.apply_snapshot(%Tracks{}, [{"video", :r1}])
    {a, tracks} = Tracks.add_pad(tracks, @pad_a, "video")
    {b, tracks} = Tracks.add_pad(tracks, @pad_b, "video")
    tracks = Tracks.activate(tracks, a)

    {diff, tracks} = Tracks.apply_snapshot(tracks, [{"video", :r2}])

    # Only the live subscription ends; the waiting pad may still resolve
    # against the replacement rendition.
    assert diff.ended == [{a, @pad_a}]
    assert Tracks.pad_for(tracks, a) == nil
    assert Tracks.waiting(tracks) == [{b, @pad_b, "video"}]
  end
end
