defmodule Membrane.MoQ.Source.TracksTest do
  @moduledoc "Pure tests of the Source's subscription/catalog bookkeeping."

  use ExUnit.Case, async: true

  alias Membrane.MoQ.Source.Tracks

  @pad_a {:pad, :a}
  @pad_b {:pad, :b}

  test "pads get consecutive tokens and removal returns them once" do
    tracks = %Tracks{}

    {0, tracks} = Tracks.add_pad(tracks, @pad_a)
    {1, tracks} = Tracks.add_pad(tracks, @pad_b)
    assert Tracks.pad_for(tracks, 0) == @pad_a

    {0, tracks} = Tracks.remove_pad(tracks, @pad_a)
    assert {nil, ^tracks} = Tracks.remove_pad(tracks, @pad_a)
    assert Tracks.pad_for(tracks, 0) == nil

    # Tokens are never reused, even after the highest pad is removed.
    {2, _tracks} = Tracks.add_pad(tracks, @pad_a)
  end

  test "waiting lists only tokens without a live subscription" do
    tracks = %Tracks{}
    {a, tracks} = Tracks.add_pad(tracks, @pad_a)
    {b, tracks} = Tracks.add_pad(tracks, @pad_b)

    tracks = Tracks.activate(tracks, a)
    assert Tracks.waiting(tracks) == [{b, @pad_b}]

    tracks = Tracks.deactivate(tracks, a)
    assert Enum.sort(Tracks.waiting(tracks)) == [{a, @pad_a}, {b, @pad_b}]
  end

  test "apply_snapshot categorizes the diff and stores the renditions" do
    track_of = fn _pad -> flunk("no live subscription should be consulted") end

    {diff, tracks} =
      Tracks.apply_snapshot(%Tracks{}, [{"video", :r1}, {"audio", :r1}], track_of)

    assert %{removed: [], changed: [], ended: []} = diff
    assert Enum.sort(diff.added) == ["audio", "video"]
    assert Tracks.rendition(tracks, "video") == :r1

    {diff, tracks} = Tracks.apply_snapshot(tracks, [{"video", :r2}, {"text", :r1}], track_of)

    assert %{removed: ["audio"], added: ["text"], changed: ["video"], ended: []} = diff
    assert Tracks.rendition(tracks, "video") == :r2
    assert Tracks.rendition(tracks, "audio") == nil
  end

  test "an identical snapshot diffs to nothing" do
    track_of = fn _pad -> flunk("no live subscription should be consulted") end
    renditions = [{"video", :r1}]

    {_diff, tracks} = Tracks.apply_snapshot(%Tracks{}, renditions, track_of)
    {diff, _tracks} = Tracks.apply_snapshot(tracks, renditions, track_of)

    assert diff == %{removed: [], added: [], changed: [], ended: []}
  end

  test "a changed rendition ends and retires its live subscription; waiting ones stay" do
    track_of = %{@pad_a => "video", @pad_b => "video"}

    {_diff, tracks} = Tracks.apply_snapshot(%Tracks{}, [{"video", :r1}], &track_of[&1])
    {a, tracks} = Tracks.add_pad(tracks, @pad_a)
    {b, tracks} = Tracks.add_pad(tracks, @pad_b)
    tracks = Tracks.activate(tracks, a)

    {diff, tracks} = Tracks.apply_snapshot(tracks, [{"video", :r2}], &track_of[&1])

    # Only the live subscription ends; the waiting pad may still resolve
    # against the replacement rendition.
    assert diff.ended == [{a, @pad_a}]
    assert Tracks.pad_for(tracks, a) == nil
    assert Tracks.waiting(tracks) == [{b, @pad_b}]
  end
end
