defmodule Membrane.MoQ.Source.Tracks do
  @moduledoc false

  alias ExMoQ.Native

  @type token :: integer()

  @type snapshot_diff :: %{
          removed: [Native.track()],
          added: [Native.track()],
          changed: [Native.track()],
          ended: [{token(), Membrane.Pad.ref()}]
        }

  @type renditions :: %{Native.track() => Native.rendition()}

  @type t :: %__MODULE__{
          next_token: token(),
          # Entry lives from pad being added until pad EOS
          subscriptions: BiMap.t(token(), {Membrane.Pad.ref(), Native.track()}),
          # Entry lives from native subscription task start until pad EOS
          active: MapSet.t(token()),
          renditions: renditions()
        }

  defstruct next_token: 0,
            subscriptions: BiMap.new(),
            active: MapSet.new(),
            renditions: %{}

  @spec add_pad(t(), Membrane.Pad.ref(), Native.track()) :: {token(), t()}
  def add_pad(tracks, pad, track) do
    token = tracks.next_token

    {token,
     %{
       tracks
       | next_token: token + 1,
         subscriptions: BiMap.put(tracks.subscriptions, token, {pad, track})
     }}
  end

  @spec remove_pad(t(), Membrane.Pad.ref(), Native.track()) :: {token() | nil, t()}
  def remove_pad(tracks, pad, track) do
    case BiMap.get_key(tracks.subscriptions, {pad, track}) do
      nil -> {nil, tracks}
      token -> {token, drop(tracks, token)}
    end
  end

  @spec remove_token(t(), token()) :: {Membrane.Pad.ref() | nil, t()}
  def remove_token(tracks, token) do
    case BiMap.get(tracks.subscriptions, token) do
      nil -> {nil, tracks}
      {pad, _track} -> {pad, drop(tracks, token)}
    end
  end

  @spec pad_for(t(), token()) :: Membrane.Pad.ref() | nil
  def pad_for(tracks, token) do
    case BiMap.get(tracks.subscriptions, token) do
      nil -> nil
      {pad, _track} -> pad
    end
  end

  @spec rendition(t(), Native.track()) :: Native.rendition() | nil
  def rendition(tracks, track), do: tracks.renditions[track]

  @spec waiting(t()) :: [{token(), Membrane.Pad.ref(), Native.track()}]
  def waiting(tracks) do
    for {token, {pad, track}} <- tracks.subscriptions,
        not MapSet.member?(tracks.active, token),
        do: {token, pad, track}
  end

  @spec activate(t(), token()) :: t()
  def activate(tracks, token) do
    %{tracks | active: MapSet.put(tracks.active, token)}
  end

  @spec apply_snapshot(t(), [{Native.track(), Native.rendition()}]) :: {snapshot_diff(), t()}
  def apply_snapshot(tracks, renditions) do
    new_renditions = Map.new(renditions)
    {removed, added, changed} = diff(tracks.renditions, new_renditions)

    ended =
      for token <- tracks.active,
          {pad, track} = BiMap.fetch!(tracks.subscriptions, token),
          track in changed,
          do: {token, pad}

    tracks = %{tracks | renditions: new_renditions}
    tracks = Enum.reduce(ended, tracks, fn {token, _pad}, tracks -> drop(tracks, token) end)

    {%{removed: removed, added: added, changed: changed, ended: ended}, tracks}
  end

  @spec diff(old :: renditions(), new :: renditions()) ::
          {removed :: [Native.track()], added :: [Native.track()], changed :: [Native.track()]}
  defp diff(old, new) do
    removed = for {name, _rendition} <- old, not is_map_key(new, name), do: name
    added = for {name, _rendition} <- new, not is_map_key(old, name), do: name

    changed =
      Map.intersect(new, old, fn _name, old_rendition, new_rendition ->
        old_rendition != new_rendition
      end)
      |> Map.filter(fn {_name, changed?} -> changed? end)
      |> Map.keys()

    {removed, added, changed}
  end

  @spec drop(t(), token()) :: t()
  defp drop(tracks, token) do
    %{
      tracks
      | subscriptions: BiMap.delete_key(tracks.subscriptions, token),
        active: MapSet.delete(tracks.active, token)
    }
  end
end
