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
          token_to_pad: BiMap.t(token(), Membrane.Pad.ref()),
          active: MapSet.t(token()),
          renditions: renditions()
        }

  defstruct next_token: 0,
            token_to_pad: BiMap.new(),
            active: MapSet.new(),
            renditions: %{}

  @spec add_pad(t(), Membrane.Pad.ref()) :: {token(), t()}
  def add_pad(tracks, pad) do
    token = tracks.next_token

    {token,
     %{tracks | next_token: token + 1, token_to_pad: BiMap.put(tracks.token_to_pad, token, pad)}}
  end

  @spec remove_pad(t(), Membrane.Pad.ref()) :: {token() | nil, t()}
  def remove_pad(tracks, pad) do
    case BiMap.get_key(tracks.token_to_pad, pad) do
      nil -> {nil, tracks}
      token -> {token, drop(tracks, token)}
    end
  end

  @spec remove_token(t(), token()) :: {Membrane.Pad.ref() | nil, t()}
  def remove_token(tracks, token) do
    case BiMap.get(tracks.token_to_pad, token) do
      nil -> {nil, tracks}
      pad -> {pad, drop(tracks, token)}
    end
  end

  @spec pad_for(t(), token()) :: Membrane.Pad.ref() | nil
  def pad_for(tracks, token), do: BiMap.get(tracks.token_to_pad, token)

  @spec rendition(t(), Native.track()) :: Native.rendition() | nil
  def rendition(tracks, track), do: tracks.renditions[track]

  @spec waiting(t()) :: [{token(), Membrane.Pad.ref()}]
  def waiting(tracks) do
    for {token, pad} <- tracks.token_to_pad,
        not MapSet.member?(tracks.active, token),
        do: {token, pad}
  end

  @spec activate(t(), token()) :: t()
  def activate(tracks, token) do
    %{tracks | active: MapSet.put(tracks.active, token)}
  end

  @spec apply_snapshot(
          t(),
          [{Native.track(), Native.rendition()}],
          (Membrane.Pad.ref() -> Native.track())
        ) :: {snapshot_diff(), t()}
  def apply_snapshot(tracks, renditions, track_of) do
    new_renditions = Map.new(renditions)
    {removed, added, changed} = diff(tracks.renditions, new_renditions)

    ended =
      for token <- tracks.active,
          pad = pad_for(tracks, token),
          track_of.(pad) in changed,
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
      | token_to_pad: BiMap.delete_key(tracks.token_to_pad, token),
        active: MapSet.delete(tracks.active, token)
    }
  end
end
