defmodule Membrane.MoQ.Source.Tracks do
  @moduledoc false

  alias ExMoQ.Native

  @type token :: integer()

  @type snapshot_diff :: %{
          removed: [String.t()],
          added: [String.t()],
          changed: [String.t()],
          ended: [{token(), Membrane.Pad.ref()}]
        }

  @type t :: %__MODULE__{
          next_token: token(),
          tokens: BiMap.t(String.t(), token()),
          active: MapSet.t(token()),
          renditions: %{String.t() => Native.rendition()}
        }

  defstruct next_token: 0,
            tokens: BiMap.new(),
            active: MapSet.new(),
            renditions: %{}

  @spec add_pad(t(), Membrane.Pad.ref()) :: {token(), t()}
  def add_pad(tracks, pad) do
    token = tracks.next_token

    {token, %{tracks | next_token: token + 1, tokens: BiMap.put(tracks.tokens, token, pad)}}
  end

  @spec remove_pad(t(), Membrane.Pad.ref()) :: {token() | nil, t()}
  def remove_pad(tracks, pad) do
    case BiMap.get_key(tracks.tokens, pad) do
      nil -> {nil, tracks}
      token -> {token, drop(tracks, token)}
    end
  end

  @spec pad_for(t(), token()) :: Membrane.Pad.ref() | nil
  def pad_for(tracks, token), do: BiMap.get(tracks.tokens, token)

  @spec rendition(t(), String.t()) :: Native.rendition() | nil
  def rendition(tracks, track), do: tracks.renditions[track]

  @spec waiting(t()) :: [{token(), Membrane.Pad.ref()}]
  def waiting(tracks) do
    for {token, pad} <- tracks.tokens,
        not MapSet.member?(tracks.active, token),
        do: {token, pad}
  end

  @spec activate(t(), token()) :: t()
  def activate(tracks, token) do
    %{tracks | active: MapSet.put(tracks.active, token)}
  end

  @spec deactivate(t(), token()) :: t()
  def deactivate(tracks, token) do
    %{tracks | active: MapSet.delete(tracks.active, token)}
  end

  @spec apply_snapshot(
          t(),
          [{String.t(), Native.rendition()}],
          (Membrane.Pad.ref() -> String.t())
        ) :: {snapshot_diff(), t()}
  def apply_snapshot(tracks, renditions, track_of) do
    new_renditions = Map.new(renditions)
    {removed, added, changed} = diff(tracks.renditions, new_renditions)

    changed_set = MapSet.new(changed)

    ended =
      for {token, pad} <- tracks.tokens,
          MapSet.member?(tracks.active, token),
          MapSet.member?(changed_set, track_of.(pad)),
          do: {token, pad}

    tracks = %{tracks | renditions: new_renditions}
    tracks = Enum.reduce(ended, tracks, fn {token, _pad}, tracks -> drop(tracks, token) end)

    {%{removed: removed, added: added, changed: changed, ended: ended}, tracks}
  end

  @spec diff(renditions, renditions) ::
          {removed :: [String.t()], added :: [String.t()], changed :: [String.t()]}
        when renditions: %{String.t() => Native.rendition()}
  defp diff(old, new) do
    removed = for {name, _rendition} <- old, not is_map_key(new, name), do: name
    added = for {name, _rendition} <- new, not is_map_key(old, name), do: name

    changed =
      for {name, rendition} <- new,
          is_map_key(old, name),
          Map.fetch!(old, name) != rendition,
          do: name

    {removed, added, changed}
  end

  @spec drop(t(), token()) :: t()
  defp drop(tracks, token) do
    %{
      tracks
      | tokens: BiMap.delete_key(tracks.tokens, token),
        active: MapSet.delete(tracks.active, token)
    }
  end
end
