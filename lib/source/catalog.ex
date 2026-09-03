defmodule Membrane.MoQ.Source.Catalog do
  @moduledoc false

  alias ExMoQ.Native

  @type diff :: %{
          removed: [Native.track()],
          added: [Native.track()],
          changed: [Native.track()]
        }

  @type t :: %__MODULE__{
          renditions: Native.renditions()
        }

  defstruct renditions: %{}

  @spec update(t(), Native.renditions()) :: {diff(), t()}
  def update(catalog, new) do
    old = catalog.renditions

    removed = for {name, _rendition} <- old, not is_map_key(new, name), do: name
    added = for {name, _rendition} <- new, not is_map_key(old, name), do: name

    changed =
      for {name, new_rendition} <- new,
          is_map_key(old, name),
          old[name] != new_rendition,
          do: name

    {%{removed: removed, added: added, changed: changed}, %{catalog | renditions: new}}
  end

  @spec rendition(t(), Native.track()) :: Native.track_format() | :unrecognized | nil
  def rendition(catalog, track), do: catalog.renditions[track]
end
