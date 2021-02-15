defmodule Indexed.Entity do
  @moduledoc "Configuration for a type of thing to be indexed."
  defstruct fields: [], prefilters: [], ref: nil

  @typedoc """
  * `:fields` - List of `t:field/0`s to be indexed for this entity.
  * `:prefilters` - List of tuples indicating which fields should be
    prefiltered on. This means that separate indexes will be managed for each
    unique value for each of these fields, across all records of this entity
    type. Each two-element tuple has the field name atom and a keyword list
    of options. Allowed options:
    * `:maintain_unique` - List of field name atoms for which a list of
      unique values under the prefilter will be managed. These lists can be
      fetched via `get_uniques_list/4` and `get_uniques_map/4`.
  * `:ref` - ETS table reference where records of this entity type are
    stored, keyed by id.
  """
  @type t :: %__MODULE__{
          fields: [field],
          prefilters: [prefilter_config],
          ref: :ets.tid()
        }

  @typedoc """
  A field to be indexed. 2-element tuple has the field name, followed by a
  sorting strategy, :date or nil for simple sort.

  ## Options

  * `:sort` - Indicates how the field should be sorted in ascending order:
    * `:date_time` - `DateTime.compare/2` should be used for sorting.
    * `nil` (default) - `Enum.sort/1` will be used.
  """
  @type field :: {name :: atom, opts :: keyword}

  @typedoc "Configuration info for a prefilter."
  @type prefilter_config :: {atom, opts :: keyword}
end
