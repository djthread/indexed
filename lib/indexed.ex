defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  alias Indexed.{Entity, UniquesBundle}
  alias __MODULE__

  @typedoc "A record map being cached & indexed. `:id` key is required."
  @type record :: %{required(:id) => any}

  @typedoc "The value of a record's `:id` field - usually a UUID or integer."
  @type id :: any

  @typedoc """
  Field name and value for which separate indexes for each field should be
  kept. Note that these are made in conjunction with `get_uniques_list/4`
  and are not kept in state.
  """
  @type prefilter :: {atom, any} | nil

  defstruct entities: %{}, index_ref: nil

  @typedoc """
  * `:entities` - Map of entity name keys to `t:Indexed.Entity.t/0`
  * `:index_ref` - ETS table reference for the indexes.
  """
  @type t :: %__MODULE__{
          entities: %{atom => Entity.t()},
          index_ref: :ets.tid()
        }

  defdelegate put(index, entity_name, record), to: Indexed.Actions.Put, as: :run
  defdelegate warm(args), to: Indexed.Actions.Warm, as: :run
  defdelegate paginate(index, entity_name, params), to: Indexed.Paginator

  @doc "Get an entity by id from the index."
  @spec get(t, atom, id) :: any
  def get(index, entity_name, id) do
    case :ets.lookup(Map.fetch!(index.entities, entity_name).ref, id) do
      [{^id, val}] -> val
      [] -> nil
    end
  end

  @doc "Get an index data structure."
  @spec get_index(t, atom, atom, :asc | :desc, prefilter) :: list | map | nil
  def get_index(index, entity_name, field_name, direction, prefilter \\ nil) do
    get_index(index, index_key(entity_name, field_name, direction, prefilter))
  end

  @doc """
  For the given `prefilter`, get a list (sorted ascending) of unique values
  for `field_name` under `entity_name`. Returns `nil` if prefilter is
  non-existent.
  """
  @spec get_uniques_list(t, atom, atom, prefilter) :: [any] | nil
  def get_uniques_list(index, entity_name, field_name, prefilter \\ nil) do
    get_index(index, unique_values_key(entity_name, prefilter, field_name, :list))
  end

  @doc """
  For the given `prefilter`, get a map where keys are unique values for
  `field_name` under `entity_name` and vals are occurrence counts. Returns
  `nil` if prefilter is non-existent.
  """
  @spec get_uniques_map(t, atom, atom, prefilter) :: UniquesBundle.counts_map() | nil
  def get_uniques_map(index, entity_name, field_name, prefilter \\ nil) do
    get_index(index, unique_values_key(entity_name, prefilter, field_name, :counts))
  end

  @doc """
  Get a list of all cached entities of a certain type.

  `prefilter` - 2-element tuple (`t:prefilter/0`) indicating which
  sub-section of the data should be queried. Default is `nil` - no prefilter.
  Returns `nil` if prefilter is non-existent.
  """
  @spec get_values(t, atom, atom, :asc | :desc, prefilter) :: [record] | nil
  def get_values(index, entity_name, order_field, order_direction, prefilter \\ nil) do
    index
    |> get_index(entity_name, order_field, order_direction, prefilter)
    |> Enum.map(&get(index, entity_name, &1))
  end

  @doc "Cache key for a given entity, field, direction, and prefilter."
  @spec index_key(atom, atom, :asc | :desc, Indexed.prefilter()) :: String.t()
  def index_key(entity_name, field_name, direction, prefilter \\ nil)

  def index_key(entity_name, field_name, direction, nil) do
    "#{entity_name}[]#{field_name}_#{direction}"
  end

  def index_key(entity_name, field_name, direction, {pf_key, pf_val}) do
    "#{entity_name}[#{pf_key}=#{pf_val}]#{field_name}_#{direction}"
  end

  @doc """
  Cache key holding unique values for a given entity, field, and prefilter.
  """
  @spec unique_values_key(atom, Indexed.prefilter(), atom, :counts | :list) :: String.t()
  def unique_values_key(entity_name, nil, field_name, mode) do
    "unique_#{mode}_#{entity_name}[]#{field_name}"
  end

  def unique_values_key(entity_name, {pf_key, pf_val}, field_name, mode) do
    "unique_#{mode}_#{entity_name}[#{pf_key}=#{pf_val}]#{field_name}"
  end

  @doc "Get an index data structure by key (see `index_key/4`)."
  @spec get_index(Indexed.t(), String.t(), any) :: list | map
  def get_index(index, index_name, default \\ nil) do
    case :ets.lookup(index.index_ref, index_name) do
      [{^index_name, val}] -> val
      [] -> default
    end
  end
end
