defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  alias Indexed.{Entity, UniquesBundle, View}
  alias __MODULE__

  @ets_opts [read_concurrency: true]

  @typedoc "A record map being cached & indexed. `:id` key is required."
  @type record :: map

  @typedoc "The value of a record's `:id` field - usually a UUID or integer."
  @type id :: any

  @typedoc """
  Specifies a discrete data set of an entity, pre-partitioned into a group.
  A tuple indicates a field name and value which must match, a string
  indicates a view fingerprint, and `nil` means the full data set.
  """
  @type prefilter :: {atom, any} | String.t() | nil

  @typedoc """
  A function which takes a record and returns a value which will be evaluated
  for truthiness. If true, the value will be included in the result set.
  """
  @type filter :: (record -> any)

  @typedoc "Map held in ETS - tracks all views and their created timestamps."
  @type views :: %{String.t() => DateTime.t()}

  defstruct entities: %{}, index_ref: nil

  @typedoc """
  * `:entities` - Map of entity name keys to `t:Indexed.Entity.t/0`
  * `:index_ref` - ETS table reference for the indexes.
  """
  @type t :: %__MODULE__{
          entities: %{atom => Entity.t()},
          index_ref: :ets.tid()
        }

  defdelegate warm(args), to: Indexed.Actions.Warm, as: :run
  defdelegate put(index, entity_name, record), to: Indexed.Actions.Put, as: :run
  defdelegate drop(index, entity_name, id), to: Indexed.Actions.Drop, as: :run
  defdelegate create_view(index, entity_name, fp, opts), to: Indexed.Actions.CreateView, as: :run
  defdelegate destroy_view(index, entity_name, fp), to: Indexed.Actions.DestroyView, as: :run
  defdelegate paginate(index, entity_name, params), to: Indexed.Actions.Paginate, as: :run

  @doc "Get the ETS options to be used for any and all tables."
  @spec ets_opts :: keyword
  def ets_opts, do: @ets_opts

  @doc "Get an entity by id from the index."
  @spec get(t, atom, id, any) :: any
  def get(index, entity_name, id, default \\ nil) do
    case :ets.lookup(Map.fetch!(index.entities, entity_name).ref, id) do
      [{^id, val}] -> val
      [] -> default
    end
  end

  @doc "Get an index data structure."
  @spec get_index(t, atom, prefilter, atom, :asc | :desc) :: list | map | nil
  def get_index(index, entity_name, prefilter, order_field, order_dir) do
    get_index(index, index_key(entity_name, prefilter, order_field, order_dir))
  end

  @doc "Get an index data structure by key."
  @spec get_index(Indexed.t(), String.t(), any) :: list | map | nil
  def get_index(index, index_key, default \\ nil) do
    case :ets.lookup(index.index_ref, index_key) do
      [{^index_key, val}] -> val
      [] -> default
    end
  end

  @doc """
  For the given data set, get a list (sorted ascending) of unique values for
  `field_name` under `entity_name`. Returns `nil` if no data is found.
  """
  @spec get_uniques_list(t, atom, prefilter, atom) :: list | nil
  def get_uniques_list(index, entity_name, prefilter, field_name) do
    get_index(index, uniques_list_key(entity_name, prefilter, field_name))
  end

  @doc """
  For the given `prefilter`, get a map where keys are unique values for
  `field_name` under `entity_name` and vals are occurrence counts. Returns
  `nil` if no data is found.
  """
  @spec get_uniques_map(t, atom, prefilter, atom) :: UniquesBundle.counts_map() | nil
  def get_uniques_map(index, entity_name, prefilter, field_name) do
    get_index(index, uniques_map_key(entity_name, prefilter, field_name))
  end

  @doc """
  Get a list of all cached records of a certain type.

  `prefilter` - 2-element tuple (`t:prefilter/0`) indicating which
  sub-section of the data should be queried. Default is `nil` - no prefilter.
  """
  @spec get_records(t, atom, prefilter, atom, :asc | :desc) :: [record] | nil
  def get_records(index, entity_name, prefilter, order_field, order_direction) do
    with records when is_list(records) <-
           get_index(index, entity_name, prefilter, order_field, order_direction) do
      Enum.map(records, &get(index, entity_name, &1))
    end
  end

  @doc "Cache key for a given entity, field and direction."
  @spec index_key(atom, prefilter, atom, :asc | :desc) :: String.t()
  def index_key(entity_name, prefilter, field_name, direction) do
    "idx_#{entity_name}#{prefilter_id(prefilter)}#{field_name}_#{direction}"
  end

  @doc """
  Cache key holding unique values & counts for a given entity and field.
  """
  @spec uniques_map_key(atom, prefilter, atom) :: String.t()
  def uniques_map_key(entity_name, prefilter, field_name) do
    "uniques_map_#{entity_name}#{prefilter_id(prefilter)}#{field_name}"
  end

  @doc "Cache key holding unique values for a given entity and field."
  @spec uniques_list_key(atom, prefilter, atom) :: String.t()
  def uniques_list_key(entity_name, prefilter, field_name) do
    "uniques_list_#{entity_name}#{prefilter_id(prefilter)}#{field_name}"
  end

  @doc "Cache key holding `t:views/0` for a certain entity."
  @spec views_key(atom) :: String.t()
  def views_key(entity_name), do: "views_#{entity_name}"

  @doc "Get a map of fingerprints to view structs (view metadata)."
  @spec get_views(t, atom) :: %{View.fingerprint() => View.t()}
  def get_views(index, entity_name) do
    get_index(index, views_key(entity_name)) || %{}
  end

  @doc "Get a particular view struct (view metadata) by its fingerprint."
  @spec get_view(t, atom, View.fingerprint()) :: View.t() | nil
  def get_view(index, entity_name, fingerprint) do
    with %{} = views <- get_views(index, entity_name) do
      Map.get(views, fingerprint)
    end
  end

  # Create a piece of an ETS table key to identify the set being stored.
  @spec prefilter_id(prefilter) :: String.t()
  defp prefilter_id({k, v}), do: "[#{k}=#{v}]"
  defp prefilter_id(fp) when is_binary(fp), do: "<#{fp}>"
  defp prefilter_id(_), do: "[]"
end
