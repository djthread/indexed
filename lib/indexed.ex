defimpl Inspect, for: Indexed do
  def inspect(_state, _opts) do
    "#Indexed<>"
  end
end

defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  alias Indexed.View
  alias __MODULE__

  # Baseline opts. Others such as :named_table may be added.
  @ets_opts [read_concurrency: true]

  defstruct entities: %{}, index_ref: nil

  @typedoc """
  * `:entities` - Map of entity name keys to `t:Indexed.Entity.t/0`
  * `:index_ref` - ETS table reference for the indexes. This will be nil in the
    version compiled into a managed module. If nil, a named table is used.
  """
  # * `:namespace` - If non-nil, named ETS tables are on and prefixed with this.
  @type t :: %Indexed{
          entities: %{optional(atom) => Indexed.Entity.t()},
          index_ref: :ets.tid() | nil
          # namespace: namespace | nil
        }

  @typedoc "A record map being cached & indexed. `:id` key is required."
  @type record :: map

  @typedoc "The value of a record's `:id` field - usually a UUID or integer."
  @type id :: any

  @typedoc "A value to prefix ETS table names."
  @type namespace :: atom

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

  @typedoc "A parameter to indicate a sort field and optionally direction."
  @type order_hint ::
          atom | {direction :: :asc | :desc, field_name :: atom} | [{:asc | :desc, atom}]

  @typedoc """
  Either an indexed struct or a module where we could get the struct by calling
  `__index__/0`."
  """
  @type index_or_module :: t | module

  defdelegate warm(args), to: Indexed.Actions.Warm, as: :run
  defdelegate put(index, entity_name, record), to: Indexed.Actions.Put, as: :run
  defdelegate drop(index, entity_name, id), to: Indexed.Actions.Drop, as: :run
  defdelegate create_view(index, entity_name, fp, opts), to: Indexed.Actions.CreateView, as: :run
  defdelegate destroy_view(index, entity_name, fp), to: Indexed.Actions.DestroyView, as: :run

  if Code.ensure_loaded?(Paginator) do
    defdelegate paginate(index, entity_name, params), to: Indexed.Actions.Paginate, as: :run
  end

  @doc "Get the ETS options to be used for any and all tables."
  @spec ets_opts(namespace | nil) :: keyword
  def ets_opts(nil), do: @ets_opts
  def ets_opts(_), do: [:named_table | @ets_opts]

  @doc "Get an entity by id from the index."
  @spec get(index_or_namespace :: t | atom, atom, id, any) :: any
  def get(index_or_ns, entity_name, id, default \\ nil)

  def get(%{} = index_or_ns, entity_name, id, default) do
    case :ets.lookup(Map.fetch!(index_or_ns.entities, entity_name).ref, id) do
      [{^id, val}] -> val
      [] -> default
    end
  end

  def get(index_or_ns, entity_name, id, default) do
    case :ets.lookup(table_name(index_or_ns, entity_name), id) do
      [{^id, val}] -> val
      [] -> default
    end
  end

  @doc "Build an ETS table name from its namespace and entity_name."
  @spec table_name(namespace | nil, atom) :: atom
  def table_name(namespace \\ nil, entity_name \\ nil)
  def table_name(nil, nil), do: :indexed
  def table_name(nil, entity_name), do: entity_name
  def table_name(namespace, nil), do: :"#{namespace}"
  def table_name(namespace, entity_name), do: :"#{namespace}:#{entity_name}"

  @doc "Get an index data structure."
  @spec get_index(t, atom, prefilter, order_hint | nil) :: list | map | nil
  def get_index(index, entity_name, prefilter, order_hint) when is_atom(entity_name) do
    order_hint = order_hint || default_order_hint(index, entity_name)
    get_index(index, index_key(entity_name, prefilter, order_hint))
  end

  @doc "Get an index data structure by key."
  @spec get_index(index_or_module, String.t(), any) :: any
  def get_index(iom, index_key, default \\ nil) do
    {iom, ref(iom), index_key} |> IO.inspect(label: "get_index")

    case :ets.lookup(ref(iom), index_key) do
      [{^index_key, val}] -> val
      [] -> default
    end
  end

  @doc """
  Get an ETS table reference or name for an entity name.
  Default with no `entity_name` is indexes table ref.
  """
  @spec ref(index_or_module, atom) :: atom | :ets.tid()
  def ref(iom, entity_name \\ nil)
  def ref(%{index_ref: ref, namespace: nil}, nil), do: ref
  def ref(%{namespace: ns}, nil), do: table_name(ns)
  def ref(mod, nil), do: table_name(mod.namespace)

  def ref(%{entities: entities, namespace: nil}, entity_name) do
    %{^entity_name => %{ref: ref}} = entities
    ref
  end

  def ref(%{namespace: ns}, entity_name), do: table_name(ns, entity_name)
  def ref(mod, entity_name), do: table_name(mod.__namespace__, entity_name)

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
  @spec get_uniques_map(t, atom, prefilter, atom) :: Indexed.UniquesBundle.counts_map() | nil
  def get_uniques_map(index, entity_name, prefilter, field_name) do
    get_index(index, uniques_map_key(entity_name, prefilter, field_name))
  end

  @doc """
  Get a list of all cached records of a certain type.

  `prefilter` - 2-element tuple (`t:prefilter/0`) indicating which
  sub-section of the data should be queried. Default is `nil` - no prefilter.
  """
  @spec get_records(t, atom, prefilter, order_hint | nil) :: [record] | nil
  def get_records(index, entity_name, prefilter, order_hint \\ nil) do
    k = &Access.key(&1)

    order_hint =
      order_hint ||
        index |> get_in([k.(:entities), entity_name, k.(:fields)]) |> hd() |> elem(0)

    with records when is_list(records) <- get_index(index, entity_name, prefilter, order_hint) do
      Enum.map(records, &get(index, entity_name, &1))
    end
  end

  @doc "Cache key for a given entity, field and direction."
  @spec index_key(atom, prefilter, order_hint) :: String.t()
  def index_key(entity_name, prefilter, order_hint) do
    sort_str =
      order_hint
      |> Indexed.Helpers.normalize_order_hint()
      |> Enum.map_join(",", fn {d, n} -> "#{d}_#{n}" end)

    "idx_#{entity_name}#{prefilter_id(prefilter)}#{sort_str}"
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

  @doc """
  Get the name of the first indexed field for an entity.
  Good order_hint default.
  """
  @spec default_order_hint(t, atom) :: atom
  def default_order_hint(index, entity_name) do
    k = &Access.key(&1)
    index |> get_in([k.(:entities), entity_name, k.(:fields)]) |> hd() |> elem(0)
  end
end
