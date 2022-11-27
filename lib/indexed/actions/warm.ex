defmodule Indexed.Actions.Warm do
  @moduledoc "Holds internal state info during operations."
  import Indexed.Helpers, only: [add_to_lookup: 4, id: 2]
  alias Indexed.{Entity, UniquesBundle}
  alias __MODULE__
  require Logger

  defstruct [:data_tuple, :entity_name, :id_key, :index_ref]

  @typedoc """
  * `:data_tuple` - full input data set with order/direction hint
  * `:entity_name` - entity name atom (eg. `:cars`)
  * `:id_key` - Specifies how to find the id for a record.
    See `t:Indexed.Entity.t/0`.
  * `:index_ref` - ETS table reference for storing index data or an atom for a
    named table.
  """
  @type t :: %Warm{
          data_tuple: data_tuple,
          entity_name: atom,
          id_key: any,
          index_ref: atom | :ets.tid()
        }

  @typedoc """
  A list of records, wrapped in a hint about a field and direction it's
  already sorted by.
  """
  @type data_tuple :: {sort_dir :: :asc | :desc, sort_field :: atom, [Indexed.record()]}

  @typedoc """
  Data option for warming the cache. If data_tuple, the given ordering is
  explicit and we can assume it's correct and skip a sorting routine.
  """
  @type data_opt :: data_tuple | Indexed.record() | [Indexed.record()] | nil

  @doc """
  For a set of entities, load data and indexes to ETS for each.

  Argument is a keyword list where the top level has:

  * `:entities` - A keyword list, configuring each entity to be stored.
    One or more of these are expected. Options within listed below.
  * `:namespace` - Atom to prefix the ETS table names with. If nil (default)
    then named tables will not be used.

  OR if the `:entities` key is not present, the whole keyword list is presumed
  to be the option.

  ## Entity Options

  The `:entities` keyword list main option described above has entity name atoms
  as keys and keyword lists of options are values. Allowed options are as
  follows:

  * `:data` - List of maps (with id key) -- the data to index and cache.
    Required. May take one of the following forms:
    * `{direction, field, list}` - data `list`, with a hint that it is already
      sorted by field (atom) and direction (:asc or :desc), `t:data_tuple/0`.
    * `list` - data list with unknown ordering; must be sorted for every field.
  * `:fields` - List of field name atoms to index by. At least one required.
    * If field is a DateTime, use sort: `{:my_field, sort: :date_time}`.
    * Ascending and descending will be indexed for each field.
  * `:id_key` - Primary key to use in indexes and for accessing the records of
    this entity.  See `t:Indexed.Entity.t/0`. Default: `:id`.
  * `:lookups` - See `Indexed.Entity.t/0`.
  * `:namespace` - Atom name of the ETS table when a named table is
    desired. Useful for accessing the data in a process without the table ref.
    When this is non-nil, named tables will be used instead of references and
    the namespace is used as a prefix for them.
  * `:prefilters` - List of field name atoms which should be prefiltered on.
    This means that separate indexes will be managed for each unique value for
    each of these fields, across all records of this entity type. While field
    name `nil` refers to the indexes where no prefilter is used (all records)
    and it is included by default, it may be defined in the arguments if
    further options are needed. Default `[{nil, []}]`. If options are needed,
    2-element tuples may be used in place of the atom where the the first
    element is the field name atom, and the second is a keyword list of any
    of the following options:
    * `:maintain_unique` - List of field name atoms for which a list of unique
      values under the prefilter will be managed. If the `nil` prefilter is
      defined, leave the other prefilter fields off the `:maintain_unique`
      option as these are automatically included. These lists can be fetched
      via `get_uniques_list/4`.
  """
  @spec run(keyword) :: Indexed.t()
  def run(args \\ []) do
    ns = args[:namespace]
    ets_opts = Indexed.ets_opts(ns)
    index_ref = :ets.new(Indexed.table_name(ns), ets_opts)

    entities =
      Map.new(args[:entities] || args, fn {entity_name, opts} ->
        ref =
          ns
          |> Indexed.table_name(entity_name)
          |> :ets.new(ets_opts)

        fields = resolve_fields_opt(opts[:fields], entity_name)
        id_key = opts[:id_key] || :id
        lookups = opts[:lookups] || []
        prefilter_configs = resolve_prefilters_opt(opts[:prefilters])

        {_dir, _field, full_data} =
          data_tuple = resolve_data_opt(opts[:data], entity_name, fields)

        # Load the records into ETS, keyed by :id or the :id_key field.
        Enum.each(full_data, &:ets.insert(ref, {id(&1, id_key), &1}))

        warm = %Warm{
          data_tuple: data_tuple,
          entity_name: entity_name,
          id_key: id_key,
          index_ref: index_ref
        }

        Logger.debug("Warming #{entity_name}...")

        # Create and insert the caches for this entity: for each prefilter
        # configured, build & store indexes for each indexed field.
        # Internally, a `t:prefilter/0` refers to a `{:my_field, "possible
        # value"}` tuple or `nil` which we implicitly include, where no
        # prefilter is applied.
        for prefilter_config <- prefilter_configs,
            do: warm_prefilter(warm, prefilter_config, fields)

        # Create lookups: %{"Some Field Value" => [123, 456]}
        for field_name <- lookups,
            do: warm_index(warm, field_name)

        {entity_name,
         %Entity{
           fields: fields,
           id_key: id_key,
           lookups: lookups,
           prefilters: prefilter_configs,
           ref: ref
         }}
      end)

    %Indexed{entities: entities, index_ref: index_ref}
  end

  # %{"Some Field Value" => [123, 456]}
  defp warm_index(%{data_tuple: {_, _, records}} = warm, field) do
    lookup =
      Enum.reduce(records, %{}, fn record, acc ->
        add_to_lookup(acc, record, field, id(record, warm.id_key))
      end)

    key = Indexed.lookup_key(warm.entity_name, field)

    :ets.insert(warm.index_ref, {key, lookup})
  end

  # If `pf_key` is nil, then we're warming the full set -- no prefilter.
  #   In this case, load indexes for each field.
  # If `pf_key` is a field name atom to prefilter on, then group the given data
  #   by that field. For each grouping, a full set of indexes for each
  #   field/value pair will be created. Unique values list is updated, too.
  @spec warm_prefilter(Warm.t(), Entity.prefilter_config(), [Entity.field()]) :: :ok
  defp warm_prefilter(warm, {pf_key, pf_opts}, fields) do
    %{data_tuple: {d_dir, d_name, full_data}, entity_name: entity_name, index_ref: index_ref} =
      warm

    warm_sorted = fn prefilter, field, data ->
      data_tuple = {d_dir, d_name, data}
      warm_sorted(warm, prefilter, field, data_tuple)
    end

    Logger.debug("""
      * Putting index (PF #{pf_key || "nil"}) for \
    #{inspect(Enum.map(fields, &elem(&1, 0)))}\
    """)

    if is_nil(pf_key) do
      Enum.each(fields, &warm_sorted.(nil, &1, full_data))

      # Store :maintain_unique fields on the nil prefilter. Other prefilters
      # imply a unique index and are handled when they are processed below.
      store_all_uniques(index_ref, entity_name, nil, pf_opts, full_data)
    else
      grouped = Enum.group_by(full_data, &Map.get(&1, pf_key))

      # Prepare & store list of no-prefilter uniques for this field.
      # (Remember that prefilter fields imply :maintain_unique on the nil
      # prefilter since these are needed in order to know what is useful to
      # pass into `get_uniques_list/4`.)
      {counts_map, list} =
        Enum.reduce(grouped, {%{}, []}, fn {pf_val, records}, {counts_map, list} ->
          {Map.put(counts_map, pf_val, length(records)), [pf_val | list]}
        end)

      bundle = UniquesBundle.new(counts_map, Enum.sort(Enum.uniq(list)))
      UniquesBundle.put(bundle, index_ref, entity_name, nil, pf_key, new?: true)

      Logger.debug("--> Putting UB (for #{pf_key}) with #{map_size(counts_map)} elements.")

      # For each value found for the prefilter, create a set of indexes.
      Enum.each(grouped, fn {pf_val, data} ->
        prefilter = {pf_key, pf_val}
        Enum.each(fields, &warm_sorted.(prefilter, &1, data))
        store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data)
      end)
    end
  end

  @doc "Create the asc and desc indexes for one field."
  @spec warm_sorted(t, Indexed.prefilter(), Entity.field(), data_tuple()) :: true
  # Data direction hint matches this field -- no need to sort.
  def warm_sorted(warm, prefilter, {name, _sort_hint}, {data_dir, name, data}) do
    data_ids = id_list(data, warm.id_key)

    asc_key = Indexed.index_key(warm.entity_name, prefilter, name)
    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    desc_key = Indexed.index_key(warm.entity_name, prefilter, {:desc, name})
    desc_ids = if data_dir == :desc, do: data_ids, else: Enum.reverse(data_ids)

    :ets.insert(warm.index_ref, {asc_key, asc_ids})
    :ets.insert(warm.index_ref, {desc_key, desc_ids})
  end

  # Data direction hint does NOT match this field -- sorting needed.
  def warm_sorted(warm, prefilter, {name, _} = field, {_, _, data}) do
    asc_key = Indexed.index_key(warm.entity_name, prefilter, name)
    desc_key = Indexed.index_key(warm.entity_name, prefilter, {:desc, name})
    asc_ids = data |> Enum.sort(Warm.record_sort_fn(field)) |> id_list(warm.id_key)

    :ets.insert(warm.index_ref, {asc_key, asc_ids})
    :ets.insert(warm.index_ref, {desc_key, Enum.reverse(asc_ids)})
  end

  @doc "From a field, make a compare function, suitable for `Enum.sort/2`."
  @spec record_sort_fn(Entity.field()) :: (any, any -> boolean)
  def record_sort_fn({name, opts}) do
    case opts[:sort] do
      :date_time -> &(:lt == DateTime.compare(Map.get(&1, name), Map.get(&2, name)))
      nil -> &(Map.get(&1, name) < Map.get(&2, name))
    end
  end

  # Save list of unique values for each field configured by :maintain_unique.
  @spec store_all_uniques(:ets.tid(), atom, Indexed.prefilter(), keyword, [Indexed.record()]) ::
          :ok
  defp store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      counts_map =
        Enum.reduce(data, %{}, fn record, counts_map ->
          val = Map.get(record, field_name)
          num = Map.get(counts_map, val, 0) + 1
          Map.put(counts_map, val, num)
        end)

      list = counts_map |> Map.keys() |> Enum.sort()
      bundle = UniquesBundle.new(counts_map, list)

      Logger.debug("""
      --> Putting UB (PF #{inspect(prefilter)}, #{field_name}) \
      with #{map_size(counts_map)} elements."\
      """)

      UniquesBundle.put(bundle, index_ref, entity_name, prefilter, field_name, new?: true)
    end)
  end

  @doc "Normalize fields."
  @spec resolve_fields_opt([atom | Entity.field()], atom) :: [Entity.field()]
  def resolve_fields_opt(fields, _entity_name) do
    # match?([_ | _], fields) || raise "At least one field to index is required on #{entity_name}."

    Enum.map(fields, fn
      {_name, _opts} = f -> f
      name when is_atom(name) -> {name, []}
    end)
  end

  @doc "Normalize `warm/1`'s data option."
  @spec resolve_data_opt(data_opt, atom, [Entity.field()]) :: {atom, atom, [Indexed.record()]}
  def resolve_data_opt({dir, name, data}, entity_name, fields)
      when dir in [:asc, :desc] and is_list(data) do
    # If the data hint field isn't even being indexed, raise.
    if Enum.any?(fields, &(elem(&1, 0) == name)),
      do: {dir, name, data},
      else: raise("Field #{name} is not being indexed for #{entity_name}.")
  end

  def resolve_data_opt({d, _, _}, entity_name, _),
    do: raise("Bad input data direction for #{entity_name}: #{d}")

  def resolve_data_opt(data, _, _) when is_list(data), do: {nil, nil, data}

  def resolve_data_opt(data, _, _), do: {nil, nil, [data]}

  @doc """
  Normalize the prefilters option to tuples, adding `nil` prefilter if needed.
  """
  @spec resolve_prefilters_opt([atom | keyword] | nil) :: keyword(keyword)
  def resolve_prefilters_opt(prefilters) do
    prefilters =
      Enum.map(prefilters || [], fn
        {pf, opts} -> {pf, Keyword.take(opts, [:maintain_unique])}
        nil -> raise "Found simple `nil` prefilter. Leave it off unless opts are needed."
        pf -> {pf, []}
      end)

    if Enum.any?(prefilters, &match?({nil, _}, &1)),
      do: prefilters,
      else: [{nil, []} | prefilters]
  end

  @doc "Return a list of all ids from the `collection`."
  @spec id_list([Indexed.record()], any) :: [Indexed.id()]
  def id_list(collection, id_key) do
    Enum.map(collection, &id(&1, id_key))
  end
end
