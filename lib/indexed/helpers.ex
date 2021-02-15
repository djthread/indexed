defmodule Indexed.Helpers do
  @moduledoc "Indexed tools for internal use."
  alias Indexed.Entity

  @doc "Normalize fields."
  @spec resolve_fields_opt([atom | Indexed.field_config()], atom) :: [Indexed.field_config()]
  def resolve_fields_opt(fields, entity_name) do
    match?([_ | _], fields) || raise "At least one field to index is required on #{entity_name}."

    Enum.map(fields, fn
      {_name, _opts} = f -> f
      name when is_atom(name) -> {name, []}
    end)
  end

  @doc "Normalize `warm/1`'s data option."
  @spec resolve_data_opt({atom, atom, [Indexed.record()]} | [Indexed.record()] | nil, atom, [
          Entity.field()
        ]) :: {atom, atom, [Indexed.record()]}
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

  @doc "Store two indexes for unique value tracking."
  @spec put_uniques_bundle(Indexed.uniques_bundle(), :ets.tid(), atom, Indexed.prefilter(), atom) ::
          true
  def put_uniques_bundle(
        {counts_map, list, list_updated?},
        index_ref,
        entity_name,
        prefilter,
        field_name
      ) do
    if list_updated? do
      list_key = unique_values_key(entity_name, prefilter, field_name, :list)
      :ets.insert(index_ref, {list_key, list})
    end

    counts_key = unique_values_key(entity_name, prefilter, field_name, :counts)
    :ets.insert(index_ref, {counts_key, counts_map})
  end

  @doc "Create the asc and desc indexes for one field."
  @spec warm_index(:ets.tid(), atom, Indexed.prefilter(), Entity.field(), Indexed.data_tuple()) ::
          true
  # Data direction hint matches this field -- no need to sort.
  def warm_index(ref, entity_name, prefilter, {name, _sort_hint}, {data_dir, name, data}) do
    data_ids = id_list(data)

    asc_key = index_key(entity_name, name, :asc, prefilter)
    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    desc_key = index_key(entity_name, name, :desc, prefilter)
    desc_ids = if data_dir == :desc, do: data_ids, else: Enum.reverse(data_ids)

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, desc_ids})
  end

  # Data direction hint does NOT match this field -- sorting needed.
  def warm_index(ref, entity_name, prefilter, {name, opts}, {_, _, data}) do
    sort_fn =
      case opts[:sort] do
        :date_time -> &(:lt == DateTime.compare(Map.get(&1, name), Map.get(&2, name)))
        nil -> &(Map.get(&1, name) < Map.get(&2, name))
      end

    asc_key = index_key(entity_name, name, :asc, prefilter)
    desc_key = index_key(entity_name, name, :desc, prefilter)
    asc_ids = data |> Enum.sort(sort_fn) |> id_list()

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, Enum.reverse(asc_ids)})
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

  @doc "Return a list of all `:id` elements from the `collection`."
  @spec id_list([Indexed.record()]) :: [Indexed.id()]
  def id_list(collection) do
    Enum.map(collection, & &1.id)
  end

  @doc "Get an index data structure by key (see `index_key/4`)."
  @spec get_index(Indexed.t(), String.t()) :: list | map
  def get_index(index, index_name) do
    case :ets.lookup(index.index_ref, index_name) do
      [{^index_name, val}] -> val
      [] -> raise "No such index: #{index_name}"
    end
  end

  # Get counts_map and list versions of a unique values list at the same time.
  @spec get_uniques_bundle(Indexed.t(), atom, atom, Indexed.prefilter()) ::
          Indexed.uniques_bundle()
  def get_uniques_bundle(index, entity_name, field_name, prefilter) do
    map = Indexed.get_uniques_map(index, entity_name, field_name, prefilter)
    list = Indexed.get_uniques_list(index, entity_name, field_name, prefilter)
    {map, list, false}
  end

  @doc "Insert a record into the cached data. (Indexes still need updating.)"
  @spec put(Indexed.t(), atom, Indexed.record()) :: true
  def put(index, entity_name, %{id: id} = record) do
    :ets.insert(Map.fetch!(index.entities, entity_name).ref, {id, record})
  end

  @doc """
  Remove `value` from the `field_name` unique values tally for the given
  entity/prefilter.
  """
  @spec remove_unique(Indexed.uniques_bundle(), any) :: Indexed.uniques_bundle()
  def remove_unique({counts_map, list, list_updated?}, value) do
    case Map.fetch!(counts_map, value) do
      1 -> {Map.delete(counts_map, value), list -- [value], true}
      n -> {Map.put(counts_map, value, n - 1), list, list_updated?}
    end
  end

  # Add a value to the uniques: in ETS and in the returned bundle.
  @spec add_unique(Indexed.uniques_bundle(), any) :: Indexed.uniques_bundle()
  def add_unique({counts_map, list, list_updated?}, value) do
    case counts_map[value] do
      nil ->
        first_bigger_idx = Enum.find_index(list, &(&1 > value))
        new_list = List.insert_at(list, first_bigger_idx || 0, value)
        new_counts_map = Map.put(counts_map, value, 1)
        {new_counts_map, new_list, true}

      orig_count ->
        {Map.put(counts_map, value, orig_count + 1), list, list_updated?}
    end
  end

  # Add the id of `record` to the list of descending ids, sorting by `field`.
  @spec insert_by([Indexed.id()], Indexed.record(), atom, Entity.field(), Indexed.t()) :: [
          Indexed.id()
        ]
  def insert_by(old_desc_ids, record, entity_name, {name, opts}, index) do
    find_fun =
      case opts[:sort] do
        :date_time ->
          fn id ->
            val = Map.get(Indexed.get(index, entity_name, id), name)
            :lt == DateTime.compare(val, Map.get(record, name))
          end

        nil ->
          this_value = Map.get(record, name)
          &(Map.get(Indexed.get(index, entity_name, &1), name) < this_value)
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, record.id)
  end
end
