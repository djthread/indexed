defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """

  @typep id :: any
  @typep record :: map

  @ets_opts [read_concurrency: true]

  defdelegate paginate(index, entity, params), to: Indexed.Paginator

  defmodule Index do
    @moduledoc """
    A struct defining a cache, including references to its ETS tables.
    """
    defstruct fields: %{}, index_ref: nil, refs: %{}

    @typedoc """
    * `:fields` - Map with entity name keys mapped to lists of fields. These are
      the fields that will be indexed for each entity.
    * `:index_ref` - ETS table reference for the indexes.
    * `:refs` - Map with entity name keys mapped to ETS table references where
      the records are stored, keyed by id.
    """
    @type t :: %__MODULE__{
            fields: %{atom => [field]},
            index_ref: reference,
            refs: %{atom => reference}
          }

    @typedoc """
    A field to be indexed. 2-element tuple has the field name, followed by a
    sorting strategy, :date or nil for simple sort.
    """
    @type field :: {atom, :date | nil}
  end

  @doc """
  For a set of entities, load data and indexes to ETS for each.

  Argument is a keyword list where entity name atoms are keys and keyword
  lists of options are values. Allowed options are as follows:

  * `:data` - list of maps (with id key) -- the data to index and cache
    * `{field, direction, list}` - data list, with a hint that it is already
      sorted by field (atom) and direction (:asc or :desc)
    * `list` - data list with unknown ordering; must be sorted for every field.
  * `:fields` - list of field name atoms to index by. (Ascending and descending
    will be indexed for each.)
  """
  @spec warm(keyword) :: struct
  def warm(args) do
    index_ref = :ets.new(:indexes, @ets_opts)

    Enum.reduce(args, %Index{}, fn {entity, opts}, acc ->
      fields =
        Enum.map(opts[:fields] || [], fn
          {_name, :date} = f -> f
          name -> {name, nil}
        end)

      # Data sub-options indicate the ordering data already has on the way in.
      {data_dir, data_field, data} = resolve_data_opt(opts[:data], entity, fields)

      ref = :ets.new(entity, @ets_opts)

      # Load the records into ETS, keyed by id.
      Enum.each(data, &:ets.insert(ref, {&1.id, &1}))

      # Create and insert the caches for each indexed field of this entity.
      Enum.each(fields, fn field ->
        warm_index(index_ref, entity, field, data, {data_dir, data_field})
      end)

      %{
        acc
        | fields: Map.put(acc.fields, entity, fields),
          index_ref: index_ref,
          refs: Map.put(acc.refs, entity, ref)
      }
    end)
  end

  # Interpret `warm/1`'s data option.
  @spec resolve_data_opt({atom, atom, [record]} | [record] | nil, atom, [Index.field()]) ::
          {atom, atom, [record]}
  defp resolve_data_opt({dir, name, data}, entity, fields)
       when dir in [:asc, :desc] and is_list(data) do
    # If the data hint field isn't even being indexed, raise.
    if Enum.any?(fields, &(elem(&1, 0) == name)),
      do: {dir, name, data},
      else: raise("Field #{name} is not being indexed for #{entity}.")
  end

  defp resolve_data_opt({d, _, _}, entity, _),
    do: raise("Bad input data direction for #{entity}: #{d}")

  defp resolve_data_opt(data, _, _) when is_list(data), do: {nil, nil, data}

  # Create the asc and desc indexes for one field.
  # If the data is already ordered for this field, we can avoid deep sorting.
  @spec warm_index(:ets.tid(), atom, Index.field(), [record], {:asc | :desc | nil, atom | nil}) ::
          true
  # Data direction hint matches this field -- no need to sort.
  defp warm_index(ref, entity, {name, _sort_hint}, data, {data_dir, name}) do
    data_ids = id_list(data)

    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    :ets.insert(ref, {index_key(entity, name, :asc), asc_ids})

    desc_ids = if data_dir == :desc, do: data_ids, else: Enum.reverse(data_ids)
    :ets.insert(ref, {index_key(entity, name, :desc), desc_ids})
  end

  # Data direction hint does NOT match this field -- sorting needed.
  defp warm_index(ref, entity, {name, sort_hint}, data, _input_by) do
    sort_fn =
      case sort_hint do
        :date -> &(:lt == DateTime.compare(Map.get(&1, name), Map.get(&2, name)))
        nil -> &(Map.get(&1, name) < Map.get(&2, name))
      end

    asc_ids = data |> Enum.sort(sort_fn) |> id_list()
    :ets.insert(ref, {index_key(entity, name, :asc), asc_ids})
    :ets.insert(ref, {index_key(entity, name, :desc), Enum.reverse(asc_ids)})
  end

  @doc "Cache key for a given entity, field, direction."
  @spec index_key(atom, atom, :asc | :desc) :: String.t()
  def index_key(entity, field_name, direction) do
    "#{entity}_#{field_name}_#{direction}"
  end

  # Return a list of all `:id` elements from the `collection`.
  @spec id_list([record]) :: [id]
  defp id_list(collection) do
    Enum.map(collection, & &1.id)
  end

  @doc "Get an entity by id from the index."
  @spec get(Index.t(), atom, id) :: any
  def get(index, entity, id) do
    case :ets.lookup(Map.fetch!(index.refs, entity), id) do
      [{^id, val}] -> val
      [] -> nil
    end
  end

  @doc "Get an index data structure."
  @spec get_index(
          Index.t(),
          {entity :: atom, field_name :: atom, direction :: :asc | :desc} | String.t()
        ) :: [id]
  def get_index(index, {entity, field_name, direction}) do
    get_index(index, index_key(entity, field_name, direction))
  end

  def get_index(index, index_name) do
    case :ets.lookup(index.index_ref, index_name) do
      [{^index_name, val}] -> val
      [] -> raise "No such index: #{index_name}"
    end
  end

  @doc "Get a list of all cached entities of a certain type."
  @spec get_values(Index.t(), atom, atom, :asc | :desc) :: [record]
  def get_values(index, entity, order_field, order_direction) do
    id_keyed_map =
      index.refs
      |> Map.fetch!(entity)
      |> :ets.tab2list()
      |> Map.new()

    key = index_key(entity, order_field, order_direction)

    Enum.map(get_index(index, key), &id_keyed_map[&1])
  end

  # Insert a record into the cached data. (Indexes still need updating.)
  @spec put(Index.t(), atom, record) :: true
  defp put(index, entity, %{id: id} = record) do
    :ets.insert(Map.fetch!(index.refs, entity), {id, record})
  end

  # Set an index into ets, overwriting for the key, if need be.
  @spec put_index(Index.t(), String.t(), [id]) :: true
  defp put_index(index, index_name, id_list) do
    :ets.insert(index.index_ref, {index_name, id_list})
  end

  @doc """
  Add or update a record, along with the indexes to reflect the change.

  If it is known for sure whether or not the record was previously held in
  cache, include the `already_held?` argument to speed the operation
  slightly.
  """
  @spec set_record(Index.t(), atom, record, boolean | nil) :: :ok
  def set_record(index, entity, record, already_held? \\ nil) do
    fields = Map.fetch!(index.fields, entity)

    already_held? =
      if is_boolean(already_held?),
        do: already_held?,
        else: not is_nil(get(index, entity, record.id))

    put(index, entity, record)

    Enum.each(fields, fn {name, _sort_hint} = field ->
      desc_key = index_key(entity, name, :desc)
      desc_ids = get_index(index, desc_key)

      # Remove the id from the list if it exists.
      desc_ids =
        if already_held?,
          do: Enum.reject(desc_ids, &(&1 == record.id)),
          else: desc_ids

      desc_ids = insert_by(desc_ids, record, entity, field, index)

      put_index(index, desc_key, desc_ids)
      put_index(index, index_key(entity, name, :asc), Enum.reverse(desc_ids))
    end)
  end

  # Add the id of `record` to the list of descending ids, sorting by `field`.
  @spec insert_by([id], record, atom, Index.field(), Index.t()) :: [id]
  defp insert_by(old_desc_ids, record, entity, {name, sort_hint}, index) do
    find_fun =
      case sort_hint do
        :date ->
          fn id ->
            val = Map.get(get(index, entity, id), name)
            :lt == DateTime.compare(val, Map.get(record, name))
          end

        nil ->
          &(Map.get(get(index, entity, &1), name) < Map.get(record, name))
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, record.id)
  end
end
