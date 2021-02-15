defmodule Indexed do
  @moduledoc """
  Tools for creating an index module.
  """
  import Indexed.Helpers
  alias Indexed.Entity
  alias __MODULE__

  @ets_opts [read_concurrency: true]

  @typedoc """
  A list of records, wrapped in a hint about a field and direction it's
  already sorted by.
  """
  @type data_tuple :: {:asc, atom, [record]} | {:desc, atom, [record]} | {nil, nil, [record]}

  @typedoc """
  Configuration info for a field to be indexed.

  If the field name holds a DateTime, the second element being `:date_time`
  will hint the sorting to use `DateTime.compare/2` as is necessary.

  ## Options

  * `:sort` - Indicates how the field should be sorted in ascending order:
    * `:date_time` - `DateTime.compare/2` should be used for sorting.
    * `nil` (default) - `Enum.sort/1` will be used.
  """
  @type field_config :: {field_name :: atom, opts :: keyword}

  @typedoc "Occurrences of each value (map key) under a prefilter."
  @type counts_map :: %{any => non_neg_integer}

  @typedoc "A record map being cached & indexed. `:id` key is required."
  @type record :: %{required(:id) => any}

  @typedoc "The value of a record's `:id` field - usually a UUID or integer."
  @type id :: any

  @typedoc """
  An aligning counts_map and list of its keys.

  The third argument is a boolean which will be set to `true` if the list has
  been updated and should be saved by `Helpers.put_uniques_bundle/5`. The Map
  is always updated in this function.
  """
  @type uniques_bundle :: {counts_map, list :: [any] | nil, list_updated? :: boolean}

  @typedoc """
  Field name and value for which separate indexes for each field should be
  kept. Note that these are made in conjunction with `get_unique_values/4`
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

  defdelegate paginate(index, entity_name, params), to: Indexed.Paginator

  @doc """
  For a set of entities, load data and indexes to ETS for each.

  Argument is a keyword list where entity name atoms are keys and keyword
  lists of options are values. Allowed options are as follows:

  * `:data` - List of maps (with id key) -- the data to index and cache.
    Required. May take one of the following forms:
    * `{field, direction, list}` - data `list`, with a hint that it is already
      sorted by field (atom) and direction (:asc or :desc), `t:data_tuple/0`.
    * `list` - data list with unknown ordering; must be sorted for every field.
  * `:fields` - List of field name atoms to index by. At least one required.
    * If field is a DateTime, use sort: `{:my_field, sort: :date_time}`.
    * Ascending and descending will be indexed for each field.
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
      via `get_unique_values/4`.
  """
  @spec warm(keyword) :: t
  def warm(args) do
    index_ref = :ets.new(:indexes, @ets_opts)

    entities =
      Map.new(args, fn {entity_name, opts} ->
        ref = :ets.new(entity_name, @ets_opts)
        fields = resolve_fields_opt(opts[:fields], entity_name)
        prefilters = resolve_prefilters_opt(opts[:prefilters])

        {_dir, _field, full_data} =
          data_tuple = resolve_data_opt(opts[:data], entity_name, fields)

        # Load the records into ETS, keyed by id.
        Enum.each(full_data, &:ets.insert(ref, {&1.id, &1}))

        # Create and insert the caches for this entity: for each prefilter
        # configured, build & store indexes for each indexed field.
        # Internally, a `t:prefilter/0` refers to a `{:my_field, "possible
        # value"}` tuple or `nil` which we implicitly include, where no
        # prefilter is applied.
        for prefilter <- prefilters do
          warm_prefilter(index_ref, entity_name, prefilter, fields, data_tuple)
        end

        {entity_name, %Entity{fields: fields, prefilters: prefilters, ref: ref}}
      end)

    %Indexed{entities: entities, index_ref: index_ref}
  end

  # If `pf_key` is nil, then we're warming the full set -- no prefilter.
  #   In this case, load indexes for each field.
  # If `pf_key` is a field name atom to prefilter on, then group the given data
  #   by that field. For each grouping, a full set of indexes for each
  #   field/value pair will be created. Unique values list is updated, too.
  @spec warm_prefilter(:ets.tid(), atom, Entity.prefilter_config(), [Entity.field()], data_tuple) ::
          :ok
  defp warm_prefilter(
         index_ref,
         entity_name,
         {pf_key, pf_opts},
         fields,
         {d_dir, d_name, full_data}
       ) do
    warm_index = fn prefilter, field, data ->
      data_tuple = {d_dir, d_name, data}
      warm_index(index_ref, entity_name, prefilter, field, data_tuple)
    end

    if is_nil(pf_key) do
      Enum.each(fields, &warm_index.(nil, &1, full_data))

      # Store :maintain_unique fields on the nil prefilter. Other prefilters
      # imply a unique index and are handled when they are processed below.
      store_all_uniques(index_ref, entity_name, nil, pf_opts, full_data)
    else
      grouped = Enum.group_by(full_data, &Map.get(&1, pf_key))

      # Prepare & store list of no-prefilter uniques for this field.
      # (Remember that prefilter fields imply :maintain_unique on the nil
      # prefilter since these are needed in order to know what is useful to
      # pass into `get_unique_values/4`.)
      {counts_map, list} =
        Enum.reduce(grouped, {%{}, []}, fn {pf_val, records}, {counts_map, list} ->
          {Map.put(counts_map, pf_val, length(records)), [pf_val | list]}
        end)

      bundle = {counts_map, Enum.sort(Enum.uniq(list)), true}
      put_uniques_bundle(bundle, index_ref, entity_name, nil, pf_key)

      # For each value found for the prefilter, create a set of indexes.
      Enum.each(grouped, fn {pf_val, data} ->
        prefilter = {pf_key, pf_val}
        Enum.each(fields, &warm_index.(prefilter, &1, data))
        store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data)
      end)
    end
  end

  # Save list of unique values for each field configured by :maintain_unique.
  @spec store_all_uniques(:ets.tid(), atom, prefilter, keyword, [record]) :: :ok
  defp store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      {counts_map, list} =
        Enum.reduce(data, {%{}, []}, fn record, {counts_map, list} ->
          val = Map.get(record, field_name)
          num = Map.get(counts_map, val, 0) + 1
          {Map.put(counts_map, val, num), [val | list]}
        end)

      bundle = {counts_map, Enum.sort(Enum.uniq(list)), true}
      put_uniques_bundle(bundle, index_ref, entity_name, prefilter, field_name)
    end)
  end

  @doc "Get an entity by id from the index."
  @spec get(t, atom, id) :: any
  def get(index, entity_name, id) do
    case :ets.lookup(Map.fetch!(index.entities, entity_name).ref, id) do
      [{^id, val}] -> val
      [] -> nil
    end
  end

  @doc "Get an index data structure."
  @spec get_index(t, atom, atom, :asc | :desc, prefilter) :: list | map
  def get_index(index, entity_name, field_name, direction, prefilter \\ nil) do
    get_index(index, index_key(entity_name, field_name, direction, prefilter))
  end

  @doc """
  For the given `prefilter`, get a list (sorted ascending) of unique values
  for `field_name` under `entity_name`.
  """
  @spec get_uniques_list(t, atom, atom, prefilter) :: [any]
  def get_uniques_list(index, entity_name, field_name, prefilter \\ nil) do
    get_index(index, unique_values_key(entity_name, prefilter, field_name, :list))
  end

  @doc """
  For the given `prefilter`, get a map where keys are unique values for
  `field_name` under `entity_name` and vals are occurrence counts.
  """
  @spec get_uniques_map(t, atom, atom, prefilter) :: counts_map
  def get_uniques_map(index, entity_name, field_name, prefilter \\ nil) do
    get_index(index, unique_values_key(entity_name, prefilter, field_name, :counts))
  end

  @doc """
  Get a list of all cached entities of a certain type.

  ## Options

  * `:prefilter` - Field atom given to `warm/1`'s `:prefilter` option,
    indicating which of the entity's prepared, prefiltered list should be
    used. Default is nil - no prefilter.
  """
  @spec get_values(t, atom, atom, :asc | :desc, keyword) :: [record]
  def get_values(index, entity_name, order_field, order_direction, opts \\ []) do
    index
    |> get_index(entity_name, order_field, order_direction, opts[:prefilter])
    |> Enum.map(&get(index, entity_name, &1))
  end

  @doc """
  Add or update a record, along with the indexes to reflect the change.

  ## Options

  * `:new_record?` - If true, it will be assumed that the record was not
    previously held. This can speed the operation a bit.
  """
  @spec set_record(t, atom, record, keyword) :: :ok
  def set_record(index, entity_name, record, opts \\ []) do
    %{fields: fields} = entity = Map.fetch!(index.entities, entity_name)

    # previous = get(index, entity_name, record.id)
    previous = if opts[:new_record?], do: nil, else: get(index, entity_name, record.id)
    # previous = opts[:already_held?] != false && get(index, entity_name, record.id)

    # Update the record itself (by id).
    put(index, entity_name, record)

    Enum.each(entity.prefilters, fn
      {nil, pf_opts} ->
        update_index_for_fields(index, entity_name, nil, fields, previous, record)
        update_for_maintain_unique(index, entity_name, pf_opts, nil, previous, record)

      {pf_key, pf_opts} ->
        # Get global (prefilter nil) uniques bundle.
        {_, list, _} = bundle = get_uniques_bundle(index, entity_name, pf_key, nil)

        # For each unique value under pf_key, updated uniques.
        Enum.each(list, fn value ->
          update_uniques_for_global_prefilter(
            index,
            entity_name,
            pf_key,
            bundle,
            value,
            previous,
            record
          )

          prefilter = {pf_key, value}

          update_index_for_fields(index, entity_name, prefilter, fields, previous, record)
          update_for_maintain_unique(index, entity_name, pf_opts, prefilter, previous, record)
        end)
    end)
  end

  # Get and update global (prefilter nil) uniques for the pf field.
  @spec update_uniques_for_global_prefilter(
          t,
          atom,
          atom,
          uniques_bundle,
          any,
          record | nil,
          record
        ) :: :ok
  defp update_uniques_for_global_prefilter(
         %{index_ref: index_ref},
         entity_name,
         pf_key,
         bundle,
         value,
         previous,
         record
       ) do
    cond do
      previous && Map.get(previous, pf_key) == Map.get(record, pf_key) ->
        # For this prefilter key, record hasn't moved. Do nothing.
        nil

      previous && previous[pf_key] == value ->
        # Record was moved to another prefilter. Remove it from this one.
        bundle
        |> remove_unique(value)
        |> put_uniques_bundle(index_ref, entity_name, nil, pf_key)

      Map.get(record, pf_key) == value ->
        # Record was moved to this prefilter. Add it.
        bundle
        |> add_unique(value)
        |> put_uniques_bundle(index_ref, entity_name, nil, pf_key)

      true ->
        nil
    end

    :ok
  end

  # Update indexes for each field under the prefilter.
  @spec update_index_for_fields(t, atom, prefilter, [Entity.field()], record | nil, record) :: :ok
  defp update_index_for_fields(index, entity_name, prefilter, fields, previous, record) do
    Enum.each(fields, fn {field_name, _} = field ->
      asc_key = index_key(entity_name, field_name, :asc, prefilter)
      desc_key = index_key(entity_name, field_name, :desc, prefilter)
      desc_ids = get_index(index, desc_key)

      # Remove the id from the list if it exists; insert it for sure.
      desc_ids = if previous, do: desc_ids -- [record.id], else: desc_ids
      desc_ids = insert_by(desc_ids, record, entity_name, field, index)

      :ets.insert(index.index_ref, {desc_key, desc_ids})
      :ets.insert(index.index_ref, {asc_key, Enum.reverse(desc_ids)})
    end)
  end

  # Update any configured :maintain_uniques fields for this prefilter.
  @spec update_for_maintain_unique(t, atom, keyword, prefilter, record | nil, record) :: :ok
  defp update_for_maintain_unique(index, entity_name, pf_opts, prefilter, previous, record) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      bundle = get_uniques_bundle(index, entity_name, field_name, prefilter)
      new_value = Map.get(record, field_name)

      new_bundle =
        if previous && Map.get(previous, field_name) != new_value,
          do: remove_unique(bundle, Map.get(previous, field_name)),
          else: bundle

      new_bundle
      |> add_unique(new_value)
      |> put_uniques_bundle(index.index_ref, entity_name, prefilter, field_name)
    end)
  end
end
