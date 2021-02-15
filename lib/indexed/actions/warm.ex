defmodule Indexed.Actions.Warm do
  @moduledoc "Holds internal state info during operations."
  alias Indexed.{Entity, UniquesBundle}
  alias __MODULE__

  @ets_opts [read_concurrency: true]

  defstruct [:data_tuple, :entity_name, :index_ref]

  @typedoc """
  * `:data` -
  """
  @type t :: %__MODULE__{
          data_tuple: data_tuple,
          entity_name: atom,
          index_ref: :ets.tid()
        }

  @typedoc """
  A list of records, wrapped in a hint about a field and direction it's
  already sorted by.
  """
  @type data_tuple :: {sort_dir :: :asc | :desc, sort_field :: atom, [Indexed.record()]}

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
      via `get_uniques_list/4`.
  """
  @spec run(keyword) :: Indexed.t()
  def run(args) do
    index_ref = :ets.new(:indexes, @ets_opts)

    entities =
      Map.new(args, fn {entity_name, opts} ->
        ref = :ets.new(entity_name, @ets_opts)
        fields = resolve_fields_opt(opts[:fields], entity_name)
        prefilter_configs = resolve_prefilters_opt(opts[:prefilters])

        {_dir, _field, full_data} =
          data_tuple = resolve_data_opt(opts[:data], entity_name, fields)

        # Load the records into ETS, keyed by id.
        Enum.each(full_data, &:ets.insert(ref, {&1.id, &1}))

        warm = %Warm{data_tuple: data_tuple, entity_name: entity_name, index_ref: index_ref}

        # Create and insert the caches for this entity: for each prefilter
        # configured, build & store indexes for each indexed field.
        # Internally, a `t:prefilter/0` refers to a `{:my_field, "possible
        # value"}` tuple or `nil` which we implicitly include, where no
        # prefilter is applied.
        for prefilter_config <- prefilter_configs do
          warm_prefilter(warm, prefilter_config, fields)
        end

        {entity_name, %Entity{fields: fields, prefilters: prefilter_configs, ref: ref}}
      end)

    %Indexed{entities: entities, index_ref: index_ref}
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
      # pass into `get_uniques_list/4`.)
      {counts_map, list} =
        Enum.reduce(grouped, {%{}, []}, fn {pf_val, records}, {counts_map, list} ->
          {Map.put(counts_map, pf_val, length(records)), [pf_val | list]}
        end)

      bundle = {counts_map, Enum.sort(Enum.uniq(list)), true}
      UniquesBundle.put(bundle, index_ref, entity_name, nil, pf_key)

      # For each value found for the prefilter, create a set of indexes.
      Enum.each(grouped, fn {pf_val, data} ->
        prefilter = {pf_key, pf_val}
        Enum.each(fields, &warm_index.(prefilter, &1, data))
        store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data)
      end)
    end
  end

  @doc "Create the asc and desc indexes for one field."
  @spec warm_index(:ets.tid(), atom, Indexed.prefilter(), Entity.field(), data_tuple()) :: true
  # Data direction hint matches this field -- no need to sort.
  def warm_index(ref, entity_name, prefilter, {name, _sort_hint}, {data_dir, name, data}) do
    data_ids = id_list(data)

    asc_key = Indexed.index_key(entity_name, name, :asc, prefilter)
    asc_ids = if data_dir == :asc, do: data_ids, else: Enum.reverse(data_ids)
    desc_key = Indexed.index_key(entity_name, name, :desc, prefilter)
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

    asc_key = Indexed.index_key(entity_name, name, :asc, prefilter)
    desc_key = Indexed.index_key(entity_name, name, :desc, prefilter)
    asc_ids = data |> Enum.sort(sort_fn) |> id_list()

    :ets.insert(ref, {asc_key, asc_ids})
    :ets.insert(ref, {desc_key, Enum.reverse(asc_ids)})
  end

  # Save list of unique values for each field configured by :maintain_unique.
  @spec store_all_uniques(:ets.tid(), atom, Indexed.prefilter(), keyword, [Indexed.record()]) ::
          :ok
  defp store_all_uniques(index_ref, entity_name, prefilter, pf_opts, data) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      {counts_map, list} =
        Enum.reduce(data, {%{}, []}, fn record, {counts_map, list} ->
          val = Map.get(record, field_name)
          num = Map.get(counts_map, val, 0) + 1
          {Map.put(counts_map, val, num), [val | list]}
        end)

      bundle = {counts_map, Enum.sort(Enum.uniq(list)), true}
      UniquesBundle.put(bundle, index_ref, entity_name, prefilter, field_name)
    end)
  end

  @doc "Normalize fields."
  @spec resolve_fields_opt([atom | Entity.field()], atom) :: [Entity.field()]
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

  @doc "Return a list of all `:id` elements from the `collection`."
  @spec id_list([Indexed.record()]) :: [Indexed.id()]
  def id_list(collection) do
    Enum.map(collection, & &1.id)
  end
end
