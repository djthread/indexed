defmodule Indexed.Actions.CreateView do
  @moduledoc """
  Create a view - a custom result set prefilter.
  """
  alias Indexed.Actions.Warm
  alias Indexed.View

  @typep id :: any

  @doc """
  Build a view for a particular set of search results, indexed by the
  `:fields` config on `index`, identified by `fingerprint`.

  ## Options

  * `:filter` - Function which takes a record and returns true if it should be
    included in the result set. This is evaluated after the prefilter.
    Default is `nil` where all values will be accepted.
  * `:maintain_unique` - List of field name atoms for which a list of unique
    values under the prefilter will be managed. These lists can be fetched via
    `Indexed.get_uniques_list/4` and `Indexed.get_uniques_map/4`.
  * `:params` - Keyword list of parameters which were used to generate the
    fingerprint. If provided, they will be added to the `%Indexed.View{}` for
    use in authorization checking by the depending application.
  * `:prefilter` - Selects a pre-partitioned section of the full data of
    `entity_name`. The filter function will be applied onto this in order to
    arrive at the view's data set. See `t:Indexed.prefilter/0`.
  """
  @spec run(Indexed.t(), atom, View.fingerprint(), keyword) :: {:ok, View.t()} | :error
  def run(index, entity_name, fingerprint, opts \\ []) do
    entity = Map.fetch!(index.entities, entity_name)
    prefilter = opts[:prefilter]

    # Get current view map to ensure we're not creating an existing one.
    views_key = Indexed.views_key(entity_name)
    views = Indexed.get_index(index, views_key, %{})

    # Any pre-sorted field will do. At least we won't need to sort this one.
    {order_field, _} = hd(entity.fields)

    with false <- Map.has_key?(views, fingerprint),
         ids when is_list(ids) <-
           Indexed.get_index(index, entity_name, prefilter, order_field, :asc) do
      {view_ids, counts_map_map} = gather_records_and_uniques(index, entity_name, ids, opts)
      save_uniques(index, entity_name, fingerprint, counts_map_map, opts)
      save_indexes(index, entity_name, entity, fingerprint, view_ids, order_field)

      view = %View{
        filter: opts[:filter],
        maintain_unique: opts[:maintain_unique] || [],
        params: opts[:params],
        prefilter: prefilter
      }

      # Add view metadata to ETS.
      :ets.insert(index.index_ref, {views_key, Map.put(views, fingerprint, view)})

      {:ok, view}
    else
      _ -> :error
    end
  end

  # Pare down existing nil-filter index (of everything) to records for this view.
  # Also, build a map of field name to uniques map for this view:
  #   %{field_name: %{"Some Value" => 2}}
  @spec gather_records_and_uniques(Indexed.t(), atom, [id], keyword) ::
          {[id], %{atom => Indexed.UniquesBundle.counts_map()}}
  defp gather_records_and_uniques(index, entity_name, ids, opts) do
    filter = opts[:filter]
    maintain_unique = opts[:maintain_unique] || []

    {view_ids, counts_map_map} =
      Enum.reduce(ids, {[], %{}}, fn id, {ids, counts_map_map} ->
        record = Indexed.get(index, entity_name, id)

        if is_nil(filter) || filter.(record) do
          cmm =
            Enum.reduce(maintain_unique, counts_map_map, fn field_name, cmm ->
              value = Map.get(record, field_name)
              orig_count = get_in(counts_map_map, [field_name, value]) || 0
              put_in(cmm, Enum.map([field_name, value], &Access.key(&1, %{})), orig_count + 1)
            end)

          {[id | ids], cmm}
        else
          {ids, counts_map_map}
        end
      end)

    {Enum.reverse(view_ids), counts_map_map}
  end

  # Save unique value info for each field in :maintain_unique option to ETS.
  @spec save_uniques(
          Indexed.t(),
          atom,
          Indexed.View.fingerprint(),
          %{atom => Indexed.UniquesBundle.counts_map()},
          keyword
        ) :: any
  defp save_uniques(index, entity_name, fingerprint, counts_map_map, opts) do
    maintain_unique = opts[:maintain_unique] || []

    for field_name <- maintain_unique do
      counts_map = Map.get(counts_map_map, field_name, %{})
      list = counts_map |> Map.keys() |> Enum.sort()

      map_key = Indexed.uniques_map_key(entity_name, fingerprint, field_name)
      list_key = Indexed.uniques_list_key(entity_name, fingerprint, field_name)

      :ets.insert(index.index_ref, {map_key, counts_map})
      :ets.insert(index.index_ref, {list_key, list})
    end
  end

  # Save field indexes to ETS.
  @spec save_indexes(
          Indexed.t(),
          atom,
          Indexed.Entity.t(),
          Indexed.View.fingerprint(),
          [id],
          atom
        ) :: any
  defp save_indexes(index, entity_name, entity, fingerprint, view_ids, order_field) do
    view_records = Enum.map(view_ids, &Indexed.get(index, entity_name, &1))

    for {field_name, _} = field <- entity.fields do
      sorted_ids =
        if field_name == order_field,
          do: view_ids,
          else: view_records |> Enum.sort(Warm.record_sort_fn(field)) |> Enum.map(& &1.id)

      asc_key = Indexed.index_key(entity_name, fingerprint, field_name, :asc)
      desc_key = Indexed.index_key(entity_name, fingerprint, field_name, :desc)

      :ets.insert(index.index_ref, {asc_key, sorted_ids})
      :ets.insert(index.index_ref, {desc_key, Enum.reverse(sorted_ids)})
    end
  end
end
