defmodule Indexed.Actions.Put do
  @moduledoc "Holds internal state info during operations."
  alias Indexed.{Entity, UniquesBundle}
  alias __MODULE__

  defstruct [:bundle, :entity_name, :index, :previous, :record]

  @typedoc """
  * `:bundle` - See `t:Indexed.UniquesBundle/0`.
  * `:entity_name` - Entity name being operated on.
  * `:index` - See `t:Indexed.t/0`.
  * `:previous` - The previous version of the record. `nil` if none.
  * `:record` - The new record being added in the put operation.
  """
  @type t :: %__MODULE__{
          bundle: UniquesBundle.t() | nil,
          entity_name: atom,
          index: Indexed.t(),
          previous: Indexed.record() | nil,
          record: Indexed.record()
        }

  @doc """
  Add or update a record, along with the indexes to reflect the change.
  """
  @spec run(Indexed.t(), atom, Indexed.record()) :: :ok
  def run(index, entity_name, record) do
    %{fields: fields} = entity = Map.fetch!(index.entities, entity_name)
    previous = Indexed.get(index, entity_name, record.id)

    put = %Put{entity_name: entity_name, index: index, previous: previous, record: record}

    # Update the record itself (by id).
    :ets.insert(Map.fetch!(index.entities, entity_name).ref, {record.id, record})

    Enum.each(entity.prefilters, fn
      {nil, pf_opts} ->
        update_index_for_fields(put, nil, fields, false)
        update_for_maintain_unique(put, pf_opts, nil, false)

      {pf_key, pf_opts} ->
        # Get global (prefilter nil) uniques bundle.
        %{bundle: {map, list, _}} = put = get_uniques_bundle(put, pf_key, nil)
        record_value = Map.get(record, pf_key)

        fun = fn value, newly_seen_value? ->
          update_uniques_for_global_prefilter(put, pf_key, value)

          prefilter = {pf_key, value}

          update_index_for_fields(put, prefilter, fields, newly_seen_value?)
          update_for_maintain_unique(put, pf_opts, prefilter, newly_seen_value?)
        end

        # If record has a newly-seen prefilter value, add fresh indexes.
        unless Map.has_key?(map, record_value), do: fun.(record_value, true)

        # For each existing unique value for the prefilter field, update indexes.
        Enum.each(list, &fun.(&1, false))
    end)
  end

  # Get and update global (prefilter nil) uniques for the field_name.
  # These field_names would be used as prefilter keys when querying prefilters.
  # `value` is the current global prefilter value being iterated over.
  @spec update_uniques_for_global_prefilter(t, atom, any) :: :ok
  defp update_uniques_for_global_prefilter(put, field_name, value) do
    prev_value = put.previous && Map.get(put.previous, field_name)
    new_value = Map.get(put.record, field_name)

    cond do
      put.previous && prev_value == new_value ->
        # For this prefilter key, record hasn't moved. Do nothing.
        nil

      put.previous && prev_value == value ->
        # Record was moved to another prefilter. Remove it from this one.
        put |> remove_unique(value) |> put_uniques_bundle(nil, field_name)

      new_value == value ->
        # Record was moved to this prefilter. Add it.
        put |> add_unique(value) |> put_uniques_bundle(nil, field_name)

      true ->
        nil
    end

    :ok
  end

  # Update indexes for each field under the prefilter.
  @spec update_index_for_fields(t, Indexed.prefilter(), [Entity.field()], boolean) :: :ok
  defp update_index_for_fields(put, prefilter, fields, newly_seen_value?) do
    %{previous: previous, record: record} = put

    Enum.each(fields, fn {field_name, _} = field ->
      record_under_prefilter = under_prefilter?(record, prefilter)
      prev_under_prefilter = previous && under_prefilter?(previous, prefilter)
      record_value = Map.get(record, field_name)
      prev_value = previous && Map.get(previous, field_name)

      if previous do
        if record_under_prefilter && prev_under_prefilter do
          if record_value != prev_value do
            # Value differs, but we remain in the same prefilter. Remove & add.
            put_index(put, field, prefilter, [:remove, :add], newly_seen_value?)
          end
        else
          if prev_under_prefilter do
            # Record is moving out of this prefilter.
            put_index(put, field, prefilter, [:remove], newly_seen_value?)
          end
        end
      else
        if record_under_prefilter do
          # Record is moving into this prefilter.
          put_index(put, field, prefilter, [:add], newly_seen_value?)
        end
      end
    end)
  end

  @spec put_index(t, Entity.field(), Indexed.prefilter(), [atom], boolean) :: :ok
  defp put_index(put, {field_name, _} = field, prefilter, actions, newly_seen_value?) do
    asc_key = Indexed.index_key(put.entity_name, field_name, :asc, prefilter)
    desc_key = Indexed.index_key(put.entity_name, field_name, :desc, prefilter)

    desc_ids = fn desc_key ->
      if newly_seen_value?, do: [], else: Indexed.get_index(put.index, desc_key)
    end

    save = fn desc_ids ->
      :ets.insert(put.index.index_ref, {desc_key, desc_ids})
      :ets.insert(put.index.index_ref, {asc_key, Enum.reverse(desc_ids)})
    end

    new_desc_ids =
      Enum.reduce(actions, desc_ids.(desc_key), fn
        :remove, dids -> dids -- [put.record.id]
        :add, dids -> insert_by(put, dids, field)
      end)

    save.(new_desc_ids)

    :ok
  end

  # Update any configured :maintain_unique fields for this prefilter.
  @spec update_for_maintain_unique(t, keyword, Indexed.prefilter(), boolean) :: :ok
  defp update_for_maintain_unique(put, pf_opts, prefilter, newly_seen_value?) do
    Enum.each(pf_opts[:maintain_unique] || [], fn field_name ->
      put =
        if newly_seen_value?,
          do: %{put | bundle: {%{}, [], false}},
          else: get_uniques_bundle(put, field_name, prefilter)

      new_value = Map.get(put.record, field_name)
      previous_value = put.previous && Map.get(put.previous, field_name)

      put =
        if put.previous do
          put =
            if under_prefilter?(put.previous, prefilter),
              do: remove_unique(put, previous_value),
              else: put

          if under_prefilter?(put.record, prefilter),
            do: add_unique(put, new_value),
            else: put
        else
          add_unique(put, new_value)
        end

      put_uniques_bundle(put, prefilter, field_name)
    end)
  end

  # Returns true if the record is under the prefilter.
  @spec under_prefilter?(Indexed.record(), Indexed.prefilter()) :: boolean
  defp under_prefilter?(_record, nil), do: true
  defp under_prefilter?(record, {pf_key, pf_val}), do: Map.get(record, pf_key) == pf_val

  # Expands parameters from `put` on the way to `UniquesBundle.put/5`.
  @spec put_uniques_bundle(t, Indexed.prefilter(), atom) :: true
  defp put_uniques_bundle(put, prefilter, field_name) do
    UniquesBundle.put(put.bundle, put.index.index_ref, put.entity_name, prefilter, field_name)
  end

  # Get uniques_bundle - map and list versions of a unique values list
  @spec get_uniques_bundle(t, atom, Indexed.prefilter()) :: t
  def get_uniques_bundle(put, field_name, prefilter) do
    map = Indexed.get_uniques_map(put.index, put.entity_name, field_name, prefilter)
    list = Indexed.get_uniques_list(put.index, put.entity_name, field_name, prefilter)
    %{put | bundle: {map, list, false}}
  end

  @doc "Remove value from the uniques bundle."
  @spec remove_unique(t, any) :: t
  def remove_unique(%{bundle: {counts_map, list, list_updated?}} = put, value) do
    new_bundle =
      case Map.fetch!(counts_map, value) do
        1 -> {Map.delete(counts_map, value), list -- [value], true}
        n -> {Map.put(counts_map, value, n - 1), list, list_updated?}
      end

    %{put | bundle: new_bundle}
  end

  @doc "Add a value to the uniques bundle."
  @spec add_unique(t, any) :: t
  def add_unique(%{bundle: {counts_map, list, list_updated?}} = put, value) do
    new_bundle =
      case counts_map[value] do
        nil ->
          first_bigger_idx = Enum.find_index(list, &(&1 > value))
          new_list = List.insert_at(list, first_bigger_idx || -1, value)
          new_counts_map = Map.put(counts_map, value, 1)
          {new_counts_map, new_list, true}

        orig_count ->
          {Map.put(counts_map, value, orig_count + 1), list, list_updated?}
      end

    %{put | bundle: new_bundle}
  end

  @doc "Add the id of `record` to the list of descending ids, sorting by `field`."
  @spec insert_by(t, [Indexed.id()], Entity.field()) :: [Indexed.id()]
  def insert_by(put, old_desc_ids, {name, opts}) do
    find_fun =
      case opts[:sort] do
        :date_time ->
          fn id ->
            val = Map.get(Indexed.get(put.index, put.entity_name, id), name)
            :lt == DateTime.compare(val, Map.get(put.record, name))
          end

        nil ->
          this_value = Map.get(put.record, name)
          &(Map.get(Indexed.get(put.index, put.entity_name, &1), name) < this_value)
      end

    first_smaller_idx = Enum.find_index(old_desc_ids, find_fun)

    List.insert_at(old_desc_ids, first_smaller_idx || -1, put.record.id)
  end
end
