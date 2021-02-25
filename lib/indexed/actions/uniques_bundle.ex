defmodule Indexed.UniquesBundle do
  @moduledoc """
  A piece of data defining unique values of a field under a prefilter.

  It tracks similar data in two ways. A map is tracked with each unique value
  found as keys and the number of occurrences found in the data set as vals.
  This is useful for quick updates to the data, while the second piece of
  data mainained, a list of the map's keys, is a handy, ascending-sorted list
  of these values.
  """
  alias __MODULE__

  @typedoc """
  A 3-element tuple defines unique values under a prefilter:

  1. Map of discrete values to their occurrence counts.
  2. List of discrete values. (Keys of #1's map.)
  3. A boolean which is true when the list of keys has been updated and
     should be saved.
  4. A boolean which is true when `remove/2` is used and it has taken the
     last remaining instance of the given value.
  """
  @type t ::
          {counts_map, list :: [any] | nil, list_updated? :: boolean,
           last_instance_removed? :: boolean}

  @typedoc "Occurrences of each value (map key) under a prefilter."
  @type counts_map :: %{any => non_neg_integer}

  # Get uniques_bundle - map and list versions of a unique values list
  @spec get(Indexed.t(), atom, Indexed.prefilter(), atom) :: t
  def get(index, entity_name, prefilter, field_name) do
    map = Indexed.get_uniques_map(index, entity_name, prefilter, field_name)
    list = Indexed.get_uniques_list(index, entity_name, prefilter, field_name)
    {map, list, false, false}
  end

  @doc "Remove value from the uniques bundle."
  @spec remove(t, any) :: t
  def remove({counts_map, list, list_updated?, last_instance_removed?}, value) do
    case Map.fetch!(counts_map, value) do
      1 -> {Map.delete(counts_map, value), list -- [value], true, true}
      n -> {Map.put(counts_map, value, n - 1), list, list_updated?, last_instance_removed?}
    end
  end

  @doc "Add a value to the uniques bundle."
  @spec add(t, any) :: t
  def add({counts_map, list, list_updated?, last_instance_removed?}, value) do
    case counts_map[value] do
      nil ->
        first_bigger_idx = Enum.find_index(list, &(&1 > value))
        new_list = List.insert_at(list, first_bigger_idx || -1, value)
        new_counts_map = Map.put(counts_map, value, 1)
        {new_counts_map, new_list, true, last_instance_removed?}

      orig_count ->
        {Map.put(counts_map, value, orig_count + 1), list, list_updated?, last_instance_removed?}
    end
  end

  @doc "Store unique values for a field in a prefilter (as a list and mop)."
  @spec put(t, :ets.tid(), atom, Indexed.prefilter(), atom) :: UniquesBundle.t()
  def put(
        {counts_map, list, list_updated?, _} = bundle,
        index_ref,
        entity_name,
        prefilter,
        field_name
      ) do
    list_key = Indexed.uniques_list_key(entity_name, prefilter, field_name)
    counts_key = Indexed.uniques_map_key(entity_name, prefilter, field_name)

    if list_updated? and Enum.empty?(list) and not is_binary(prefilter) do
      # This prefilter has ran out of records -- delete the ETS table.
      # Note that for views (binary prefilter) we allow the uniques to remain
      # in the empty state. They go when the view is destroyed.
      :ets.delete(index_ref, list_key)
      :ets.delete(index_ref, counts_key)
    else
      if list_updated?, do: :ets.insert(index_ref, {list_key, list})
      :ets.insert(index_ref, {counts_key, counts_map})
    end

    bundle
  end
end
