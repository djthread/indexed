defmodule Indexed.UniquesBundle do
  @moduledoc """
  A piece of data defining unique values of a field under a prefilter.
  """

  @typedoc """
  A 3-element tuple defines unique values under a prefilter:

  1. Map of discrete values to their occurrence counts.
  2. List of discrete values. (Keys of #1's map.)
  3. A boolean which is true when the list of keys has been updated and
     should be saved.
  """
  @type t :: {counts_map, list :: [any] | nil, list_updated? :: boolean}

  @typedoc "Occurrences of each value (map key) under a prefilter."
  @type counts_map :: %{any => non_neg_integer}

  @doc "Store two indexes for unique value tracking."
  @spec put(t, :ets.tid(), atom, Indexed.prefilter(), atom) :: true
  def put(
        {counts_map, list, list_updated?},
        index_ref,
        entity_name,
        prefilter,
        field_name
      ) do
    list_key = fn -> Indexed.unique_values_key(entity_name, prefilter, field_name, :list) end
    counts_key = fn -> Indexed.unique_values_key(entity_name, prefilter, field_name, :counts) end

    if list_updated? and Enum.empty?(list) do
      # This prefilter has ran out of records -- delete the ETS table.
      :ets.delete(index_ref, list_key.())
      :ets.delete(index_ref, counts_key.())
    else
      if list_updated?, do: :ets.insert(index_ref, {list_key.(), list})
      :ets.insert(index_ref, {counts_key.(), counts_map})
    end
  end
end
