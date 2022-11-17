defmodule Indexed.Helpers do
  @moduledoc "Helper functions for internal use."

  @doc """
  Get the id of the record being operated on.

  See `t:Indexed.Entity.t/0`.
  """
  @spec id(map, any) :: any
  def id(record, id_key) when is_function(id_key), do: id_key.(record)
  def id(record, {mod, fun}), do: apply(mod, fun, [record])
  def id(record, nil), do: raise("No id_key found for #{inspect(record)}")
  def id(record, id_key), do: Map.get(record, id_key)

  @doc """
  Get the id of the record being operated on from an action state.

  See `id_value/2`.
  """
  @spec id(map) :: any
  def id(%{entity_name: entity_name, index: %{entities: entities}, record: record}) do
    id(record, entities[entity_name].id_key)
  end

  @doc "Convert a field-only order hint into a tuple one."
  @spec normalize_order_hint(Indexed.order_hint()) :: [{:asc | :desc, atom}]
  def normalize_order_hint({_direction, _field} = hint), do: [hint]
  def normalize_order_hint(hint) when is_atom(hint), do: [asc: hint]

  def normalize_order_hint(hint) when is_list(hint),
    do: Enum.map(hint, &hd(normalize_order_hint(&1)))

  def normalize_order_hint(hint), do: [{:asc, hint}]

  @doc "Given a lookup map, add `field` according to `record`."
  @spec add_to_lookup(Indexed.lookup(), Indexed.record(), atom, Indexed.id()) :: Indexed.lookup()
  def add_to_lookup(lookup, record, field, id) do
    val = Map.fetch!(record, field)
    Map.update(lookup, val, [id], &[id | &1])
  end

  @doc "Given a lookup map, remove `field` according to `record`."
  def rm_from_lookup(lookup, record, field, id) do
    val = Map.fetch!(record, field)
    Map.update!(lookup, val, &(&1 -- [id]))
  end
end
