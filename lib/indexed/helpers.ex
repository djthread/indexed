defmodule Indexed.Helpers do
  @moduledoc "Helper functions for internal use."

  @doc """
  Get the id of the record being operated on.

  If the configured `:id_key` is a one-arity function, pass in the record to
  build the id.
  """
  @spec id_value(map, any) :: any
  def id_value(record, id_key) when is_function(id_key), do: id_key.(record)
  def id_value(record, id_key), do: Map.get(record, id_key)

  @doc """
  Get the id of the record being operated on from an action state.

  See `id_value/2`.
  """
  @spec id_value(map) :: any
  def id_value(%{entity_name: entity_name, index: %{entities: entities}, record: record}) do
    id_value(record, entities[entity_name].id_key)
  end

  @doc "Convert a field-only order hint into a tuple one."
  @spec normalize_order_hint(Indexed.order_hint()) :: [{:asc | :desc, atom}]
  def normalize_order_hint({_direction, _field} = hint), do: [hint]
  def normalize_order_hint(hint) when is_atom(hint), do: [asc: hint]

  def normalize_order_hint(hint) when is_list(hint),
    do: Enum.map(hint, &hd(normalize_order_hint(&1)))

  def normalize_order_hint(hint), do: [{:asc, hint}]
end
