defmodule Indexed.Helpers do
  @moduledoc "Helper functions for internal use."

  @doc "Get the id of the record being operated on."
  @spec id_value(map) :: any
  def id_value(%{entity_name: entity_name, index: %{entities: entities}, record: record}) do
    Map.get(record, entities[entity_name].id_key)
  end
end
