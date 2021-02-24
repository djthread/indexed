defmodule Indexed.Actions.DestroyView do
  @moduledoc """
  Destroy a view when it is no longer needed.
  """
  alias Indexed.View

  @doc "Destroy a view when it is no longer needed."
  @spec run(Indexed.t(), atom, View.fingerprint()) :: :ok | :error
  def run(index, entity_name, fingerprint) do
    views_key = Indexed.views_key(entity_name)
    views = Indexed.get_index(index, views_key)
    entity = Map.fetch!(index.entities, entity_name)

    case views[fingerprint] do
      nil ->
        :error

      view ->
        :ets.insert(index.index_ref, {views_key, Map.delete(views, fingerprint)})
        destroy_indexes(index, entity_name, fingerprint, entity.fields)
        destroy_uniques(index, entity_name, fingerprint, view.maintain_unique)
        :ok
    end
  end

  @spec destroy_uniques(Indexed.t(), atom, View.fingerprint(), [atom]) :: :ok
  defp destroy_uniques(index, entity_name, fingerprint, maintain_unique) do
    Enum.each(maintain_unique, fn field_name ->
      map_key = Indexed.uniques_map_key(entity_name, fingerprint, field_name)
      list_key = Indexed.uniques_list_key(entity_name, fingerprint, field_name)
      Enum.each([map_key, list_key], &:ets.delete(index.index_ref, &1))
    end)
  end

  @spec destroy_indexes(Indexed.t(), atom, View.fingerprint(), [atom]) :: :ok
  defp destroy_indexes(index, entity_name, fingerprint, fields) do
    Enum.each(fields, fn {field_name, _} ->
      asc_key = Indexed.index_key(entity_name, fingerprint, field_name, :asc)
      desc_key = Indexed.index_key(entity_name, fingerprint, field_name, :desc)
      Enum.each([asc_key, desc_key], &:ets.delete(index.index_ref, &1))
    end)
  end
end
