defmodule Indexed.Actions.Drop do
  @moduledoc """
  An index action where a record is deleted.

  - For each prefilter, drop record from id list indexes.
  - For each prefilter, drop record from uniques.
  - Drop record itself from id-keyed lookup table.
  """
  import Indexed.Helpers, only: [id: 1, rm_from_lookup: 4]
  alias Indexed.{Entity, UniquesBundle, View}
  alias __MODULE__

  defstruct [:current_view, :entity_name, :index, :record]

  @typedoc """
  * `:current_view` - View struct currently being updated.
  * `:entity_name` - Entity name being operated on.
  * `:index` - See `t:Indexed.t/0`.
  * `:record` - The new record being added in the drop operation.
  """
  @type t :: %__MODULE__{
          current_view: View.t() | nil,
          entity_name: atom,
          index: Indexed.t(),
          record: Indexed.record()
        }

  @typep id :: any

  @doc """
  Add or update a record, along with the indexes to reflect the change.
  """
  @spec run(Indexed.t(), atom, id) :: :ok | :error
  def run(%{index_ref: index_ref} = index, name, record_id) do
    case Indexed.get(index, name, record_id) do
      nil ->
        :error

      record ->
        %{
          fields: fields,
          lookups: lookups,
          prefilters: prefilters,
          ref: ref
        } = Map.fetch!(index.entities, name)

        drop = %Drop{entity_name: name, index: index, record: record}

        Enum.each(prefilters, fn
          {nil, pf_opts} ->
            drop_from_index_for_fields(drop, nil, fields)
            drop_from_all_uniques(drop, pf_opts[:maintain_unique] || [], nil)

          {pf_key, pf_opts} ->
            {_, list, _, _} = bundle = UniquesBundle.get(index, name, nil, pf_key)

            handle_prefilter_value = fn value ->
              prefilter = {pf_key, value}

              drop_from_index_for_fields(drop, prefilter, fields)
              drop_from_all_uniques(drop, pf_opts[:maintain_unique] || [], prefilter)

              drop
              |> drop_from_uniques_for_global_prefilter(bundle, pf_key, value)
              |> drop_prefilter_indexes_if_last_instance(drop, prefilter)
            end

            # For each existing unique value for the prefilter field, update indexes.
            Enum.each(list, &handle_prefilter_value.(&1))
        end)

        # Update the data for each view.
        with %{} = views <- Indexed.get_views(index, name) do
          Enum.each(views, fn {fp, view} ->
            update_view_data(%{drop | current_view: view}, fp)
          end)
        end

        # Delete the record from each lookup.
        for field <- lookups do
          key = Indexed.lookup_key(name, field)
          map = Indexed.get_index(index, key)
          map = rm_from_lookup(map, record, field, record_id)
          :ets.insert(index_ref, {key, map})
        end

        # Delete the record itself.
        :ets.delete(ref, record_id)

        :ok
    end
  end

  # For each prefilter field, we track unique record values for each field
  # configured for the entity to be indexed. If `UniquesBundle.remove/2`
  # dropped the last instance of the value, then we should clean up the indexes
  # entirely (as they would be empty and the value would be missing from global
  # uniques, from which the question "what prefilters exist?" is answered).
  @spec drop_prefilter_indexes_if_last_instance(UniquesBundle.t(), t, Indexed.prefilter()) :: :ok
  defp drop_prefilter_indexes_if_last_instance({_, _, _, true}, drop, prefilter) do
    %{fields: fields} = Map.fetch!(drop.index.entities, drop.entity_name)

    Enum.each(fields, fn {field_name, _} ->
      asc_key = Indexed.index_key(drop.entity_name, prefilter, field_name)
      desc_key = Indexed.index_key(drop.entity_name, prefilter, {:desc, field_name})

      :ets.delete(drop.index.index_ref, asc_key)
      :ets.delete(drop.index.index_ref, desc_key)
    end)
  end

  defp drop_prefilter_indexes_if_last_instance(_, _, _), do: :ok

  # Loop the fields of a `:maintain_unique` option, updating uniques indexes.
  @spec drop_from_all_uniques(t, [atom], Indexed.prefilter()) :: :ok
  defp drop_from_all_uniques(drop, maintain_unique, prefilter) do
    Enum.each(maintain_unique, fn field_name ->
      if under_prefilter?(drop, drop.record, prefilter) do
        value = Map.get(drop.record, field_name)

        drop.index
        |> UniquesBundle.get(drop.entity_name, prefilter, field_name)
        |> UniquesBundle.remove(value)
        |> UniquesBundle.put(drop.index.index_ref, drop.entity_name, prefilter, field_name)
      end
    end)
  end

  # Get and update global (prefilter nil) uniques for the field_name.
  # These field_names would be used as prefilter keys when querying prefilters.
  # `value` is the current global prefilter value being iterated over.
  @spec drop_from_uniques_for_global_prefilter(t, UniquesBundle.t(), atom, any) ::
          UniquesBundle.t()
  defp drop_from_uniques_for_global_prefilter(drop, bundle, field_name, value) do
    if value == Map.get(drop.record, field_name) do
      bundle
      |> UniquesBundle.remove(value)
      |> UniquesBundle.put(drop.index.index_ref, drop.entity_name, nil, field_name)
    else
      bundle
    end
  end

  # Remove the record's id from all relevant prefilter indexes for each field.
  @spec drop_from_index_for_fields(t, Indexed.prefilter(), [Entity.field()]) :: :ok
  defp drop_from_index_for_fields(drop, prefilter, fields) do
    Enum.each(fields, fn {field_name, _} ->
      if under_prefilter?(drop, drop.record, prefilter) do
        asc_key = Indexed.index_key(drop.entity_name, prefilter, field_name)
        desc_key = Indexed.index_key(drop.entity_name, prefilter, {:desc, field_name})
        new_desc_ids = Indexed.get_index(drop.index, desc_key) -- [id(drop)]

        :ets.insert(drop.index.index_ref, {desc_key, new_desc_ids})
        :ets.insert(drop.index.index_ref, {asc_key, Enum.reverse(new_desc_ids)})
      end
    end)
  end

  # Returns true if the record is under the prefilter.
  @spec under_prefilter?(t, Indexed.record(), Indexed.prefilter()) :: boolean
  defp under_prefilter?(_drop, _record, nil), do: true
  defp under_prefilter?(_drop, record, {pf_key, pf_val}), do: Map.get(record, pf_key) == pf_val

  defp under_prefilter?(%{current_view: %{filter: filter, prefilter: view_pf}} = drop, record, fp)
       when is_binary(fp) do
    under_prefilter?(drop, record, view_pf) && (is_nil(filter) || filter.(record))
  end

  # Update indexes and unique tracking for a view.
  @spec update_view_data(t, View.fingerprint()) :: :ok
  defp update_view_data(%{current_view: view} = drop, fingerprint) do
    %{fields: fields} = Map.fetch!(drop.index.entities, drop.entity_name)
    drop_from_index_for_fields(drop, fingerprint, fields)
    drop_from_all_uniques(drop, view.maintain_unique, fingerprint)
    :ok
  end
end
