defmodule Indexed.Actions.Put do
  @moduledoc "An index action where a record is being added or updated."
  alias Indexed.{Entity, UniquesBundle, View}
  alias __MODULE__

  defstruct [:current_view, :entity_name, :index, :previous, :pubsub, :record]

  @typedoc """
  * `:current_view` - View struct currently being updated.
  * `:entity_name` - Entity name being operated on.
  * `:index` - See `t:Indexed.t/0`.
  * `:previous` - The previous version of the record. `nil` if none.
  * `:pubsub` - If configured, a Phoenix.PubSub module to send view updates.
  * `:record` - The new record being added in the put operation.
  """
  @type t :: %__MODULE__{
          current_view: View.t() | nil,
          entity_name: atom,
          index: Indexed.t(),
          previous: Indexed.record() | nil,
          pubsub: module | nil,
          record: Indexed.record()
        }

  @doc """
  Add or update a record, along with the indexes to reflect the change.
  """
  @spec run(Indexed.t(), atom, Indexed.record()) :: :ok
  def run(index, entity_name, record) do
    %{fields: fields} = entity = Map.fetch!(index.entities, entity_name)

    put = %Put{
      entity_name: entity_name,
      index: index,
      previous: Indexed.get(index, entity_name, record.id),
      pubsub: Application.get_env(:indexed, :pubsub),
      record: record
    }

    # Update the record itself (by id).
    :ets.insert(entity.ref, {record.id, record})

    # Update indexes for each prefilter.
    Enum.each(entity.prefilters, fn
      {nil, pf_opts} ->
        update_index_for_fields(put, nil, fields, false)
        update_all_uniques(put, pf_opts[:maintain_unique] || [], nil, false)

      {pf_key, pf_opts} ->
        # Get global (prefilter nil) uniques bundle.
        {map, list, _, _} = bundle = UniquesBundle.get(index, entity_name, nil, pf_key)
        record_value = Map.get(record, pf_key)

        handle_prefilter_value = fn value, new_value? ->
          # This will be how it is known what the unique values this pf key are
          # so users and machines alike can know which prefilters (key and val)
          # actually exist!
          update_uniques_for_global_prefilter(put, bundle, pf_key, value)

          prefilter = {pf_key, value}
          update_index_for_fields(put, prefilter, fields, new_value?)
          update_all_uniques(put, pf_opts[:maintain_unique] || [], prefilter, new_value?)
        end

        # If record has a newly-seen prefilter value, add fresh indexes.
        unless Map.has_key?(map, record_value) do
          handle_prefilter_value.(record_value, true)
        end

        # For each existing unique value for the prefilter field, update indexes.
        Enum.each(list, fn value ->
          handle_prefilter_value.(value, false)
        end)
    end)

    # Update the data for each view.
    with views when is_map(views) <- Indexed.get_views(index, entity_name) do
      Enum.each(views, fn {fp, view} ->
        update_view_data(%{put | current_view: view}, fp)
      end)
    end

    :ok
  end

  # Loop the fields of a `:maintain_unique` option, updating uniques indexes.
  # `new_value?` of true indicates the prefilter value is new and not indexed.
  @spec update_all_uniques(t, [atom], Indexed.prefilter(), boolean) :: :ok
  defp update_all_uniques(put, maintain_unique, prefilter, new_value?) do
    Enum.each(maintain_unique, fn field_name ->
      prev_in_pf? = put.previous && under_prefilter?(put, put.previous, prefilter)
      this_in_pf? = under_prefilter?(put, put.record, prefilter)

      bundle =
        if new_value?,
          do: {%{}, [], false, false},
          else: UniquesBundle.get(put.index, put.entity_name, prefilter, field_name)

      update_uniques(put, prefilter, field_name, bundle, prev_in_pf?, this_in_pf?)
    end)
  end

  # Update any configured :maintain_unique fields for this prefilter.
  # `prev_in_pf?` and `this_in_pf?` tell the logic whether the previous and new
  # records are in the prefilter.
  @spec update_uniques(t, Indexed.prefilter(), atom, UniquesBundle.t(), boolean, boolean) ::
          UniquesBundle.t()
  defp update_uniques(put, prefilter, field_name, bundle, prev_in_pf?, this_in_pf?) do
    new_value = Map.get(put.record, field_name)
    previous_value = put.previous && Map.get(put.previous, field_name)

    bundle =
      if put.previous do
        bundle = if prev_in_pf?, do: UniquesBundle.remove(bundle, previous_value), else: bundle
        if this_in_pf?, do: UniquesBundle.add(bundle, new_value), else: bundle
      else
        UniquesBundle.add(bundle, new_value)
      end

    UniquesBundle.put(bundle, put.index.index_ref, put.entity_name, prefilter, field_name)
  end

  # Get and update global (prefilter nil) uniques for the field_name.
  # These field_names would be used as prefilter keys when querying prefilters.
  # `value` is the current global prefilter value being iterated over.
  @spec update_uniques_for_global_prefilter(t, UniquesBundle.t(), atom, any) :: :ok
  defp update_uniques_for_global_prefilter(put, bundle, field_name, value) do
    prev_value = put.previous && Map.get(put.previous, field_name)
    new_value = Map.get(put.record, field_name)
    put_bundle = &UniquesBundle.put(&1, put.index.index_ref, put.entity_name, nil, field_name)

    cond do
      put.previous && prev_value == new_value ->
        # For this prefilter key, record hasn't moved. Do nothing.
        nil

      put.previous && prev_value == value ->
        # Record was moved to another prefilter. Remove it from this one.
        bundle |> UniquesBundle.remove(value) |> put_bundle.()

      new_value == value ->
        # Record was moved to this prefilter. Add it.
        bundle |> UniquesBundle.add(value) |> put_bundle.()

      true ->
        nil
    end

    :ok
  end

  # Update id indexes for each field under the prefilter.
  # If prefilter is a view fingerprint and a pubsub is configured, broadcast
  # messages to subscribers for any changes made.
  @spec update_index_for_fields(t, Indexed.prefilter(), [Entity.field()], boolean) :: :ok
  defp update_index_for_fields(put, prefilter, fields, newly_seen_value?) do
    %{previous: previous, record: record} = put

    Enum.each(fields, fn {field_name, _} = field ->
      this_under_pf = under_prefilter?(put, record, prefilter)
      prev_under_pf = previous && under_prefilter?(put, previous, prefilter)
      record_value = Map.get(record, field_name)
      prev_value = previous && Map.get(previous, field_name)

      cond do
        prev_under_pf && this_under_pf ->
          if record_value != prev_value do
            # Value differs, but we remain in the same prefilter. Fix sorting.
            put_index(put, field, prefilter, [:remove, :add], newly_seen_value?)
            msg = %{fingerprint: prefilter, record: put.record}
            maybe_broadcast(put, prefilter, [:update], msg)
          end

        prev_under_pf ->
          # Record is leaving this prefilter.
          put_index(put, field, prefilter, [:remove], newly_seen_value?)
          msg = %{fingerprint: prefilter, id: put.record.id}
          maybe_broadcast(put, prefilter, [:remove], msg)

        this_under_pf ->
          # Record is entering this prefilter.
          put_index(put, field, prefilter, [:add], newly_seen_value?)
          msg = %{fingerprint: prefilter, record: put.record}
          maybe_broadcast(put, prefilter, [:add], msg)

        true ->
          nil
      end
    end)
  end

  # Update a pair of indexes (asc/desc) by understanding if the record's id
  # must be resorted by removing and adding it or simply one of the two if it
  # is entering or leaving the prefilter.
  @spec put_index(t, Entity.field(), Indexed.prefilter(), [:remove | :add], boolean) :: :ok
  defp put_index(put, {field_name, _} = field, prefilter, actions, newly_seen_value?) do
    asc_key = Indexed.index_key(put.entity_name, prefilter, field_name, :asc)
    desc_key = Indexed.index_key(put.entity_name, prefilter, field_name, :desc)

    desc_ids = fn desc_key ->
      if newly_seen_value?, do: [], else: Indexed.get_index(put.index, desc_key)
    end

    new_desc_ids =
      Enum.reduce(actions, desc_ids.(desc_key), fn
        :remove, dids -> dids -- [put.record.id]
        :add, dids -> insert_by(put, dids, field)
      end)

    :ets.insert(put.index.index_ref, {desc_key, new_desc_ids})
    :ets.insert(put.index.index_ref, {asc_key, Enum.reverse(new_desc_ids)})

    :ok
  end

  # If a pubsub is configured and the prefilter is a view fingerprint,
  # broadcast the view change.
  @spec maybe_broadcast(t, Indexed.prefilter(), [atom], Indexed.record()) :: :ok | nil
  defp maybe_broadcast(%{pubsub: nil}, _, _, _), do: nil

  defp maybe_broadcast(%{pubsub: pubsub}, fingerprint, event, record) when is_binary(fingerprint),
    do: Phoenix.PubSub.broadcast(pubsub, fingerprint, {Indexed, event, record})

  defp maybe_broadcast(_, _, _, _), do: nil

  # Returns true if the record is under the prefilter.
  @spec under_prefilter?(t, Indexed.record(), Indexed.prefilter()) :: boolean
  defp under_prefilter?(_put, _record, nil), do: true
  defp under_prefilter?(_put, record, {pf_key, pf_val}), do: Map.get(record, pf_key) == pf_val

  defp under_prefilter?(%{current_view: %{filter: filter, prefilter: view_pf}} = put, record, fp)
       when is_binary(fp) do
    under_prefilter?(put, record, view_pf) && (is_nil(filter) || filter.(record))
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

  # Update indexes and unique tracking for a view.
  @spec update_view_data(t, View.fingerprint()) :: :ok
  defp update_view_data(%{current_view: view} = put, fingerprint) do
    %{fields: fields} = Map.fetch!(put.index.entities, put.entity_name)
    update_index_for_fields(put, fingerprint, fields, false)
    update_all_uniques(put, view.maintain_unique, fingerprint, false)
    :ok
  end
end
