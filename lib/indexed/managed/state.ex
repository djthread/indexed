defimpl Inspect, for: Indexed.Managed.State do
  def inspect(_state, _opts) do
    "#Indexed.Managed.State<>"
  end
end

defmodule Indexed.Managed.State do
  @moduledoc "A piece of GenServer state for Managed."
  alias Indexed.Managed
  alias __MODULE__

  defstruct [:index, :module, :repo, :tmp, :tracking]

  @typedoc """
  Data structure used to hold temporary data while running `manage/5`.

  * `:done_ids` - Entity name-keyed map with lists of record IDs which have been
    processed during the `:add` or `:remove` phases because another entity has
    :one of them. This allows us to skip processing it next time(s) if it
    appears elsewhere.
  * `:many_added` - For each `%{entity_name => id}`, a list of :many assoc
    fields. A field is added here when processing it during the :add phase, and
    it's used to know whether to skip the field in drop_rm_ids.
  * `:one_rm_queue` - When a :one association is handled in the :rm phase, a
    tuple is put here under the parent name and parent id containing the sub
    path and the record to remove. Removals will occur either immediately
    preceeding the same parent being processed during the :add phase OR during
    the finishing step. (But only if no other record has a :many relationship to
    it still.)
  * `:records` - Records which may be committed to ETS at the end of the
    operation. Outer map is keyed by entity name. Inner map is keyed by id.
  * `:rm_ids` - Record IDs queued for removal with respect to their parent.
    Outer map is keyed by entity name. Inner map is keyed by parent ID.
    Inner-most map is keyed by parent field containing the children.
  * `:top_rm_ids` - Top-level record IDs queued for removal.
  * `:tracking` - For record ids relevant to the operation, initial values are
    copied from State and manipulated as needed within this structure.
  """
  @type tmp :: %{
          done_ids: %{atom => %{phase => [id]}},
          many_added: %{atom => %{id => [atom]}},
          one_rm_queue: %{atom => %{id => {list, record}}},
          records: %{atom => %{id => record}},
          rm_ids: %{atom => %{id => %{atom => [id]}}},
          top_rm_ids: [id],
          tracking: tracking
        }

  @typep phase :: :add | :rm

  @typedoc """
  * `:index` - Indexed struct. Static after created via `Indexed.warm/1`.
  * `:module` - Module which has `use Indexed.Managed`.
  * `:repo` - Ecto Repo module to use for loading data.
  * `:tmp` - Data structure used internally during a call to `manage/5`.
    Otherwise `nil`.
  * `:tracking` - Data about how many :one refs there are to a certain entity.
  """
  @type t :: %State{
          index: Indexed.t() | nil,
          module: module,
          repo: module,
          tmp: tmp | nil,
          tracking: tracking
        }

  @typedoc """
  A set of tracked entity statuses.

  An entity is tracked if another entity refers to it with a :one relationship.
  """
  @type tracking :: %{atom => tracking_status}

  @typedoc """
  Map of tracked record IDs to occurrences throughout the records held.
  Used to manage subscriptions and garbage collection.
  """
  @type tracking_status :: %{id => non_neg_integer}

  @typep id :: Indexed.id()
  @typep record :: Indexed.record()
  @typep parent_info :: Managed.parent_info()

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init(module, module) :: t
  def init(mod, repo) do
    %State{module: mod, repo: repo, tracking: %{}}
  end

  @doc "Returns a freshly initialized state for `Indexed.Managed`."
  @spec init_tmp(t) :: t
  def init_tmp(state) do
    %{
      state
      | tmp: %{
          done_ids: %{},
          many_added: %{},
          one_rm_queue: %{},
          records: %{},
          rm_ids: %{},
          top_rm_ids: [],
          tracking: %{}
        }
    }
  end

  @doc "Get one_rm_queue map for all ids of an entity 'name'."
  @spec one_rm_queue(t, atom) :: %{id => tuple}
  def one_rm_queue(state, name),
    do: get_in(state, [Access.key(:tmp), :one_rm_queue, name]) || %{}

  @doc "Get one_rm_queue for an id."
  @spec one_rm_queue(t, atom, id) :: tuple | nil
  def one_rm_queue(state, name, id),
    do: state |> one_rm_queue(name) |> Map.get(id)

  @doc "Add a set of records into tmp's `:one_rm_queue`."
  @spec add_one_rm_queue(t, atom, list, %{id => record}) :: t
  def add_one_rm_queue(state, name, sub_path, record_map) do
    update_in(state, [Access.key(:tmp), :one_rm_queue, Access.key(name, %{})], fn of_entity ->
      Enum.reduce(record_map, of_entity || %{}, fn {id, rec}, acc ->
        Map.put(acc, id, {sub_path, rec})
      end)
    end)
  end

  @doc "Remove an id from tmp one_rm_queue."
  @spec subtract_one_rm_queue(t, atom, id) :: t
  def subtract_one_rm_queue(state, name, id) do
    keys = [Access.key(:tmp), :one_rm_queue, Access.key(name, %{})]
    update_in(state, keys, &Map.delete(&1, id))
  end

  @doc "Get `ids` list for `name` in tmp's done_ids."
  @spec tmp_done_ids(t, atom, phase) :: [id]
  def tmp_done_ids(state, name, phase) do
    get_in(state, [Access.key(:tmp), :done_ids, name, phase]) || []
  end

  @doc "Add `ids` list for `name` in tmp's done_ids."
  @spec add_tmp_done_ids(t, atom, phase, [id]) :: t
  def add_tmp_done_ids(state, name, phase, ids) do
    update_in(state, [Access.key(:tmp), :done_ids, name], fn
      %{^phase => existing_ids} = map -> Map.put(map, phase, existing_ids ++ ids)
      %{} = map -> Map.put(map, phase, ids)
      nil -> %{phase => ids}
    end)
  end

  @doc """
  Drop from the index all records in rm_ids EXCEPT where

  1. We haven't done the :add phase for the relationship (tmp.many_added) AND
  2. The parent still exists in cache.
  """
  @spec drop_rm_ids(t) :: :ok
  def drop_rm_ids(%{module: mod, tmp: %{rm_ids: rm_ids}} = state) do
    Enum.each(rm_ids, fn {parent_name, map} ->
      Enum.each(map, fn {parent_id, map2} ->
        Enum.each(map2, fn {path_entry, ids} ->
          with %{^path_entry => {:many, name, _, _}} <- mod.__managed__(parent_name).children,
               true <-
                 in_many_added?(state, parent_name, parent_id, path_entry) or
                   is_nil(Managed.get(state, parent_name, parent_id)) do
            Enum.each(ids, &Managed.drop(state, name, &1))
          end
        end)
      end)
    end)

    put_in(state, [Access.key(:tmp), :rm_ids], %{})
  end

  @doc "Drop from the index all records in tmp.top_rm_ids."
  @spec drop_top_rm_ids(t, atom) :: t
  def drop_top_rm_ids(%{tmp: %{top_rm_ids: ids}} = state, name) do
    Enum.each(ids, &Managed.drop(state, name, &1))
    put_in(state, [Access.key(:tmp), :top_rm_ids], [])
  end

  @doc "Remove an assoc id from tmp rm_ids or top_rm_ids."
  @spec subtract_tmp_rm_id(t, parent_info, id) :: t
  def subtract_tmp_rm_id(state, :top, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids], fn
      nil -> []
      l -> l -- [id]
    end)
  end

  def subtract_tmp_rm_id(%{tmp: %{rm_ids: rm_ids}} = state, {a, b, c}, id) do
    k = &Access.key/2
    keys = [k.(a, %{}), k.(b, %{}), k.(c, [])]

    # Ugly logic to drop empty structures.
    rm_ids =
      case update_in(rm_ids, keys, &(&1 -- [id])) do
        %{^a => %{^b => %{^c => []} = bmap} = amap} = map
        when 1 == map_size(bmap) and 1 == map_size(amap) and 1 == map_size(map) ->
          %{}

        %{^a => %{^b => %{^c => []} = bmap} = amap} = map
        when 1 == map_size(bmap) and 1 == map_size(amap) ->
          Map.delete(map, a)

        %{^a => %{^b => %{^c => []} = bmap} = amap} = map
        when 1 == map_size(bmap) ->
          %{map | a => Map.delete(amap, b)}

        %{^a => %{^b => %{^c => []} = bmap}} = map ->
          %{map | a => %{b => Map.delete(bmap, c)}}

        map ->
          map
      end

    put_in(state, [k.(:tmp, 42), :rm_ids], rm_ids)
  end

  @doc "Add an id to tmp rm_ids or top_rm_ids."
  @spec add_tmp_rm_id(t, parent_info, id) :: t
  def add_tmp_rm_id(state, :top, id) do
    update_in(state, [Access.key(:tmp), :top_rm_ids], &[id | &1])
  end

  def add_tmp_rm_id(state, parent_info, id) do
    update_in_tmp_rm_id(state, parent_info, &[id | &1])
  end

  @doc "Use a function to update tmp rm_ids for a `parent_info`."
  @spec update_in_tmp_rm_id(t, parent_info, (list -> list)) :: t
  def update_in_tmp_rm_id(state, {a, b, c}, fun) do
    k = &Access.key/2
    keys = [k.(:tmp, 42), :rm_ids, k.(a, %{}), k.(b, %{}), k.(c, [])]
    update_in(state, keys, fun)
  end

  # Add an assoc id into tmp.many_added.
  @spec add_tmp_many_added(t, atom, id, atom) :: t
  def add_tmp_many_added(state, name, id, field) do
    k = &Access.key(&1, &2)
    keys = [k.(:tmp, 42), :many_added, k.(name, %{}), k.(id, [])]
    update_in(state, keys, &[field | &1])
  end

  @spec in_many_added?(t, atom, id, atom) :: boolean
  defp in_many_added?(state, name, id, field) do
    field in (get_in(state, [Access.key(:tmp), :many_added, name, id]) || [])
  end

  # Get the tracking (number of references) for the given entity and id.
  @spec tracking(t, atom, any) :: non_neg_integer
  def tracking(%{tracking: tracking}, name, id),
    do: get_in(tracking, [name, id]) || 0

  @doc "Add an id and ref count to `:tracking`."
  @spec put_tracking(t, atom, id, non_neg_integer) :: t
  def put_tracking(state, name, id, num) do
    k = &Access.key/2
    put_in(state, [k.(:tracking, 42), k.(name, %{}), k.(id, %{})], num)
  end

  @doc "Delete an id and its ref count from `:tracking`."
  @spec delete_tracking(t, atom, id) :: t
  def delete_tracking(%{tracking: tracking} = state, name, id) do
    tracking =
      case Map.fetch!(tracking, name) do
        %{^id => _} = m when 1 == map_size(m) -> Map.delete(tracking, name)
        %{^id => _} -> Map.update!(tracking, name, &Map.delete(&1, id))
      end

    %{state | tracking: tracking}
  end

  # Get the tmp tracking (number of references) for the given entity and id.
  @spec tmp_tracking(t, atom, any) :: non_neg_integer
  def tmp_tracking(%{tmp: %{tracking: tt}, tracking: t}, name, id) do
    get = &get_in(&1, [name, id])
    get.(tt) || get.(t) || 0
  end

  @doc "Reset tmp tracking map."
  @spec clear_tmp_tracking(t) :: t
  def clear_tmp_tracking(state) do
    put_in(state, [Access.key(:tmp), :tracking], %{})
  end

  # Update tmp tracking. If a function is given, its return value will be used.
  # As input, the fun gets the current count, using non-tmp tracking if empty.
  @spec put_tmp_tracking(t, atom, id, non_neg_integer | (non_neg_integer -> non_neg_integer)) :: t
  def put_tmp_tracking(state, name, id, num_or_fun) when is_function(num_or_fun) do
    update_in(state, [Access.key(:tmp), :tracking, Access.key(name, %{}), id], fn
      nil ->
        num = get_in(state.tracking, [name, id]) || 0
        num_or_fun.(num)

      num ->
        num_or_fun.(num)
    end)
  end

  def put_tmp_tracking(state, name, id, num_or_fun),
    do: put_tmp_tracking(state, name, id, fn _ -> num_or_fun end)

  def put_tmp_record(state, name, id, record),
    do: put_in(state, [Access.key(:tmp), :records, Access.key(name, %{}), id], record)
end
