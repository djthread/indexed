defmodule Indexed.Managed.Helpers do
  @moduledoc "Some tools for `Indexed.Managed`."
  alias Ecto.Association.NotLoaded
  alias Indexed.Managed, as: M

  @typep assoc_spec :: M.assoc_spec()
  @typep id :: Indexed.id()
  @typep record :: Indexed.record()
  @typep state :: M.State.t()

  @doc """
  Invoke fun with the managed state, finding it in the :managed key if needed.
  If fun returns a managed state and it was wrapped, rewrap it.
  """
  @spec with_state(M.state_or_wrapped(), (state -> any)) :: any
  def with_state(%{managed: state} = sow, fun) do
    with %M.State{} = new_managed <- fun.(state),
         do: %{sow | managed: new_managed}
  end

  def with_state(%{} = sow, fun), do: fun.(sow)

  @doc """
  Given state, wrapped state, or a module, invoke `fun` with the
  `t:Indexed.t/0`. If a module is given, use the index from its `__index__/0`.
  """
  @spec with_index(M.state_or_module(), (Indexed.t() -> any)) :: any
  def with_index(%{managed: state} = som, fun),
    do: %{som | managed: with_index(state, fun)}

  def with_index(%{} = som, fun),
    do: do_with_index(som.index, som.module, fun)

  def with_index(som, fun),
    do: do_with_index(som.__index__(), som, fun)

  def do_with_index(index, _mod, fun) when is_function(fun, 1), do: fun.(index)
  def do_with_index(index, mod, fun), do: fun.(index, mod)

  # Returns true if we're holding in cache
  # another record with a has_many including the record for match_id.
  @spec has_referring_many?(state, atom, id) :: boolean
  def has_referring_many?(%{module: mod} = state, match_name, match_id) do
    Enum.any?(mod.__managed_names__(), fn name ->
      Enum.any?(mod.__managed__(name).children, fn
        {:many, ^match_name, prefilter_key, _} ->
          match_id == Map.fetch!(M.get(state, match_name, match_id), prefilter_key)

        _ ->
          false
      end)
    end)
  end

  # Get the foreign key for the `path_entry` field of `module`.
  @spec get_fkey(module, atom) :: atom
  def get_fkey(module, path_entry) do
    module.__schema__(:association, path_entry).related_key
  end

  # Wrap ecto query if `:query` function is defined.
  @spec build_query(M.t()) :: Ecto.Queryable.t()
  def build_query(%{module: assoc_mod, query: nil}),
    do: assoc_mod

  def build_query(%{module: assoc_mod, query: query_fn}),
    do: query_fn.(assoc_mod)

  # Attempt to lift an association directly from its parent.
  @spec assoc_from_record(record, atom) :: record | nil
  def assoc_from_record(record, path_entry) do
    case record do
      %{^path_entry => %NotLoaded{}} -> nil
      %{^path_entry => %{} = assoc} -> assoc
      _ -> nil
    end
  end

  # Invoke :subscribe function for the given entity id if one is defined.
  @spec subscribe(module, atom, id) :: any
  def subscribe(mod, name, id) do
    with %{subscribe: sub} when is_function(sub) <- get_managed(mod, name),
         do: sub.(id)
  end

  # Invoke :unsubscribe function for the given entity id if one is defined.
  @spec unsubscribe(module, atom, id) :: any
  def unsubscribe(mod, name, id) do
    with %{unsubscribe: usub} when is_function(usub) <- get_managed(mod, name),
         do: usub.(id)
  end

  # Get the %Managed{} or raise an error.
  @spec get_managed(state | module, atom) :: M.t()
  def get_managed(%{module: mod}, name), do: get_managed(mod, name)

  def get_managed(mod, name) do
    mod.__managed__(name) ||
      raise ":#{name} must have a managed declaration on #{inspect(mod)}."
  end

  @doc """
  Given a preload function spec, create a preload function. `key` is the key of
  the parent entity which should be filled with the child or list of children.

  See `t:preload/0`.
  """
  @spec preload_fn(assoc_spec, Ecto.Repo.t()) :: (map, state -> any) | nil
  def preload_fn({:one, name, key}, _repo) do
    fn record, state_or_module ->
      M.get(state_or_module, name, Map.get(record, key))
    end
  end

  def preload_fn({:many, name, pf_key, order_hint}, _repo) do
    fn record, state_or_module ->
      pf = if pf_key, do: {pf_key, record.id}, else: nil
      M.get_records(state_or_module, name, pf, order_hint)
    end
  end

  def preload_fn({:repo, key, %{module: module}}, repo) do
    {owner_key, related} =
      case module.__schema__(:association, key) do
        %{owner_key: k, related: r} -> {k, r}
        nil -> raise "Expected association #{key} on #{inspect(module)}."
      end

    fn record, _state_or_module ->
      with id when id != nil <- Map.get(record, owner_key),
           do: repo.get(related, id)
    end
  end

  def preload_fn(_, _), do: nil

  # Unload all associations (or only `assocs`) in an ecto schema struct.
  @spec drop_associations(struct, [atom] | nil) :: struct
  def drop_associations(%mod{} = struct, assocs \\ nil) do
    Enum.reduce(assocs || mod.__schema__(:associations), struct, fn association, struct ->
      %{struct | association => build_not_loaded(mod, association)}
    end)
  rescue
    # If struct is not an Ecto.Schema, we will silently not drop associations.
    UndefinedFunctionError -> struct
  end

  @spec build_not_loaded(module, atom) :: Ecto.Association.NotLoaded.t()
  defp build_not_loaded(mod, association) do
    %{
      cardinality: cardinality,
      field: field,
      owner: owner
    } = mod.__schema__(:association, association)

    %Ecto.Association.NotLoaded{
      __cardinality__: cardinality,
      __field__: field,
      __owner__: owner
    }
  end

  @doc """
  Convert a preload shorthand into a predictable data structure.

  ## Examples

      iex> normalize_preload(:foo)
      [foo: []]
      iex> normalize_preload([:foo, bar: :baz])
      [foo: [], bar: [baz: []]]
  """
  @spec normalize_preload(atom | list) :: [tuple]
  def normalize_preload(preload) do
    preload
    |> is_list()
    |> if(do: preload, else: [preload])
    |> Enum.map(&do_normalize_preload/1)
  end

  @spec do_normalize_preload(atom | tuple | list) :: [tuple]
  defp do_normalize_preload(item) when is_atom(item), do: {item, []}
  defp do_normalize_preload(item) when is_list(item), do: Enum.map(item, &do_normalize_preload/1)
  defp do_normalize_preload({item, sub}) when is_atom(sub), do: {item, [{sub, []}]}
  defp do_normalize_preload({item, sub}) when is_list(sub), do: {item, do_normalize_preload(sub)}
end
