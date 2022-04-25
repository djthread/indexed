defmodule Indexed.Managed do
  @moduledoc """
  Assists a GenServer in managing in-memory caches.

  By annotating the entities to be managed, `manage/5` can handle updating the
  cache for the given record and its associated records. (If associations are
  not preloaded, they will be automatically fetched.) In addition, entites with
  `:subscribe` and `:unsubscribe` functions defined will be automatically
  subscribed to and unusbscribed from as the first reference appears and the
  last one is dropped.

  ## Example

  This module owns and is responsible for affecting changes on the Car with
  id 1. It subscribes to updates to Person records as they may be updated
  elsewhere.

      defmodule MyApp.CarManager do
        use GenServer
        use Indexed.Managed, repo: MyApp.Repo
        alias MyApp.{Car, Person, Repo}

        managed :cars, Car, children: [:passengers], manage_path: :passengers

        managed :people, Person,
          subscribe: &MyApp.subscribe_to_person/1,
          unsubscribe: &MyApp.unsubscribe_from_person/1

        @impl GenServer
        def init(_), do: {:ok, warm(:cars, Repo.get(Car, 1))}

        @impl GenServer
        def handle_call(:get, _from state) do
          {:reply, get(state, :cars, 1)}
        end

        def handle_call({:update, params}, _from, state) do
          case state |> get(:cars, 1) |> Car.changeset(params) |> Repo.update() do
            {:ok, new_car} = ok -> {:reply, ok, manage(state, :cars, :update, new_car)}
            {:error, _} = err -> {:reply, err, state}
          end
        end

        @impl GenServer
        def handle_info({MyApp, [:person, :update], person}, state) do
          {:noreply, manage(state, :people, :update, person)}
        end
      end

  ## Managed Macro

  For each managed entity, the name (eg. `:cars`) and module (eg. `MyApp.Car`)
  must be specified. If needed, a keyword list of options should follow.

  * `:children` - Keyword list with association fields as keys and
    `t:assoc_spec/0`s as vals. This is used when recursing in `manage/5` as
    well as when resolving. If an undeclared association is resolved,
    `Repo.get/2` will be used as a fallback.
  * `:query_fn` - Optional function which takes a queryable and returns a
    queryable. This allows for extra query logic to be added such as populating
    virtual fields. Invoked by `manage/5` when the association is needed.
  * `:id_key` - Specifies how to find the id for a record.  It can be an atom
    field name to access, a function, or a tuple in the form `{module,
    function_name}`. In the latter two cases, the record will be passed in.
    Default `:id`.
  * `:subscribe` and `:unsubscribe` - Functions which take a record's ID and
    manage the subscription. These must both be declared or neither.

  ## Tips

  If you want to `import Ecto.Query`, you'll find that its `preload/3` conflicts
  with Managed. Since Managed will use the repo as a fallback, you can exclude
  it this way.

      defmodule MyModule do
        use Indexed.Managed
        import Ecto.Query, except: [preload: 2, preload: 3]
      end
  """

  # Technical Explanation
  #
  # The core logic begins with `manage/5` where the original records and new
  # records are received. The `path` parameter (or the entity's `:manage_path`)
  # is used to define how deeply we are to recurse into the associations.
  # Operation happens in three phases:
  #
  # 1. "Remove" original records, recursing through path.
  # 2. "Add" new records, recursing through path.
  # 3. Reconcile with respect to temporary data (and clear `:tmp`).
  #
  # The state's `:tmp` data is used to track what's happened during the first
  # two phases so tracked records can be put or dropped in reconciliation.
  # See `t:Indexed.Managed.State.tmp/1`, `add/4`, `rm/4` for details.

  import Ecto.Query, except: [preload: 3]
  import Indexed.Helpers, only: [id: 2, normalize_preload: 1]
  import Indexed.Managed.Helpers
  alias Indexed.Actions.Warm
  alias Indexed.{Entity, View}
  alias Indexed.Managed.{Prepare, State}
  alias __MODULE__

  defstruct [
    :children,
    :fields,
    :id_key,
    :query,
    :manage_path,
    :module,
    :name,
    :prefilters,
    :tracked,
    :subscribe,
    :unsubscribe
  ]

  @typedoc """
  * `:children` - Map with assoc field name keys `t:assoc_spec_opt/0` values.
    When this entity is managed, all children will also be managed and so on,
    recursively.
  * `:fields` - Used to build the index. See `Managed.Entity.t/0`.
  * `:id_key` - Used to get a record id. See `Managed.Entity.t/0`.
  * `:query` - Optional function which takes a queryable and returns a
    queryable. This allows for extra query logic to be added such as populating
    virtual fields. Invoked by `manage/5` when the association is needed.
  * `:manage_path` - Default associations to traverse for `manage/5`.
  * `:module` - The struct module which will be used for the records.
  * `:name` - Atom name of the managed entity.
  * `:prefilters` - Used to build the index. See `Managed.Entity.t/0`.
  * `:subscribe` - 1-arity function which subscribes to changes by id.
  * `:tracked` - True if another entity has a :one assoc to this. Internal.
  * `:unsubscribe` - 1-arity function which unsubscribes to changes by id.
  """
  @type t :: %Managed{
          children: children,
          fields: [atom | Entity.field()],
          id_key: id_key,
          query: (Ecto.Queryable.t() -> Ecto.Queryable.t()) | nil,
          manage_path: path,
          module: module,
          name: atom,
          prefilters: [atom | keyword] | nil,
          subscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil,
          tracked: boolean,
          unsubscribe: (Ecto.UUID.t() -> :ok | {:error, any}) | nil
        }

  @typedoc "For convenience, state is also accepted within a wrapping map."
  @type state_or_wrapped :: %{:managed => state | nil, optional(any) => any} | state

  @typedoc "A map of field names to assoc specs."
  @type children :: %{atom => assoc_spec}

  @typedoc """
  An association spec defines an association to another entity.
  It is used to build the preload function among other things.

  * `{:one, entity_name, id_key}` - Preload function should get a record of
    `entity_name` with id matching the id found under `id_key` of the record.
  * `{:many, entity_name, pf_key, order_hint}` - Preload function should
    use `Indexed.get_records/4`. If `pf_key` is not null, it will be replaced
    with `{pfkey, id}` where `id` is the record's id.
  * `{:repo, key, managed}` - Preload function should use `Repo.get/2` with the
    assoc's module and the id in the foreign key field for `key` in the record.
    This is the default when a child/assoc_spec isn't defined for an assoc.
  """
  @type assoc_spec ::
          {:one, entity_name :: atom, id_key :: atom}
          | {:many, entity_name :: atom, pf_key :: atom | nil, order_hint}
          | {:repo, assoc_field :: atom, managed :: t}

  @typedoc """
  Assoc spec as provided in the managed declaration. See `t:assoc_spec/0`.

  This is always normalized to `t:assoc_spec/0` at compile time.
  Missing pieces are filled via `Ecto.Schema` reflection.
  """
  @type assoc_spec_opt ::
          atom
          | assoc_spec
          | {:many, entity_name :: atom}
          | {:many, entity_name :: atom, pf_key :: atom | nil}

  @type data_opt :: Warm.data_opt()

  # Path to follow when warming or updating data. Uses same format as preload.
  @type path :: atom | list

  @typedoc """
  Used to explain the parent entity when processing its has_many relationship.
  Either {:top, name} where name is the top-level entity name OR
  `nil` for a parent with a :one association OR a tuple with
  1. Parent entity name.
  2. ID of the parent.
  3. Field name which would have the list of :many children if loaded.
  """
  @type parent_info :: :top | {parent_name :: atom, id, path_entry :: atom} | nil

  @typep id_key :: atom | (record -> id)
  @typep add_or_rm :: :add | :rm
  @typep state :: State.t()
  @typep id :: Indexed.id()
  @typep order_hint :: Indexed.order_hint()
  @typep prefilter :: Indexed.prefilter()
  @typep record :: Indexed.record()
  @typep record_or_list :: [record] | record | nil
  @typep managed_or_name :: t | atom
  @typep preloads :: atom | list

  defmacro __using__(repo: repo) do
    quote do
      import unquote(__MODULE__)
      alias unquote(__MODULE__)
      @before_compile unquote(__MODULE__)
      @managed_repo unquote(repo)
      Module.register_attribute(__MODULE__, :managed_setup, accumulate: true)

      @doc "Create a Managed state struct, without index being initialized."
      @spec init_managed_state :: Managed.State.t()
      def init_managed_state, do: State.init(__MODULE__, unquote(repo))

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(atom, Managed.data_opt()) :: Managed.State.t()
      def warm(name, data_opt), do: warm(name, data_opt, nil)

      @doc """
      Invoke this function with (`state, entity_name, data_opt`) or
      (`entity_name, data_opt, path`).
      """
      @spec warm(
              Managed.state_or_wrapped() | atom,
              atom | Managed.data_opt(),
              Managed.data_opt() | Managed.path()
            ) ::
              Managed.state_or_wrapped()
      def warm(%{} = a, b, c), do: warm(a, b, c, nil)
      def warm(a, b, c), do: warm(init_managed_state(), a, b, c)

      @doc "Returns a freshly initialized state for `Indexed.Managed`."
      @spec warm(Managed.state_or_wrapped(), atom, Managed.data_opt(), Managed.path()) ::
              Managed.state_or_wrapped()
      def warm(state, name, data_opt, path) do
        fun = &do_warm(&1 || init_managed_state(), name, data_opt, path)
        Managed.Helpers.with_state(state, fun)
      end
    end
  end

  @doc "Loads initial data into index."
  @spec do_warm(state, atom, data_opt, path) :: state
  def do_warm(%{module: mod} = state, name, data, path) do
    state =
      if is_nil(state.index) do
        warm_args =
          Enum.reduce(mod.__managed__(), [], fn entity, acc ->
            managed = get_managed(mod, entity)

            Keyword.put(acc, entity,
              data: [],
              fields: managed.fields,
              id_key: managed.id_key,
              prefilters: managed.prefilters
            )
          end)

        %{state | index: Indexed.warm(warm_args)}
      else
        state
      end

    managed = get_managed(mod, name)
    {_, _, records} = Warm.resolve_data_opt(data, name, managed.fields)

    manage(state, name, [], records, path)
  end

  @doc "Define a managed entity."
  defmacro managed(name, module, opts \\ []) do
    quote do
      manage_path =
        case unquote(opts[:manage_path]) do
          nil -> []
          path -> normalize_preload(path)
        end

      require unquote(module)

      @managed_setup %Managed{
        children: unquote(opts[:children] || []),
        manage_path: manage_path,
        fields: unquote(opts[:fields] || []),
        query: unquote(opts[:query]),
        id_key: unquote(opts[:id_key] || :id),
        module: unquote(module),
        name: unquote(name),
        prefilters: unquote(opts[:prefilters] || []),
        subscribe: unquote(opts[:subscribe]),
        tracked: false,
        unsubscribe: unquote(opts[:unsubscribe])
      }
    end
  end

  defmacro __before_compile__(%{module: mod}) do
    attr = &Module.get_attribute(mod, &1)
    Prepare.validate_before_compile!(mod, attr.(:managed_repo), attr.(:managed_setup))
    Module.put_attribute(mod, :managed, Prepare.rewrite_manageds(attr.(:managed_setup)))
    Module.delete_attribute(mod, :managed_setup)

    quote do
      @doc "Returns a list of all managed entity names."
      @spec __managed__ :: [atom]
      def __managed__, do: Enum.map(@managed, & &1.name)

      @doc "Returns the `t:Managed.t/0` for an entity by its name or module."
      @spec __managed__(atom) :: Managed.t() | nil
      def __managed__(name), do: Enum.find(@managed, &(&1.name == name or &1.module == name))

      @doc "Returns a list of managed entity names which are tracked."
      @spec __tracked__ :: [atom]
      @tracked @managed |> Enum.filter(& &1.tracked) |> Enum.map(& &1.name)
      def __tracked__, do: @tracked

      @doc """
      Given a managed entity name or module and a field, return the preload
      function which will take a record and state and return the association
      record or list of records for `key`.
      """
      @spec __preload_fn__(atom, atom, module) :: (map, Managed.State.t() -> map | [map]) | nil
      def __preload_fn__(name, key, repo) do
        pl = &Managed.Helpers.preload_fn(&1, repo)

        case Enum.find(@managed, &(&1.name == name or &1.module == name)) do
          %{children: %{^key => assoc_spec}} -> pl.(assoc_spec)
          %{} = managed -> pl.({:repo, key, managed})
          nil -> nil
        end
      end
    end
  end

  @doc """
  Add, remove or update one or more managed records.

  The entity `name` atom should be declared as `managed`.

  Arguments 3 and 4 can take one of the following forms:

  * `:insert` and the new record: The given record and associations are added to
    the cache.
  * `:update` and the newly updated record or its ID: The given record and
    associations are updated in the cache. Raises if we don't hold the record.
  * `:upsert` and the newly updated record: The given record and associations
    are updated in the cache. If we don't hold the record, insert.
  * `:delete` and the record or ID to remove from cache.
    Raises if we don't hold the record.
  * If the original and new records are already known, they may also be supplied
    directly.

  Records and their associations are added, removed or updated in the cache by
  ID.

  `path` is formatted the same as Ecto's preload option and it specifies which
  fields and how deeply to traverse when updating the in-memory cache.
  If `path` is not supplied, the entity's `:manage_path` will be used.
  (Supply `[]` to override this and avoid managing associations.)
  """
  @spec manage(
          state_or_wrapped,
          managed_or_name,
          :insert | :update | :delete | id | record_or_list,
          id | record_or_list,
          path
        ) ::
          state_or_wrapped
  def manage(state, mon, orig, new, path \\ nil) do
    with_state(state, fn st ->
      %{id_key: id_key, name: name, manage_path: manage_path} =
        managed =
        case mon do
          %{} -> mon
          name_atom -> get_managed(st, name_atom)
        end

      {orig_records, new_records} = orig_and_new(st, name, id_key, orig, new)

      path =
        case path do
          nil -> manage_path
          p -> normalize_preload(p)
        end

      do_manage_top = &Enum.reduce(&2, &1, &3)
      do_manage_path = &do_manage_path(&1, name, &3, &2, path)

      st
      |> State.init_tmp()
      |> do_manage_top.(orig_records, &rm(&2, :top, managed, &1))
      |> do_manage_path.(orig_records, :rm)
      |> do_manage_top.(new_records, &add(&2, :top, managed, &1))
      |> do_manage_path.(new_records, :add)
      |> do_manage_finish(name)
      |> Map.put(:tmp, nil)
    end)
  end

  # Normalize manage/5's orig and new parameters into 2 lists of records.
  @spec orig_and_new(
          state,
          atom,
          id_key,
          :insert | :update | :delete | id | record_or_list,
          id | record_or_list
        ) :: {[record], [record]}
  defp orig_and_new(state, name, id_key, orig, new) do
    id = &id(&1, id_key)
    get = &get(state, name, &1)
    get! = &(get.(&1) || raise "Expected to already have #{name} id #{&1}.")

    {orig, new} =
      case {orig, new} do
        {:insert, n} -> {nil, n}
        {:update, %{} = n} -> {get!.(id.(n)), n}
        {:update, id} -> Tuple.duplicate(get!.(id), 2)
        {:upsert, n} -> {get.(id.(n)), n}
        {:delete, o} when is_map(o) -> {o, nil}
        {:delete, id} -> {get!.(id), nil}
        o_n -> o_n
      end

    to_list = fn
      nil -> []
      i when is_map(i) -> [i]
      i -> i
    end

    {to_list.(orig), to_list.(new)}
  end

  @spec do_manage_finish(state, atom) :: state
  defp do_manage_finish(%{module: mod} = state, top_name) do
    get_tmp_record = &get_in(state.tmp.records, [&1, &2])

    maybe_manage_rm = fn st, name, id ->
      case has_referring_many?(st, name, id) || State.one_rm_queue(st, name, id) do
        x when x in [true, nil] ->
          st

        {spath, rec} ->
          drop(st, name, id)
          do_manage_path(st, name, :rm, [rec], spath)
      end
    end

    # TODO: If I delete a record here, I might need to delete other connected records.
    # ... shouldn't matter if manage's path param is deep enough.
    handle = fn
      # Had 0 references, now have 1+.
      st, name, id, 0, new_c when new_c > 0 ->
        subscribe(mod, name, id)
        put(st, name, get_tmp_record.(name, id))
        State.put_tracking(st, name, id, new_c)

      # Had 1+ references, now have 0.
      st, name, id, _orig_c, 0 ->
        unsubscribe(mod, name, id)
        st = maybe_manage_rm.(st, name, id)
        State.delete_tracking(st, name, id)

      # Had 1+, still have 1+. If new record isn't in tmp, it is unchanged.
      st, name, id, _orig_c, new_c ->
        tmp_rec = get_tmp_record.(name, id)
        if tmp_rec, do: put(st, name, tmp_rec)
        State.put_tracking(st, name, id, new_c)
    end

    process_tmp_tracking = fn %{tmp: %{tracking: tmp_tracking}} = state ->
      Enum.reduce(tmp_tracking, State.clear_tmp_tracking(state), fn {name, map}, acc ->
        Enum.reduce(map, acc, fn {id, new_count}, acc2 ->
          orig_count = State.tracking(state, name, id)
          handle.(acc2, name, id, orig_count, new_count)
        end)
      end)
    end

    Enum.reduce_while(1..100, State.drop_top_rm_ids(state, top_name), fn
      _, %{tmp: %{rm_ids: rm_ids, tracking: tt}} = acc
      when 0 == map_size(rm_ids) and 0 == map_size(tt) ->
        {:halt, acc}

      _, %{tmp: %{tracking: tt}} = acc
      when 0 < map_size(tt) ->
        {:cont, process_tmp_tracking.(acc)}

      _, acc ->
        {:cont, State.drop_rm_ids(acc)}
    end)
  end

  # Remove a record according to its managed config and association to parent:
  # * :top means no parent. Queue for removal.
  # * 3-part tuple means parent has :many of these. Queue for removal.
  # * Otherwise, parent has :one of these. We MUST have been tracking at least
  #   1 reference already. Drop a reference.
  @spec rm(state, parent_info, t, record) :: state
  defp rm(state, parent_info, %{id_key: id_key, name: name}, record) do
    id = id(record, id_key)
    cur = State.tmp_tracking(state, name, id)

    case {cur, parent_info} do
      {_, :top} ->
        State.add_tmp_rm_id(state, :top, id)

      {_, {_, _, _} = parent_info} ->
        State.add_tmp_rm_id(state, parent_info, id)

      {cur, _} when cur > 0 ->
        State.put_tmp_tracking(state, name, id, cur - 1)
    end
  end

  # Add a record according to its managed config and association to parent:
  # * :top means no parent. Add to index immediately. Don't remove.
  # * nil means parent has :one of these. Track the reference, hold the record.
  # * 3-part tuple means parent has :many of these. Add to index immediately.
  #   Don't remove.
  @spec add(state, parent_info, t, record) :: state
  defp add(state, parent_info, %{id_key: id_key, name: name}, record) do
    id = id(record, id_key)
    record = drop_associations(record)

    case parent_info do
      :top ->
        put(state, name, record)
        State.subtract_tmp_rm_id(state, :top, id)

      nil ->
        state
        |> State.put_tmp_tracking(name, id, &(&1 + 1))
        |> State.put_tmp_record(name, id, record)

      info ->
        put(state, name, record)
        State.subtract_tmp_rm_id(state, info, id)
    end
  end

  # Handle managing associations according to path but not records themselves.
  @spec do_manage_path(state, atom, add_or_rm, [record], keyword) :: state
  defp do_manage_path(state, name, action, records, path) do
    Enum.reduce(path, state, fn {path_entry, sub_path}, acc ->
      %{children: children} = get_managed(acc.module, name)
      spec = Map.fetch!(children, path_entry)

      do_manage_assoc(acc, name, path_entry, spec, action, records, sub_path)
    end)
  end

  # Manage a single association across a set of (parent) records.
  # Then recursively handle associations according to sub_path therein.
  @spec do_manage_assoc(state, atom, atom, assoc_spec, add_or_rm, [record], keyword) ::
          state
  # *** ONE ADD - these records have a `belongs_to :assoc_name` association.
  defp do_manage_assoc(
         state,
         _name,
         path_entry,
         {:one, assoc_name, fkey},
         :add,
         records,
         sub_path
       ) do
    %{id_key: assoc_id_key} = assoc_managed = get_managed(state, assoc_name)

    {assoc_records, assoc_ids} =
      Enum.reduce(records, {[], []}, fn record, {acc_assoc_records, acc_assoc_ids} ->
        case Map.fetch!(record, fkey) do
          nil ->
            {acc_assoc_records, acc_assoc_ids}

          assoc_id ->
            case assoc_from_record(record, path_entry) do
              nil -> {acc_assoc_records, [assoc_id | acc_assoc_ids]}
              assoc -> {[assoc | acc_assoc_records], acc_assoc_ids}
            end
        end
      end)

    {from_db, from_db_map} =
      if [] == assoc_ids do
        {[], %{}}
      else
        q_assoc_ids = Enum.uniq(assoc_ids) -- Enum.map(assoc_records, &id(&1, assoc_id_key))
        query = build_query(assoc_managed)
        query = from x in query, where: field(x, ^assoc_id_key) in ^q_assoc_ids
        from_db = state.repo.all(query)
        {from_db, Map.new(from_db, &{Map.fetch!(&1, assoc_id_key), &1})}
      end

    add = &add(&2, nil, assoc_managed, &1)
    state = Enum.reduce(assoc_records, state, add)
    state = Enum.reduce(assoc_ids, state, &add.(Map.fetch!(from_db_map, &1), &2))

    # For each assoc not in done_ids,
    # manage :rm for original children if queued,
    # then continue manage :add new children.
    done_ids = State.tmp_done_ids(state, assoc_name, :add)
    rm_queue = State.one_rm_queue(state, assoc_name)

    {state, assoc_doing, assoc_doing_ids} =
      Enum.reduce(assoc_records ++ from_db, {state, [], []}, fn rec, {st, ad, adi} ->
        id = id(rec, assoc_id_key)

        rm_recurse = fn rec, spath ->
          st
          |> State.subtract_one_rm_queue(assoc_name, id)
          |> do_manage_path(assoc_name, :rm, [rec], spath)
        end

        case id not in done_ids && rm_queue[id] do
          false -> {st, ad, adi}
          nil -> {st, [rec | ad], [id | adi]}
          {spath, rec} -> {rm_recurse.(rec, spath), [rec | ad], [id | adi]}
        end
      end)

    state
    |> State.add_tmp_done_ids(assoc_name, :add, assoc_doing_ids)
    |> do_manage_path(assoc_name, :add, assoc_doing, sub_path)
  end

  # *** MANY ADD - these records have a `has_many :assoc_name` association.
  defp do_manage_assoc(
         state,
         name,
         path_entry,
         {:many, assoc_name, fkey, _},
         :add,
         records,
         sub_path
       ) do
    %{id_key: id_key, module: entity_mod} = get_managed(state.module, name)
    assoc_managed = get_managed(state.module, assoc_name)

    {assoc_records, ids} =
      Enum.reduce(records, {[], []}, fn record, {acc_assoc_records, acc_ids} ->
        case Map.fetch!(record, path_entry) do
          l when is_list(l) -> {l ++ acc_assoc_records, acc_ids}
          _ -> {acc_assoc_records, [id(record, id_key) | acc_ids]}
        end
      end)

    fkey =
      fkey ||
        :association
        |> entity_mod.__schema__(path_entry)
        |> Map.fetch!(:related_key)

    q = from x in build_query(assoc_managed), where: field(x, ^fkey) in ^ids
    from_db = state.repo.all(q)
    assoc_records = assoc_records ++ from_db

    state =
      Enum.reduce(assoc_records, state, fn assoc, acc ->
        parent_id = Map.fetch!(assoc, fkey)

        acc
        |> add({name, parent_id, path_entry}, assoc_managed, assoc)
        |> State.add_tmp_many_added(name, parent_id, path_entry)
      end)

    do_manage_path(state, assoc_name, :add, assoc_records, sub_path)
  end

  # *** ONE RM - these records have a `belongs_to :assoc_name` association.
  defp do_manage_assoc(
         state,
         _name,
         _path_entry,
         {:one, assoc_name, fkey},
         :rm,
         records,
         sub_path
       ) do
    assoc_managed = get_managed(state, assoc_name)
    done_ids = State.tmp_done_ids(state, assoc_name, :rm)

    maybe = fn assoc_id, assoc, {acc_ids, acc_map} ->
      if assoc_id in done_ids,
        do: {acc_ids, acc_map},
        else: {[assoc_id | acc_ids], Map.put(acc_map, assoc_id, assoc)}
    end

    {state, {assoc_doing_ids, assoc_doing_map}} =
      Enum.reduce(records, {state, {[], %{}}}, fn record, {acc_state, acc_records} ->
        case Map.fetch!(record, fkey) do
          nil ->
            {acc_state, acc_records}

          assoc_id ->
            assoc = get(acc_state, assoc_name, assoc_id)
            acc_records = maybe.(assoc_id, assoc, acc_records)
            {rm(acc_state, nil, assoc_managed, assoc), acc_records}
        end
      end)

    state
    |> State.add_tmp_done_ids(assoc_name, :rm, assoc_doing_ids)
    |> State.add_one_rm_queue(assoc_name, sub_path, assoc_doing_map)
  end

  # *** MANY RM - these records have a `has_many :assoc_name` association.
  defp do_manage_assoc(
         state,
         name,
         path_entry,
         {:many, assoc_name, fkey, _},
         :rm,
         records,
         sub_path
       ) do
    %{id_key: id_key, module: module} = get_managed(state.module, name)
    assoc_managed = get_managed(state.module, assoc_name)
    fkey = fkey || get_fkey(module, path_entry)

    {state, assoc_records} =
      Enum.reduce(records, {state, []}, fn record, {acc_state, acc_assoc_records} ->
        id = id(record, id_key)
        assoc_records = get_records(acc_state, assoc_name, {fkey, id}) || []
        fun = &rm(&2, {name, id, path_entry}, assoc_managed, &1)
        acc_state = Enum.reduce(assoc_records, acc_state, fun)

        {acc_state, assoc_records ++ acc_assoc_records}
      end)

    do_manage_path(state, assoc_name, :rm, assoc_records, sub_path)
  end

  @doc """
  Invoke `Indexed.get/3`. State may be wrapped in a map under `:managed` key.

  If `preloads` is `true`, use the entity's default path.
  """
  @spec get(state_or_wrapped, atom, id, preloads | true) :: any
  def get(state, name, id, preloads \\ nil) do
    with_state(state, fn %{index: index, module: mod} = st ->
      p = if true == preloads, do: mod.__managed__(name).manage_path, else: preloads
      record = Indexed.get(index, name, id)
      if p, do: preload(record, st, p), else: record
    end)
  end

  @doc "Invoke `Indexed.put/3` with a wrapped state for convenience."
  @spec put(state_or_wrapped, atom, record) :: :ok
  def put(state, name, record) do
    with_state(state, fn %{index: index} ->
      Indexed.put(index, name, record)
    end)
  end

  @doc "Invoke `Indexed.drop/3` with a wrapped state for convenience."
  @spec drop(state_or_wrapped, atom, id) :: :ok | :error
  def drop(state, name, id) do
    with_state(state, fn %{index: index} ->
      Indexed.drop(index, name, id)
    end)
  end

  @doc "Invoke `Indexed.get_index/4` with a wrapped state for convenience."
  @spec get_index(state_or_wrapped, atom, prefilter) :: list | map | nil
  def get_index(state, name, prefilter \\ nil, order_hint \\ nil) do
    with_state(state, fn %{index: index} ->
      Indexed.get_index(index, name, prefilter, order_hint)
    end)
  end

  @doc "Invoke `Indexed.get_records/4` with a wrapped state for convenience."
  @spec get_records(state_or_wrapped, atom, prefilter | nil, order_hint | nil) ::
          [record] | nil
  def get_records(state, name, prefilter \\ nil, order_hint \\ nil) do
    with_state(state, fn %{index: index} ->
      Indexed.get_records(index, name, prefilter, order_hint)
    end)
  end

  @doc "Invoke `Indexed.get_uniques_map/4`."
  @spec get_uniques_map(state_or_wrapped, atom, prefilter, atom) ::
          Indexed.UniquesBundle.counts_map() | nil
  def get_uniques_map(state, name, prefilter, field_name) do
    with_state(state, fn %{index: index} ->
      Indexed.get_uniques_map(index, name, prefilter, field_name)
    end)
  end

  @doc "Invoke `Indexed.get_uniques_list/4`."
  @spec get_uniques_list(state_or_wrapped, atom, prefilter, atom) :: list | nil
  def get_uniques_list(state, name, prefilter, field_name) do
    with_state(state, fn %{index: index} ->
      Indexed.get_uniques_list(index, name, prefilter, field_name)
    end)
  end

  @spec create_view(state_or_wrapped, atom, View.fingerprint(), keyword) ::
          {:ok, View.t()} | :error
  def create_view(state, name, fingerprint, opts \\ []) do
    with_state(state, fn %{index: index} ->
      Indexed.create_view(index, name, fingerprint, opts)
    end)
  end

  @spec paginate(state_or_wrapped, atom, keyword) :: Paginator.Page.t() | nil
  def paginate(state, name, params) do
    with_state(state, fn %{index: index} ->
      Indexed.paginate(index, name, params)
    end)
  end

  @doc "Invoke `Indexed.get_view/4` with a wrapped state for convenience."
  @spec get_view(state_or_wrapped, atom, View.fingerprint()) :: View.t() | nil
  def get_view(state, name, fingerprint) do
    with_state(state, fn %{index: index} ->
      Indexed.get_view(index, name, fingerprint)
    end)
  end

  # Returns a listing of entities and number of records in the cache for each.
  @spec managed_stat(state) :: keyword
  def managed_stat(state) do
    with_state(state, fn %{index: index} = st ->
      Enum.map(index.entities, fn {name, _} ->
        {name, length(get_index(st, name))}
      end)
    end)
  end

  @doc "Preload associations recursively."
  @spec preload(map | [map] | nil, state_or_wrapped, preloads) :: [map] | map | nil
  def preload(nil, _, _), do: nil

  def preload(record_or_list, %{managed: managed}, preloads) do
    preload(record_or_list, managed, preloads)
  end

  def preload(record_or_list, state, preloads) when is_list(record_or_list) do
    Enum.map(record_or_list, &preload(&1, state, preloads))
  end

  def preload(record_or_list, %{module: mod} = state, preloads) do
    record = record_or_list

    preload = fn
      %record_mod{} = record, key ->
        fun =
          mod.__preload_fn__(record_mod, key, state.repo) ||
            raise("No preload for #{inspect(record_mod)}.#{key} under #{inspect(mod)}.")

        fun.(record, state)

      _key, nil ->
        fn _, _ -> nil end
    end

    listify = fn
      nil -> []
      pl when is_list(pl) -> pl
      pl -> [pl]
    end

    Enum.reduce(listify.(preloads), record, fn
      {key, sub_pl}, acc ->
        preloaded = acc |> preload.(key) |> preload(state, listify.(sub_pl))
        Map.put(acc, key, preloaded)

      key, acc ->
        Map.put(acc, key, preload.(acc, key))
    end)
  end
end
