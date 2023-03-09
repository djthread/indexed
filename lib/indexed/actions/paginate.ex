if Code.ensure_loaded?(Paginator) do
  defmodule Indexed.Actions.Paginate do
    @moduledoc """
    Tools for paginating in-memory data structures.

    The API is meant to match `Paginator` as much as possible so that callers
    can use simple logic to decide where to pull the data from, unbeknownst to
    the client.

    Call `paginate/3` to paginate through results using opaque cursors.
    """
    import Paginator, only: [cursor_for_record: 2]
    alias Paginator.{Config, Cursor, Page, Page.Metadata}

    @default_id_key :id

    @typep record :: struct
    @typep id :: any

    # Global, default options.
    # Note that these values can be still be overriden when `paginate/3` is called.
    @config [
      limit: 10
      # maximum_limit: 100
      # include_total_count: true
    ]

    @doc """
    Fetches all the results matching the query within the cursors.

    ## Options

      * `:after` - Fetch the records after this cursor.
      * `:before` - Fetch the records before this cursor.
      * `:order_by` - `{direction, field_name}` tuple where `:direction` is
        either `:asc` or `:desc` (default `:asc`). `:order_field` is a field name
        atom (eg. `:updated_by`). Also allowed is the field name atom alone, in
        which case `:asc` sort direction will be used.
      * `:filter` - An optional function which takes a record and returns a
        boolean, true if the record is desired in pagination. Default is `nil`
        where all records (in the selected prefilter) will be included.
      * `:id_key` - Atom key for primary identifier. Default `:id`.
      * `:prefilter` - Two-element tuple, indicating the field name and value for
        the prefiltered index to be used. Default is `nil`, indicating that the
        index with the non-prefiltered, full list of records should be used.
      * `:prepare` - An optional function which takes a record and returns a new
        record to use -- both for the filter function and in the result.
    """
    # * `:fetch_cursor_value_fun` function of arity 2 to lookup cursor values on returned records.
    # Defaults to `Paginator.default_fetch_cursor_value/2`
    # * `:include_total_count` - Set this to true to return the total number of
    # records matching the query. Note that this number will be capped by
    # `:total_count_limit`. Defaults to `false`.
    # * `:total_count_primary_key_field` - Running count queries on specified column of the table
    # * `:limit` - Limits the number of records returned per page. Note that this
    # number will be capped by `:maximum_limit`. Defaults to `50`.
    # * `:maximum_limit` - Sets a maximum cap for `:limit`. This option can be useful when `:limit`
    # is set dynamically (e.g from a URL param set by a user) but you still want to
    # enfore a maximum. Defaults to `500`.
    # * `:sort_direction` - The direction used for sorting. Defaults to `:asc`.
    # It is preferred to set the sorting direction per field in `:cursor_fields`.
    # * `:total_count_limit` - Running count queries on tables with a large number
    # of records is expensive so it is capped by default. Can be set to `:infinity`
    # in order to count all the records. Defaults to `10,000`.
    @spec run(Indexed.t(), atom, keyword) :: Paginator.Page.t() | nil
    def run(index, entity_name, params) do
      order_hint =
        if params[:order_by],
          do: Indexed.Helpers.normalize_order_hint(params[:order_by]),
          else: raise(":order_by required")

      pf = params[:prefilter]
      id_key = params[:id_key] || @default_id_key
      is_atom(id_key) || raise "id_key must be an atom."

      cursor_fields = Enum.map(order_hint ++ [asc: id_key], fn {dir, field} -> {field, dir} end)

      with ordered_ids when is_list(ordered_ids) <-
             Indexed.get_index(index, entity_name, pf, order_hint) do
        getter = fn id ->
          # Run the record through the prepare function, if defined.
          case {params[:prepare], Indexed.get(index, entity_name, id)} do
            {fun, record} when is_function(fun) -> fun.(record)
            {_, record} -> record
          end
        end

        paginate(
          ordered_ids,
          getter,
          Keyword.merge(params,
            cursor_fields: cursor_fields,
            filter: params[:filter],
            id_key: id_key
          )
        )
      end
    end

    @doc """
    Given the relevant, presorted list of ids and a function to fetch a record
    by its id, build the `t:Paginator.Page.t/0` result.
    """
    @spec paginate([id], fun, keyword) :: Page.t()
    def paginate(ordered_ids, record_getter, opts \\ []) when is_list(ordered_ids) do
      id_key = opts[:id_key] || @default_id_key
      filter = opts[:filter]
      config = Config.new(Keyword.merge(@config, opts))

      Config.validate!(config)

      cursor_after_in = if a = opts[:after], do: Cursor.decode(a)
      cursor_before_in = if b = opts[:before], do: Cursor.decode(b)

      {records, _count, cursor_before, cursor_after} =
        cond do
          cursor_before_in ->
            cursor_id = cursor_before_in[id_key]
            collect_before(record_getter, ordered_ids, config, filter, cursor_id)

          cursor_after_in ->
            cursor_id = cursor_after_in[id_key]
            collect_after(record_getter, ordered_ids, config, filter, cursor_id)

          true ->
            collect_after(record_getter, ordered_ids, config, filter, nil)
        end

      %Page{
        entries: records,
        metadata: %Metadata{
          after: cursor_after || nil,
          before: cursor_before,
          limit: config.limit,
          total_count: nil,
          total_count_cap_exceeded: false
        }
      }
    end

    # Build `{records, count}` for the items preceeding and not including the
    # record with id `id`. Only records where `filter/1` returns true will be
    # included.
    @spec collect_before(fun, [id], Config.t(), fun | nil, id | nil) ::
            {records :: [record], count :: integer, cursor_before :: String.t(),
             cursor_after :: String.t()}
    defp collect_before(record_getter, ordered_ids, config, filter, cursor_id) do
      prev_ids_revd =
        Enum.reduce_while(ordered_ids, [], fn
          ^cursor_id, acc -> {:halt, acc}
          id, acc -> {:cont, [id | acc]}
        end)

      {[first_on_this_page | _] = records, count, has_previous_page?, cursor_after} =
        Enum.reduce_while(prev_ids_revd, {[], 0, false, nil}, fn id,
                                                                 {acc, count, false, cursor_after} ->
          record = record_getter.(id)

          {acc, count} =
            if is_nil(filter) || filter.(record),
              do: {[record | acc], count + 1},
              else: {acc, count}

          cursor_after =
            case acc do
              [last_on_this_page] -> cursor_for_record(last_on_this_page, config.cursor_fields)
              _ -> cursor_after
            end

          if count == config.limit + 1,
            do: {:halt, {Enum.drop(acc, 1), count - 1, true, cursor_after}},
            else: {:cont, {acc, count, false, cursor_after}}
        end)

      cursor_before =
        has_previous_page? && cursor_for_record(first_on_this_page, config.cursor_fields)

      {records, count, cursor_before, cursor_after}
    end

    # Scan ids until cursor, then collect items where `filter/1` returns true,
    # until limit. If `cursor_id` is nil, then we are on the first page.
    @spec collect_after(fun, [id], Config.t(), fun | nil, id | nil) ::
            {records :: [record], count :: integer, cursor_before :: String.t() | nil,
             cursor_after :: String.t() | nil}
    defp collect_after(record_getter, ordered_ids, config, filter, cursor_id) do
      eat = fn id, acc, read_ids, count, cursor_before ->
        record = record_getter.(id)

        {acc, count} =
          if is_nil(filter) || filter.(record),
            do: {[record | acc], count + 1},
            else: {acc, count}

        # If this is the first accumulated record, then backtrack from this point
        # to find any past match. If there is one, we will build cursor_before,
        # setting it to the cursor (string) or nil.
        cursor_before =
          if false == cursor_before and match?([_], acc),
            do:
              (is_nil(filter) or Enum.any?(read_ids || [], &filter.(record_getter.(&1)))) &&
                cursor_for_record(hd(acc), config.cursor_fields),
            else: cursor_before

        # After collecting 1 extra record, we know there's a next page...
        if count == config.limit + 1 do
          [_first_on_next_page | [last_on_this_page | _] = acc] = acc
          cursor_after = cursor_for_record(last_on_this_page, config.cursor_fields)
          {:halt, {acc, nil, count - 1, cursor_before, cursor_after, true}}
        else
          {:cont, {acc, nil, count, cursor_before, false, true}}
        end
      end

      # (When cursor is found, read_ids is unneeded and is set to nil.)
      # If there is no cursor, then we fake it here.
      # cursor_before false means we haven't invoked eat the first time yet
      # where we resolve the value.
      {cursor_before, read_ids, found_cursor?} =
        if is_nil(cursor_id),
          do: {nil, nil, true},
          else: {false, [], false}

      {revd_records, _read_ids, count, cursor_before, cursor_after, _found_cursor?} =
        Enum.reduce_while(ordered_ids, {[], read_ids, 0, cursor_before, false, found_cursor?}, fn
          ^cursor_id, {acc, read_ids, 0, false, false, false} ->
            {:cont, {acc, read_ids, 0, false, false, true}}

          id, {acc, read_ids, count, cursor_before, false, true} ->
            eat.(id, acc, read_ids, count, cursor_before)

          id, {acc, read_ids, 0, false, false, false} ->
            {:cont, {acc, [id | read_ids], 0, false, false, false}}
        end)

      {Enum.reverse(revd_records), count, cursor_before, cursor_after}
    end
  end
end
