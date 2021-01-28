defmodule Indexed.PaginatorTest do
  use ExUnit.Case
  import Indexed.Paginator, only: [paginate: 3]

  @ordered_ids [
    "aaaa2a06-0dcf-4d9b-b2fc-31bfda45527b",
    "bbbb11e2-aa9f-4567-b869-719fa5e06bfb",
    "cccc8b25-f20b-425c-ad0c-377d8cc84356",
    "dddd43bf-05f0-403b-ad9b-0dbfa5bad636",
    "eeee1cf1-d0d0-4c7a-bad1-2cbd2b9822bb"
  ]

  @entries %{
    "aaaa2a06-0dcf-4d9b-b2fc-31bfda45527b" => %{
      id: "aaaa2a06-0dcf-4d9b-b2fc-31bfda45527b",
      x: "ha"
    },
    "bbbb11e2-aa9f-4567-b869-719fa5e06bfb" => %{
      id: "bbbb11e2-aa9f-4567-b869-719fa5e06bfb",
      x: "hi"
    },
    "cccc8b25-f20b-425c-ad0c-377d8cc84356" => %{
      id: "cccc8b25-f20b-425c-ad0c-377d8cc84356",
      x: "ho"
    },
    "dddd43bf-05f0-403b-ad9b-0dbfa5bad636" => %{
      id: "dddd43bf-05f0-403b-ad9b-0dbfa5bad636",
      x: "ox"
    },
    "eeee1cf1-d0d0-4c7a-bad1-2cbd2b9822bb" => %{
      id: "eeee1cf1-d0d0-4c7a-bad1-2cbd2b9822bb",
      x: "za"
    }
  }

  def opts(args \\ []) do
    defaults = [include_total_count: true, limit: 2, order_field: :id, order_direction: :asc]
    args = Keyword.merge(defaults, args)
    Keyword.put(args, :cursor_fields, [{args[:order_field], args[:order_direction]}])
  end

  defp getter(id), do: @entries[id]

  test "can page forwards" do
    cursor_after_1 = "g3QAAAABZAACaWRtAAAAJGJiYmIxMWUyLWFhOWYtNDU2Ny1iODY5LTcxOWZhNWUwNmJmYg=="

    assert %Paginator.Page{
             entries: [%{x: "ha"}, %{x: "hi"}],
             metadata: %Paginator.Page.Metadata{
               after: ^cursor_after_1,
               before: nil,
               limit: 2,
               total_count: 2,
               total_count_cap_exceeded: false
             }
           } = paginate(@ordered_ids, &getter/1, opts())

    cursor_after_2 = "g3QAAAABZAACaWRtAAAAJGRkZGQ0M2JmLTA1ZjAtNDAzYi1hZDliLTBkYmZhNWJhZDYzNg=="

    assert %Paginator.Page{
             entries: [%{x: "ho"}, %{x: "ox"}],
             metadata: %Paginator.Page.Metadata{
               after: ^cursor_after_2,
               before: "g3QAAAABZAACaWRtAAAAJGNjY2M4YjI1LWYyMGItNDI1Yy1hZDBjLTM3N2Q4Y2M4NDM1Ng==",
               limit: 2,
               total_count: 2,
               total_count_cap_exceeded: false
             }
           } = paginate(@ordered_ids, &getter/1, opts(after: cursor_after_1))

    assert %Paginator.Page{
             entries: [%{x: "za"}],
             metadata: %Paginator.Page.Metadata{
               after: nil,
               before: "g3QAAAABZAACaWRtAAAAJGVlZWUxY2YxLWQwZDAtNGM3YS1iYWQxLTJjYmQyYjk4MjJiYg==",
               limit: 2,
               total_count: 1,
               total_count_cap_exceeded: false
             }
           } = paginate(@ordered_ids, &getter/1, opts(after: cursor_after_2))
  end

  test "can page backwards" do
    cursor_before = "g3QAAAABZAACaWRtAAAAJGVlZWUxY2YxLWQwZDAtNGM3YS1iYWQxLTJjYmQyYjk4MjJiYg=="

    assert %Paginator.Page{
             entries: [%{x: "ho"}, %{x: "ox"}],
             metadata: %Paginator.Page.Metadata{
               after: "g3QAAAABZAACaWRtAAAAJGRkZGQ0M2JmLTA1ZjAtNDAzYi1hZDliLTBkYmZhNWJhZDYzNg==",
               before: "g3QAAAABZAACaWRtAAAAJGNjY2M4YjI1LWYyMGItNDI1Yy1hZDBjLTM3N2Q4Y2M4NDM1Ng==",
               limit: 2,
               total_count: 2,
               total_count_cap_exceeded: false
             }
           } = paginate(@ordered_ids, &getter/1, opts(before: cursor_before))
  end

  test "can filter" do
    filter_fn = fn
      %{x: "h" <> _} -> false
      _ -> true
    end

    assert %Paginator.Page{entries: [%{x: "ox"}, %{x: "za"}]} =
             paginate(@ordered_ids, &getter/1, opts(filter_fn: filter_fn))
  end
end
