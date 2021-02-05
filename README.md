# Indexed

Indexed is a tool for managing records in ETS.

A record is a map with an `:id` key (perhaps an Ecto.Schema struct). An ETS
table stores all such records of a given entity, keyed by id.

Configure and warm your cache with some data and get an `%Indexed.Index{}` in
return. Pass this struct into `Indexed` functions to get, update, and paginate
records.

## Pagination

`Indexed.Paginator.paginate/4` imitates the interface of the similarly-named
function in the cursor-based pagination library,
[`paginator`](https://github.com/duffelhq/paginator/). The idea is that
server-side solutions are able to switch between using `paginator` to access
the database and `indexed` for fast, in-memory data, without any changes
being required on the client.

See `Indexed.Paginator` for more details.

## Installation

```elixir
def deps do
  [
    {:indexed,
      git: "https://github.com/instinctscience/indexed.git",
      branch: "main"}
  ]
end
```

## Examples

```elixir
defmodule Car do
  defstruct [:id, :make]
end

cars = [
  %Car{id: 1, make: "Lamborghini"},
  %Car{id: 2, make: "Mazda"}
]

# Sidenote: for date fields, instead of an atom (`:make`) use a tuple and add
# `:date` like `{:updated_at, :date}`.
index =
  Indexed.warm(
    cars: [fields: [:make], data: {:asc, :make, cars}]
  )

%Car{id: 1, make: "Lamborghini"} = car = Indexed.get(index, :cars, 1)

# `true` is a hint that the record is already held in the cache,
# speeding the operation a bit.
Indexed.set_record(index, :cars, %{car | make: "Lambo"}, true)

%Car{id: 1, make: "Lambo"} = Indexed.get(index, :cars, 1)

# `false` indicates that we know for sure the car didn't exist before.
Indexed.set_record(index, :cars, %Car{id: 3, make: "Tesla"}, false)

%Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)

# Next, let's look at the paginator capability...

after_cursor = "g3QAAAACZAACaWRhAmQABG1ha2VtAAAABU1hemRh"

%Paginator.Page{
  entries: [
    %Car{id: 3, make: "Tesla"},
    %Car{id: 2, make: "Mazda"}
  ],
  metadata: %Paginator.Page.Metadata{
    after: ^after_cursor,
    before: nil,
    limit: 2,
    total_count: nil,
    total_count_cap_exceeded: false
  }
} = Indexed.paginate(index, :cars, limit: 2, order_field: :make, order_direction: :desc)

%Paginator.Page{
  entries: [
    %Car{id: 1, make: "Lambo"}
  ],
  metadata: %Paginator.Page.Metadata{
    after: nil,
    before: "g3QAAAACZAACaWRhAWQABG1ha2VtAAAABUxhbWJv",
    limit: 2,
    total_count: nil,
    total_count_cap_exceeded: false
  }
} =
  Indexed.paginate(index, :cars,
    after: after_cursor,
    limit: 2,
    total_count: nil,
    order_field: :make,
    order_direction: :desc
  )
```
