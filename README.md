# Indexed

Indexed is a tool for managing records in ETS.

A record is a map with an `:id` key (perhaps an Ecto.Schema struct). An ETS
table stores all such records of a given entity, keyed by id.

Configure and warm your cache with some data and get an `%Indexed.Index{}` in
return. Pass this struct into `Indexed` functions to get, update, and paginate
records.

## Pagination

`Indexed.Paginator.paginate/4` operates in a similar way to
the paginate function in the cursor-based pagination library,
[`paginator`](https://github.com/duffelhq/paginator/). The idea is that, if
one is using this library in their project and a certain type of query is
particularly common, `indexed` can speed it up significantly and free up this
database capacity, without changing the external interface.

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

## Example

```elixir
cars = [
  %Car{id: 1, make: "Lamborghini"},
  %Car{id: 2, make: "Mazda"}
]

index =
  Indexed.warm(
    cars: [fields: [:make], data: {:asc, :make, cars}]
  )

%Car{id: 1, make: "Lamborghini"} = car = Indexed.get(index, :cars, 1)

Indexed.update_record(index, :cars, %{car | make: "Lambo"})

%Car{id: 1, make: "Lambo"} = Indexed.get(index, :cars, 1)

Indexed.add_record(index, :cars, %Car{id: 3, make: "Tesla"})

%Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)
```
