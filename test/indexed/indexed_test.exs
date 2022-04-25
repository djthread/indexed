defmodule Car do
  defstruct [:id, :make, :inserted_at]
end

defmodule CarWithKeyId do
  defstruct [:key, :xtra, :make, :inserted_at]
end

defmodule IndexedTest do
  use ExUnit.Case

  @cars [
    %Car{id: 1, make: "Lamborghini"},
    %Car{id: 2, make: "Mazda"}
  ]

  setup do
    [
      index:
        Indexed.warm(
          cars: [
            fields: [:make],
            data: {:asc, :make, @cars},
            prefilters: [:make]
          ]
        )
    ]
  end

  defp add_tesla(index),
    do: Indexed.put(index, :cars, %Car{id: 3, make: "Tesla"})

  test "get", %{index: index} do
    assert %Car{id: 1, make: "Lamborghini"} == Indexed.get(index, :cars, 1)
    assert is_nil(Indexed.get(index, :cars, 9))
  end

  test "get_records", %{index: index} do
    assert [%Car{id: 1, make: "Lamborghini"}, %Car{id: 2, make: "Mazda"}] ==
             Indexed.get_records(index, :cars, nil, :make)
  end

  describe "put" do
    test "when already held", %{index: index} do
      uniques_map = &Indexed.get_uniques_map(index, :cars, &1, :make)

      assert %{"Lamborghini" => 1, "Mazda" => 1} == uniques_map.(nil)

      car = Indexed.get(index, :cars, 1)
      Indexed.put(index, :cars, %{car | make: "Lambo"})

      assert %Car{id: 1, make: "Lambo"} == Indexed.get(index, :cars, 1)
      assert %{"Lambo" => 1, "Mazda" => 1} == uniques_map.(nil)
    end

    test "when not already held", %{index: index} do
      uniques_map = &Indexed.get_uniques_map(index, :cars, &1, :make)

      Indexed.put(index, :cars, %Car{id: 4, make: "GM"})

      assert %Car{id: 4, make: "GM"} == Indexed.get(index, :cars, 4)
      assert %{"Lamborghini" => 1, "Mazda" => 1, "GM" => 1} == uniques_map.(nil)
    end
  end

  describe "drop" do
    test "basic", %{index: index} do
      get = fn -> Indexed.get(index, :cars, 1) end
      assert %{id: 1, make: "Lamborghini"} = get.()
      :ok = Indexed.drop(index, :cars, 1)
      assert nil == get.()
    end

    test "non-existent", %{index: index} do
      get = fn -> Indexed.get(index, :cars, 99) end
      assert nil == get.()
      :error = Indexed.drop(index, :cars, 99)
    end
  end

  test "index_key" do
    assert "idx_cars[]asc_color" == Indexed.index_key("cars", nil, "color")
  end

  describe "get_index" do
    test "happy", %{index: index} do
      assert [2, 1] == Indexed.get_index(index, :cars, nil, {:desc, :make})
    end

    test "raise on no such index", %{index: index} do
      assert is_nil(Indexed.get_index(index, :cars, nil, {:desc, :whoops}))
    end
  end

  describe "paginate" do
    test "typical", %{index: index} do
      add_tesla(index)

      assert %Paginator.Page{
               entries: [
                 %Car{id: 3, make: "Tesla"},
                 %Car{id: 2, make: "Mazda"}
               ],
               metadata: %Paginator.Page.Metadata{
                 after: "g3QAAAACZAACaWRhAmQABG1ha2VtAAAABU1hemRh",
                 before: nil,
                 limit: 2,
                 total_count: nil,
                 total_count_cap_exceeded: false
               }
             } = Indexed.paginate(index, :cars, limit: 2, order_by: {:desc, :make})
    end

    test "no ref" do
      assert_raise ArgumentError, fn ->
        Indexed.paginate(%Indexed{}, :cars, limit: 2, order_by: :balh)
      end
    end

    test "no such index", %{index: index} do
      assert is_nil(Indexed.paginate(index, :what, limit: 2, order_by: :lol))
    end
  end

  describe "warm" do
    test "data field hint is not among those indexed" do
      assert_raise RuntimeError, fn ->
        Indexed.warm(cars: [fields: [:make], data: {:asc, :what, @cars}])
      end
    end

    test "data direction hint is not asc or desc" do
      assert_raise RuntimeError, fn ->
        Indexed.warm(cars: [fields: [:make], data: {:whoops, :make, @cars}])
      end
    end

    test "no data ordering hint is okay" do
      index = Indexed.warm(cars: [fields: [:make], data: @cars])
      assert %Car{id: 1, make: "Lamborghini"} == Indexed.get(index, :cars, 1)
    end
  end

  test "readme" do
    cars = [
      %Car{id: 1, make: "Lamborghini"},
      %Car{id: 2, make: "Mazda"}
    ]

    index = Indexed.warm(cars: [fields: [:make], data: {:asc, :make, cars}])

    assert %Car{id: 1, make: "Lamborghini"} = car = Indexed.get(index, :cars, 1)

    Indexed.put(index, :cars, %{car | make: "Lambo"})

    assert %Car{id: 1, make: "Lambo"} = Indexed.get(index, :cars, 1)

    Indexed.put(index, :cars, %Car{id: 3, make: "Tesla"})

    assert %Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)

    assert [%Car{make: "Lambo"}, %Car{make: "Mazda"}, %Car{make: "Tesla"}] =
             Indexed.get_records(index, :cars, nil, :make)

    after_cursor = "g3QAAAACZAACaWRhAmQABG1ha2VtAAAABU1hemRh"

    assert %Paginator.Page{
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
           } = Indexed.paginate(index, :cars, limit: 2, order_by: {:desc, :make})

    assert %Paginator.Page{
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
               order_by: {:desc, :make}
             )
  end

  test "nil prefilter raises" do
    assert_raise RuntimeError, fn ->
      Indexed.warm(
        cars: [
          fields: [:hi],
          data: [%{hi: 2}],
          prefilters: [nil]
        ]
      )
    end
  end

  test "datetime sort" do
    cars = [
      %Car{id: 1, make: "Lamborghini", inserted_at: ~U[2021-02-14 08:14:10.715462Z]},
      %Car{id: 2, make: "Mazda", inserted_at: ~U[2021-02-14 08:14:15.004640Z]}
    ]

    index =
      Indexed.warm(
        cars: [
          fields: [{:inserted_at, sort: :date_time}],
          data: cars
        ]
      )

    Indexed.put(index, :cars, %Car{
      id: 3,
      make: "Pinto",
      inserted_at: ~U[2021-02-14 08:14:12.004640Z]
    })

    assert [%{id: 2}, %{id: 3}, %{id: 1}] =
             Indexed.get_records(index, :cars, nil, {:desc, :inserted_at})
  end

  describe "id_key option" do
    test "with atom key name" do
      cars = [%CarWithKeyId{key: "cool", make: "Mazda"}]

      index = Indexed.warm(cars: [fields: [:make], id_key: :key, data: {:asc, :make, cars}])

      Indexed.put(index, :cars, %CarWithKeyId{key: "tez", make: "Tesla"})

      assert %CarWithKeyId{key: "cool", make: "Mazda"} == Indexed.get(index, :cars, "cool")
      assert %CarWithKeyId{key: "tez", make: "Tesla"} == Indexed.get(index, :cars, "tez")

      Indexed.put(index, :cars, %CarWithKeyId{key: "tez", make: "Something Else"})

      assert %CarWithKeyId{key: "tez", make: "Something Else"} == Indexed.get(index, :cars, "tez")
    end

    test "with function" do
      make_id = &"#{&1.key}-#{&1.xtra}"

      car = %CarWithKeyId{key: "cool", xtra: 2, make: "Mazda"}

      index = Indexed.warm(cars: [fields: [:make], id_key: make_id, data: {:asc, :make, [car]}])

      assert car == Indexed.get(index, :cars, make_id.(car))
    end
  end
end
