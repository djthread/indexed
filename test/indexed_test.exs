defmodule Car do
  defstruct [:id, :make]
end

defmodule IndexedTest do
  use ExUnit.Case

  @cars [
    %Car{id: 1, make: "Lamborghini"},
    %Car{id: 2, make: "Mazda"}
  ]

  setup do
    [index: Indexed.warm(cars: [fields: [:make], data: {:asc, :make, @cars}])]
  end

  defp add_tesla(index),
    do: Indexed.set_record(index, :cars, %Car{id: 3, make: "Tesla"}, new_record?: true)

  test "get", %{index: index} do
    assert %Car{id: 1, make: "Lamborghini"} == Indexed.get(index, :cars, 1)
    assert is_nil(Indexed.get(index, :cars, 9))
  end

  test "get_values", %{index: index} do
    assert [%Car{id: 1, make: "Lamborghini"}, %Car{id: 2, make: "Mazda"}] ==
             Indexed.get_values(index, :cars, :make, :asc)
  end

  describe "set_record" do
    test "without new_record? hint, but it is already held", %{index: index} do
      car = Indexed.get(index, :cars, 1)
      Indexed.set_record(index, :cars, %{car | make: "Lambo"})
      assert %Car{id: 1, make: "Lambo"} == Indexed.get(index, :cars, 1)
    end

    test "without new_record? hint, but it is not already held", %{index: index} do
      Indexed.set_record(index, :cars, %Car{id: 4, make: "GM"})
      assert %Car{id: 4, make: "GM"} == Indexed.get(index, :cars, 4)
    end
  end

  describe "get_index" do
    test "happy", %{index: index} do
      assert [2, 1] == Indexed.get_index(index, :cars, :make, :desc)
    end

    test "raise on no such index", %{index: index} do
      assert_raise RuntimeError, fn ->
        Indexed.get_index(index, :cars, :whoops, :desc)
      end
    end
  end

  test "paginate", %{index: index} do
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
           } =
             Indexed.paginate(index, :cars, limit: 2, order_field: :make, order_direction: :desc)
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

    # `true` is a hint that the record is already held in the cache,
    # speeding the operation a bit.
    Indexed.set_record(index, :cars, %{car | make: "Lambo"}, already_held?: true)

    assert %Car{id: 1, make: "Lambo"} = Indexed.get(index, :cars, 1)

    # `false` indicates that we know for sure the car didn't exist before.
    Indexed.set_record(index, :cars, %Car{id: 3, make: "Tesla"}, already_held?: false)

    assert %Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)

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
           } =
             Indexed.paginate(index, :cars, limit: 2, order_field: :make, order_direction: :desc)

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
               order_field: :make,
               order_direction: :desc
             )
  end
end
