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

  defp add_tesla(index), do: Indexed.add_record(index, :cars, %Car{id: 3, make: "Tesla"})

  test "get", %{index: index} do
    assert %Car{id: 1, make: "Lamborghini"} == Indexed.get(index, :cars, 1)
    assert is_nil(Indexed.get(index, :cars, 9))
  end

  test "get_values", %{index: index} do
    assert [%Car{id: 1, make: "Lamborghini"}, %Car{id: 2, make: "Mazda"}] ==
             Indexed.get_values(index, :cars, :make, :asc)
  end

  describe "update_record" do
    test "without already_held? hint, but it is already held", %{index: index} do
      car = Indexed.get(index, :cars, 1)
      Indexed.update_record(index, :cars, %{car | make: "Lambo"})
      assert %Car{id: 1, make: "Lambo"} == Indexed.get(index, :cars, 1)
    end

    test "without already_held? hint, but it is not already held", %{index: index} do
      Indexed.update_record(index, :cars, %Car{id: 4, make: "GM"})
      assert %Car{id: 4, make: "GM"} == Indexed.get(index, :cars, 4)
    end

    test "with already_held? hint, true", %{index: index} do
      car = Indexed.get(index, :cars, 1)
      Indexed.update_record(index, :cars, %{car | make: "Lambo"}, true)
      assert %Car{id: 1, make: "Lambo"} == Indexed.get(index, :cars, 1)
    end

    test "with already_held? hint, false", %{index: index} do
      Indexed.update_record(index, :cars, %Car{id: 5, make: "Ford"}, false)
      assert %Car{id: 5, make: "Ford"} == Indexed.get(index, :cars, 5)
    end
  end

  test "add_record", %{index: index} do
    add_tesla(index)
    %Car{id: 3, make: "Tesla"} = Indexed.get(index, :cars, 3)
  end

  test "paginate", %{index: index} do
    add_tesla(index)

    assert %Paginator.Page{
             entries: [
               %Car{id: 3, make: "Tesla"},
               %Car{id: 2, make: "Mazda"}
             ],
             metadata: %Paginator.Page.Metadata{
               after: "g3QAAAABZAAEbWFrZW0AAAAFTWF6ZGE=",
               before: nil,
               limit: 2,
               total_count: 2,
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
end
