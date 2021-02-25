defmodule IndexedPrefilterTest do
  @moduledoc "Test `:prefilter` and its `:maintain_unique` option."
  use ExUnit.Case

  # (Who needs album names?)
  @albums [
    %Album{id: 1, label: "Liquid V Recordings", media: "Vinyl", artist: "Calibre"},
    %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
    %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "London Elektricity"},
    %Album{id: 4, label: "Liquid V Recordings", media: "CD", artist: "Roni Size"},
    %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
  ]

  setup do
    [
      index:
        Indexed.warm(
          albums: [
            data: {:asc, :artist, @albums},
            fields: [:artist],
            prefilters: [
              nil: [maintain_unique: [:media]],
              label: [maintain_unique: [:media]]
            ]
          ]
        )
    ]
  end

  test "counts_map", %{index: index} do
    assert %{"CD" => 2, "FLAC" => 2, "Vinyl" => 1} ==
             Indexed.get_uniques_map(index, :albums, nil, :media)

    assert ["CD", "FLAC", "Vinyl"] ==
             Indexed.get_uniques_list(index, :albums, nil, :media)

    prefilter = {:label, "Hospital Records"}

    assert %{"CD" => 1, "FLAC" => 2} ==
             Indexed.get_uniques_map(index, :albums, prefilter, :media)

    assert ["CD", "FLAC"] ==
             Indexed.get_uniques_list(index, :albums, prefilter, :media)
  end

  test "basic prefilter", %{index: index} do
    assert {:ok,
            %Paginator.Page{
              entries: [
                %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
                %Album{
                  id: 3,
                  label: "Hospital Records",
                  media: "FLAC",
                  artist: "London Elektricity"
                },
                %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
              ],
              metadata: %Paginator.Page.Metadata{
                after: nil,
                before: nil,
                limit: 10,
                total_count: nil,
                total_count_cap_exceeded: false
              }
            }} ==
             Indexed.paginate(index, :albums,
               order_field: :artist,
               order_direction: :asc,
               prefilter: {:label, "Hospital Records"}
             )
  end

  describe "sorted indexes by field are maintained" do
    test "basic list with prefilter", %{index: index} do
      assert [%{artist: "Logistics"}, %{artist: "London Elektricity"}, %{artist: "S.P.Y"}] =
               Indexed.get_records(index, :albums, {:label, "Hospital Records"}, :artist, :asc)
    end

    test "when one is added", %{index: index} do
      album = %Album{id: 7, label: "Hospital Records", media: "CD", artist: "Danny Byrd"}
      Indexed.put(index, :albums, album)

      assert [
               %{artist: "Danny Byrd"},
               %{artist: "Logistics"},
               %{artist: "London Elektricity"},
               %{artist: "S.P.Y"}
             ] = Indexed.get_records(index, :albums, {:label, "Hospital Records"}, :artist, :asc)
    end

    test "when one is moved to another prefilter", %{index: index} do
      album = %Album{id: 7, label: "Hospital Records", media: "CD", artist: "Danny Byrd"}
      Indexed.put(index, :albums, album)
      album = %Album{id: 7, label: "Liquid V Recordings", media: "CD", artist: "Danny Byrd"}
      Indexed.put(index, :albums, album)

      assert [
               %{artist: "Logistics"},
               %{artist: "London Elektricity"},
               %{artist: "S.P.Y"}
             ] = Indexed.get_records(index, :albums, {:label, "Hospital Records"}, :artist, :asc)
    end

    test "when a resort is needed within the same prefilter", %{index: index} do
      album = %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "Whiney"}
      Indexed.put(index, :albums, album)

      assert [
               %{artist: "Logistics"},
               %{artist: "S.P.Y"},
               %{artist: "Whiney"}
             ] = Indexed.get_records(index, :albums, {:label, "Hospital Records"}, :artist, :asc)
    end
  end

  describe "get_uniques_list" do
    test "basic", %{index: index} do
      # This is available because prefilter field keys imply manage_uniques on
      # the top level (prefilter nil).
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_uniques_list(index, :albums, nil, :label)

      # manage_uniques for media was defined on top level (prefilter nil).
      assert ~w(CD FLAC Vinyl) ==
               Indexed.get_uniques_list(index, :albums, nil, :media)

      # Get unique media values behind the "label=Hospital Records" prefilter.
      assert ~w(CD FLAC) ==
               Indexed.get_uniques_list(index, :albums, {:label, "Hospital Records"}, :media)
    end

    test "adding a record updates uniques", %{index: index} do
      list = &Indexed.get_uniques_list(index, :albums, &1, :media)
      map = &Indexed.get_uniques_map(index, :albums, &1, :media)

      album = %Album{id: 7, label: "RAM Records", media: "Phonograph", artist: "Andy C"}
      Indexed.put(index, :albums, album)

      assert ["Phonograph"] == list.({:label, "RAM Records"})
      assert %{"Phonograph" => 1} == map.({:label, "RAM Records"})
    end

    test "moving a record between prefilters updates uniques", %{index: index} do
      list = &Indexed.get_uniques_list(index, :albums, &1, :media)

      album = %Album{id: 7, label: "RAM Records", media: "Phonograph", artist: "Andy C"}
      Indexed.put(index, :albums, album)

      album = %Album{id: 7, label: "A New Label", media: "Phonograph", artist: "Andy C"}
      Indexed.put(index, :albums, album)

      assert "Phonograph" in list.(nil)
      assert "Phonograph" in list.({:label, "A New Label"})

      # Make sure the uniques table for RAM Records is deleted.
      assert is_nil(list.({:label, "RAM Records"}))
    end

    test "moving a couple ways at once is cool", %{index: index} do
      list = &Indexed.get_uniques_list(index, :albums, &1, :media)
      album = %Album{id: 7, label: "RAM Records", media: "Phonograph", artist: "Andy C"}
      Indexed.put(index, :albums, album)

      album = %Album{id: 7, label: "A New Label", media: "Yak Bak", artist: "Andy C"}
      Indexed.put(index, :albums, album)

      refute "Phonograph" in list.(nil)
      assert ["CD", "FLAC", "Vinyl", "Yak Bak"] == list.(nil)

      # Make sure the uniques table for RAM Records is deleted.
      assert is_nil(list.({:label, "RAM Records"}))
    end
  end

  describe "looks good after adding a record" do
    setup %{index: index} do
      album = %{id: 6, label: "Hospital Records", media: "Minidisc", artist: "Bop"}
      Indexed.put(index, :albums, album)
      [album: album]
    end

    test "basic prefilter", %{album: album, index: index} do
      assert {:ok,
              %Paginator.Page{
                entries: [
                  ^album,
                  %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
                  %Album{
                    id: 3,
                    label: "Hospital Records",
                    media: "FLAC",
                    artist: "London Elektricity"
                  },
                  %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
                ]
              }} =
               Indexed.paginate(index, :albums,
                 order_field: :artist,
                 order_direction: :asc,
                 prefilter: {:label, "Hospital Records"}
               )
    end

    test "get_uniques_list", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_uniques_list(index, :albums, nil, :label)
    end
  end

  describe "looks good after wholly updating a record" do
    setup %{index: index} do
      album = %Album{id: 2, label: "Shogun Audio", media: "8-track", artist: "Fourward"}
      Indexed.put(index, :albums, album)
      [album: album]
    end

    test "get_uniques_list", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings", "Shogun Audio"] ==
               Indexed.get_uniques_list(index, :albums, nil, :label)

      assert ["8-track", "CD", "FLAC", "Vinyl"] ==
               Indexed.get_uniques_list(index, :albums, nil, :media)

      assert ["FLAC"] ==
               Indexed.get_uniques_list(index, :albums, {:label, "Hospital Records"}, :media)

      assert ["8-track"] ==
               Indexed.get_uniques_list(index, :albums, {:label, "Shogun Audio"}, :media)
    end
  end

  test "moving a record between prefilters creates and drops prefilter", %{index: index} do
    list = &Indexed.get_uniques_list(index, :albums, &1, :media)
    map = &Indexed.get_uniques_map(index, :albums, &1, :media)

    album = %Album{id: 7, label: "RAM Records", media: "Phonograph", artist: "Andy C"}
    Indexed.put(index, :albums, album)

    assert ["Phonograph"] == list.({:label, "RAM Records"})
    assert %{"Phonograph" => 1} == map.({:label, "RAM Records"})

    album = %Album{id: 7, label: "A New Label", media: "Phonograph", artist: "Andy C"}
    Indexed.put(index, :albums, album)

    assert ["CD", "FLAC", "Phonograph", "Vinyl"] == list.(nil)

    # Make sure the uniques table for RAM Records is deleted.
    assert is_nil(list.({:label, "RAM Records"}))
  end

  describe "drop" do
    defp records(i, pf), do: Indexed.get_records(i, :albums, pf, :artist, :asc)
    defp list(i, pf), do: Indexed.get_uniques_list(i, :albums, pf, :media)
    defp map(i, pf), do: Indexed.get_uniques_map(i, :albums, pf, :media)

    test "losing last unique value occurrence for pf cleans up index entries", %{index: i} do
      prefilter = {:label, "Liquid V Recordings"}

      assert [%{id: id1}, %{id: id2}] = records(i, prefilter)
      assert %{"CD" => 1, "Vinyl" => 1} == map(i, prefilter)
      assert ["CD", "Vinyl"] == list(i, prefilter)

      Indexed.drop(i, :albums, id1)
      Indexed.drop(i, :albums, id2)

      assert nil == records(i, prefilter)
      assert nil == map(i, prefilter)
      assert nil == list(i, prefilter)
    end
  end
end
