defmodule Album do
  defstruct [:id, :label, :media, :artist]
end

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
             Indexed.get_uniques_map(index, :albums, :media, nil)

    assert ["CD", "FLAC", "Vinyl"] ==
             Indexed.get_uniques_list(index, :albums, :media, nil)

    prefilter = {:label, "Hospital Records"}

    assert %{"CD" => 1, "FLAC" => 2} ==
             Indexed.get_uniques_map(index, :albums, :media, prefilter)

    assert ["CD", "FLAC"] ==
             Indexed.get_uniques_list(index, :albums, :media, prefilter)
  end

  test "basic prefilter", %{index: index} do
    assert %Paginator.Page{
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
           } ==
             Indexed.paginate(index, :albums,
               order_field: :artist,
               order_direction: :asc,
               prefilter: {:label, "Hospital Records"}
             )
  end

  test "get_uniques_list", %{index: index} do
    # This is available because prefilter field keys imply manage_uniques on
    # the top level (prefilter nil).
    assert ["Hospital Records", "Liquid V Recordings"] ==
             Indexed.get_uniques_list(index, :albums, :label)

    # manage_uniques for media was defined on top level (prefilter nil).
    assert ~w(CD FLAC Vinyl) ==
             Indexed.get_uniques_list(index, :albums, :media)

    # Get unique media values behind the "label=Hospital Records" prefilter.
    assert ~w(CD FLAC) ==
             Indexed.get_uniques_list(index, :albums, :media, {:label, "Hospital Records"})
  end

  describe "looks good after adding a record" do
    setup %{index: index} do
      album = %{id: 6, label: "Hospital Records", media: "Minidisc", artist: "Bop"}
      Indexed.set_record(index, :albums, album)
      [album: album]
    end

    test "basic prefilter", %{album: album, index: index} do
      assert %Paginator.Page{
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
             } =
               Indexed.paginate(index, :albums,
                 order_field: :artist,
                 order_direction: :asc,
                 prefilter: {:label, "Hospital Records"}
               )
    end

    test "get_uniques_list", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_uniques_list(index, :albums, :label)
    end
  end

  describe "looks good after updating a record" do
    setup %{index: index} do
      album = %Album{id: 2, label: "Hospital Records", media: "8-track", artist: "Logistics"}
      Indexed.set_record(index, :albums, album)
      [album: album]
    end

    test "basic prefilter", %{album: album, index: index} do
      assert %Paginator.Page{
               entries: [
                 ^album,
                 %Album{
                   id: 3,
                   label: "Hospital Records",
                   media: "FLAC",
                   artist: "London Elektricity"
                 },
                 %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
               ]
             } =
               Indexed.paginate(index, :albums,
                 order_field: :artist,
                 order_direction: :asc,
                 prefilter: {:label, "Hospital Records"}
               )
    end

    test "get_uniques_list", %{index: index} do
      assert ["Hospital Records", "Liquid V Recordings"] ==
               Indexed.get_uniques_list(index, :albums, :label)

      assert ["8-track", "FLAC"] ==
               Indexed.get_uniques_list(index, :albums, :media, {:label, "Hospital Records"})
    end
  end
end
