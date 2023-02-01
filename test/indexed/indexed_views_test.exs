defmodule IndexedViewsTest do
  @moduledoc ~S/Test "view" functionality./
  use ExUnit.Case
  import Indexed

  # (Who needs album names?)
  @albums [
    %Album{id: 1, label: "Liquid V Recordings", media: "Vinyl", artist: "Calibre"},
    %Album{id: 2, label: "Hospital Records", media: "CD", artist: "Logistics"},
    %Album{id: 3, label: "Hospital Records", media: "FLAC", artist: "London Elektricity"},
    %Album{id: 4, label: "Liquid V Recordings", media: "CD", artist: "Roni Size"},
    %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "S.P.Y"}
  ]

  @pubsub IndexedViewsTest.PubSub

  setup do
    params = [label: "Hospital Records", starts_with: "Lo"]
    print = "eb36c402b810b2cdc87bbaec"

    index =
      warm(
        albums: [
          data: {:asc, :artist, @albums},
          fields: [:artist, :media],
          prefilters: [
            nil: [maintain_unique: [:media]],
            label: [maintain_unique: [:media]]
          ]
        ]
      )

    {:ok, view} =
      create_view(index, :albums, print,
        prefilter: {:label, "Hospital Records"},
        maintain_unique: [:id],
        filter: &String.contains?(&1.artist, "Lo")
      )

    [fingerprint: print, index: index, params: params, view: view]
  end

  describe "fingerprint" do
    test "typical", %{fingerprint: fingerprint, params: params} do
      assert fingerprint == Indexed.View.fingerprint(params)
    end

    test "list param", %{params: params} do
      assert "cbcb293fbd4803362aa6b18d" == Indexed.View.fingerprint([{:fooz, [1, 2]} | params])
    end
  end

  test "get view", %{fingerprint: fingerprint, index: index, view: view} do
    expected_view = %Indexed.View{
      maintain_unique: [:id],
      prefilter: {:label, "Hospital Records"},
      filter: view.filter
    }

    assert expected_view == view
    assert expected_view == get_view(index, :albums, fingerprint)
    assert view.filter.(%{artist: "this has Lo in it"})
  end

  describe "just warmed up" do
    test "get view records", %{fingerprint: fingerprint, index: index} do
      a1 = %Album{artist: "Logistics", id: 2, label: "Hospital Records", media: "CD"}
      a2 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}

      assert [a1, a2] == get_records(index, :albums, fingerprint, :artist)
      assert %{2 => 1, 3 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
      assert [2, 3] == get_uniques_list(index, :albums, fingerprint, :id)

      assert [a2, a1] == get_records(index, :albums, fingerprint, {:desc, :media})
    end
  end

  describe "with a new record" do
    test "record is added to the view", %{fingerprint: fingerprint, index: index} do
      start_pubsub()
      Phoenix.PubSub.subscribe(@pubsub, fingerprint)

      album = %Album{id: 6, artist: "Nu:Logic", label: "Hospital Records", media: "FLAC"}
      put(index, :albums, album)

      assert_receive {Indexed, [:add], %{fingerprint: fingerprint, record: ^album}}

      a1 = %Album{artist: "Logistics", id: 2, label: "Hospital Records", media: "CD"}
      a2 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}
      assert [a1, a2, album] == get_records(index, :albums, fingerprint, :artist)

      assert [2, 3, 6] == get_uniques_list(index, :albums, fingerprint, :id)
      assert %{2 => 1, 3 => 1, 6 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
    end
  end

  describe "with a record updated" do
    test "record is added to the view", %{fingerprint: fingerprint, index: index} do
      start_pubsub()
      Phoenix.PubSub.subscribe(@pubsub, fingerprint)

      album = %Album{id: 5, label: "Hospital Records", media: "FLAC", artist: "Nu:Logic"}
      put(index, :albums, album)

      assert_receive {Indexed, [:add], %{fingerprint: fingerprint, record: ^album}}

      a1 = %Album{artist: "Logistics", id: 2, label: "Hospital Records", media: "CD"}
      a2 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}
      assert [a1, a2, album] == get_records(index, :albums, fingerprint, :artist)

      assert [2, 3, 5] == get_uniques_list(index, :albums, fingerprint, :id)
      assert %{2 => 1, 3 => 1, 5 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
    end

    test "record is removed from view cuz filter", %{fingerprint: fingerprint, index: index} do
      start_pubsub()
      Phoenix.PubSub.subscribe(@pubsub, fingerprint)

      album = %Album{id: 2, label: "Hospital Records", media: "FLAC", artist: "Whiney"}
      put(index, :albums, album)

      assert_receive {Indexed, [:remove], %{fingerprint: fingerprint, id: 2}}

      a = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}
      assert [a] == get_records(index, :albums, fingerprint, :artist)

      assert [3] == get_uniques_list(index, :albums, fingerprint, :id)
      assert %{3 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
    end

    test "record is removed from view cuz prefilter", %{fingerprint: fingerprint, index: index} do
      start_pubsub()
      Phoenix.PubSub.subscribe(@pubsub, fingerprint)

      album = %Album{id: 2, label: "Haha Not Hospital", media: "CD", artist: "Logistics"}
      put(index, :albums, album)

      assert_receive {Indexed, [:remove], %{fingerprint: fingerprint, id: 2}}

      a1 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}
      assert [a1] == get_records(index, :albums, fingerprint, :artist)

      assert [3] == get_uniques_list(index, :albums, fingerprint, :id)
      assert %{3 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
    end

    test "record is resorted", %{fingerprint: fingerprint, index: index} do
      start_pubsub()
      Phoenix.PubSub.subscribe(@pubsub, fingerprint)

      album = %Album{id: 2, label: "Hospital Records", media: "FLAC", artist: "Nu:Logic"}
      put(index, :albums, album)

      assert_receive {Indexed, [:update], %{fingerprint: fingerprint, record: album}}

      a1 = %Album{artist: "London Elektricity", id: 3, label: "Hospital Records", media: "FLAC"}
      assert [a1, album] == get_records(index, :albums, fingerprint, :artist)

      assert [2, 3] == get_uniques_list(index, :albums, fingerprint, :id)
      assert %{2 => 1, 3 => 1} == get_uniques_map(index, :albums, fingerprint, :id)
    end
  end

  test "destroy view", %{fingerprint: fingerprint, index: index} do
    :ok = destroy_view(index, :albums, fingerprint)

    refute Map.has_key?(get_index(index, views_key(:albums)), fingerprint)

    should_be_empty = [
      get_index(index, :albums, fingerprint, :artist),
      get_index(index, :albums, fingerprint, {:desc, :artist}),
      get_index(index, uniques_map_key(:albums, fingerprint, :id), []),
      get_index(index, uniques_list_key(:albums, fingerprint, :field_name), [])
    ]

    assert Enum.all?(should_be_empty, &Enum.empty?/1)

    # Records are still there.
    assert %{artist: "London Elektricity"} = get(index, :albums, 3)
  end

  test "destroy non-existent view", %{index: index} do
    assert :error == Indexed.destroy_view(index, :albums, "what's a fingerprint?")
  end

  # Start a test PubSub and configure indexed to use it.
  defp start_pubsub do
    start_supervised!({Phoenix.PubSub, name: @pubsub})
    Application.put_env(:indexed, :pubsub, @pubsub)
  end

  describe "drop" do
    defp records(i, fp), do: Indexed.get_records(i, :albums, fp, :artist)
    defp list(i, fp), do: Indexed.get_uniques_list(i, :albums, fp, :id)
    defp map(i, fp), do: Indexed.get_uniques_map(i, :albums, fp, :id)

    test "basic", %{fingerprint: fp, index: i} do
      assert [%{id: id1}, %{id: id2}] = records(i, fp)
      assert %{2 => 1, 3 => 1} == map(i, fp)
      assert [2, 3] == list(i, fp)

      Indexed.drop(i, :albums, id1)
      Indexed.drop(i, :albums, id2)

      assert [] == records(i, fp)
      assert %{} == map(i, fp)
      assert [] == list(i, fp)
    end
  end
end
