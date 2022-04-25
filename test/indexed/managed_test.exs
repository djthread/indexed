defmodule Indexed.ManagedTest do
  use Indexed.TestCase
  alias Indexed.Test.Repo

  setup do
    start_supervised!({Phoenix.PubSub, name: Blog})
    :ok
  end

  @user_preload [:flare_pieces, best_friend: :best_friend]

  defp preload, do: [:first_commenter, author: @user_preload, comments: [author: @user_preload]]
  defp state(bs_pid), do: :sys.get_state(bs_pid)
  defp tracking(bs_pid, name), do: Map.fetch!(state(bs_pid).tracking, name)
  defp record(name, id, preload), do: BlogServer.run(& &1.get.(name, id, preload))
  defp records(name), do: BlogServer.run(& &1.get_records.(name))
  defp paginate, do: BlogServer.paginate(preload: preload())
  defp entries, do: paginate().entries

  defp basic_setup do
    {:ok, %{id: bob_id} = bob} = Blog.create_user("bob", ["pin"])
    {:ok, %{id: jill_id}} = Blog.create_user("jill", ["hat", "mitten"])
    {:ok, %{id: lee_id}} = Blog.create_user("lee", ["wig"])
    {:ok, %{id: lucy_id}} = Blog.create_user("lucy")
    {:ok, _} = Blog.update_user(bob, %{best_friend_id: lucy_id})

    Repo.insert!(%Post{
      author_id: bob_id,
      content: "Hello World",
      comments: [
        %Comment{author_id: bob_id, content: "hi"},
        %Comment{author_id: bob_id, content: "ho"}
      ]
    })

    Repo.insert!(%Post{
      author_id: bob_id,
      content: "My post is the best.",
      comments: [
        %Comment{author_id: jill_id, content: "wow"},
        %Comment{author_id: lee_id, content: "woah"}
      ]
    })

    bs = start_supervised!(BlogServer.child_spec(feedback_pid: self()))

    {s1, s2, s3, s4} = {"user-#{bob_id}", "user-#{jill_id}", "user-#{lee_id}", "user-#{lucy_id}"}
    assert_receive [:subscribe, ^s1]
    assert_receive [:subscribe, ^s2]
    assert_receive [:subscribe, ^s3]
    assert_receive [:subscribe, ^s4]

    %{bs_pid: bs, ids: %{bob: bob_id, jill: jill_id, lee: lee_id, lucy: lucy_id}}
  end

  test "a runthrough / scenario" do
    %{bs_pid: bs_pid, ids: %{bob: bob_id, jill: jill_id, lucy: lucy_id}} = basic_setup()

    bob = Blog.get_user("bob")
    {:ok, _} = Blog.update_user(bob, %{name: "fred"})

    assert [
             %{
               content: "My post is the best.",
               author:
                 %{
                   name: "fred",
                   best_friend: %{name: "lucy"},
                   flare_pieces: [%{id: pin_id, name: "pin"}]
                 } = bob,
               comments: [
                 %{
                   content: "woah",
                   author: %{id: lee_id, name: "lee", flare_pieces: [%{name: "wig"} = flare]}
                 },
                 %{
                   id: comment_id,
                   content: "wow",
                   author: %{id: _jill_id, name: "jill", flare_pieces: [_, _]}
                 }
               ]
             },
             %{
               content: "Hello World",
               author: %{name: "fred"},
               comments: [%{id: comment2_id, content: "ho"}, %{content: "hi"}]
             }
           ] = entries()

    assert [%{name: "fred"}, %{name: "jill"}, %{name: "lee"}, %{name: "lucy"}] = records(:users)
    assert %{^bob_id => 5, ^jill_id => 2, ^lee_id => 1, ^lucy_id => 1} = tracking(bs_pid, :users)

    {:ok, _} = Blog.delete_comment(comment_id)

    msg = "user-#{jill_id}"
    assert_receive [:unsubscribe, ^msg]
    assert [%{name: "fred"}, %{name: "lee"}, %{name: "lucy"}] = records(:users)
    assert %{bob_id => 5, lee_id => 2, lucy_id => 1} == tracking(bs_pid, :users)
    assert [%{comments: [%{content: "woah"}]}, %{comments: [_, _]}] = entries()

    refute Enum.any?(records(:flare_pieces), &(&1.name in ~w(hat mitten)))
    refute Enum.any?(records(:users), &(&1.name == "jill"))
    refute Enum.any?(records(:comments), &(&1.content == "wow"))

    {:ok, _} = Blog.update_flare(flare, %{name: "tupay"})

    assert %{name: "lee", flare_pieces: [%{name: "tupay"}]} =
             record(:users, lee_id, :flare_pieces)

    {:ok, _} =
      Blog.update_user(bob, %{
        name: "bob",
        flare_pieces: [%{id: pin_id, name: "sticker"}, %{name: "cap"}]
      })

    assert %{name: "bob", flare_pieces: [%{name: "cap"}, %{name: "sticker"}]} =
             record(:users, bob.id, :flare_pieces)

    assert %{content: "ho", author: %{name: "bob"}, post: %{content: "Hello World"}} =
             record(:comments, comment2_id, [:author, :post])
  end

  test "update entity which has foreign one AND many connections" do
    %{bs_pid: _bs_pid} = basic_setup()
    %{id: comment_id} = entries() |> hd() |> Map.fetch!(:comments) |> hd()
    msg = "new stuff to say"

    {:ok, _} = Blog.update_comment(comment_id, msg)

    %{content: ^msg} = entries() |> hd() |> Map.fetch!(:comments) |> hd()
  end

  test "update many assoc: of 2, update 1 and delete 1" do
    %{bs_pid: _bs_pid, ids: %{jill: jill_id}} = basic_setup()
    entry = fn -> Enum.find(entries(), &String.contains?(&1.content, "best")) end

    %{
      id: post_id,
      content: "My" <> _,
      comments: [%{id: c1_id, content: "woah"}, %{content: "wow"}]
    } = entry.()

    assert {:ok, %{content: "plenty best", comments: [%{id: ^c1_id, content: "woah indeed"}]}} =
             Blog.update_post(post_id,
               content: "plenty best",
               comments: [%{id: c1_id, content: "woah indeed"}]
             )

    msg = "user-#{jill_id}"
    assert_receive [:unsubscribe, ^msg]

    %{id: ^post_id, comments: [%{id: ^c1_id, content: "woah indeed"}]} = entry.()
  end

  test "only :one assoc updated" do
    {:ok, bob} = Blog.create_user("bob", ["pin"])
    Repo.insert!(%Post{author_id: bob.id, content: "Hello World"})
    entry = fn -> Enum.find(entries(), &String.contains?(&1.content, "Hello")) end
    start_supervised!(BlogServer.child_spec())

    assert %{content: "Hello World", author: %{name: "bob"}} = entry.()
    assert {:ok, _} = Blog.update_user(bob, %{name: "not bob"})
    assert %{content: "Hello World", author: %{name: "not bob"}} = entry.()
  end

  test "delete a post" do
    basic_setup()

    assert [
             %{
               author: %{
                 name: "bob",
                 best_friend: %{name: "lucy"},
                 flare_pieces: [%{name: "pin"}]
               },
               content: "My post is the best."
             },
             %{
               id: post_id,
               author: %{name: "bob"},
               content: "Hello World"
             }
           ] = entries()

    Blog.forget_post(post_id)

    assert [
             %{
               author: %{
                 name: "bob",
                 best_friend: %{name: "lucy"},
                 flare_pieces: [%{name: "pin"}]
               },
               comments: [
                 %{author: %{name: "lee", flare_pieces: [%{name: "wig"}]}, content: "woah"},
                 %{
                   author: %{name: "jill", flare_pieces: [%{name: "hat"}, %{name: "mitten"}]},
                   content: "wow"
                 }
               ],
               content: "My post is the best."
             }
           ] = entries()
  end

  test "Nesty McNesterson" do
    {:ok, %{id: bob_id} = bob} = Blog.create_user("bob", ["pin"])
    {:ok, %{id: lucy_id} = lucy} = Blog.create_user("lucy")

    Repo.insert!(%Post{
      author_id: bob_id,
      content: "Ya",
      comments: [%{content: "yo", author_id: bob_id}]
    })

    start_supervised!(BlogServer.child_spec())

    {:ok, %{id: mar_id}} = Blog.create_user("mar")
    {:ok, _} = Blog.update_user(bob, %{best_friend_id: lucy_id})
    {:ok, _} = Blog.update_user(lucy, %{best_friend_id: mar_id})

    [%{id: post_id, content: "Ya"}] = entries()
    {:ok, _} = Blog.update_post(post_id, content: "hey")

    assert [
             %{
               content: "hey",
               author: %{
                 name: "bob",
                 best_friend: %{name: "lucy", best_friend: %{name: "mar"}}
               },
               comments: [%{author: %{best_friend: %{best_friend: %{name: "mar"}}}}]
             }
           ] = entries()
  end

  @tag :skip
  test "records further than path get auto-deleted" do
    # see managed.ex line 308
  end
end
