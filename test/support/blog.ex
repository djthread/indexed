defmodule Blog do
  @moduledoc """
  Facade for operations.
  """
  import Ecto.Query
  alias Indexed.Test.Repo

  @pubsub Blog
  @context Blog

  def subscribe_to_user(id), do: subscribe(user_subtopic(id))
  def unsubscribe_from_user(id), do: unsubscribe(user_subtopic(id))
  def subscribe_to_flare_piece(id), do: subscribe(flare_piece_subtopic(id))
  def unsubscribe_from_flare_piece(id), do: unsubscribe(flare_piece_subtopic(id))

  def user_subtopic(id), do: "user-#{id}"
  def flare_piece_subtopic(id), do: "flare_piece-#{id}"

  def subscribe(topic) do
    maybe_send([:subscribe, topic])
    Phoenix.PubSub.subscribe(@pubsub, topic)
  end

  def unsubscribe(topic) do
    maybe_send([:unsubscribe, topic])
    Phoenix.PubSub.unsubscribe(@pubsub, topic)
  end

  @spec broadcast(any, atom, String.t(), [atom]) :: {:ok, User.t()} | any
  def broadcast({:ok, %User{id: id} = user} = result, event) do
    full_topic = user_subtopic(id)
    Phoenix.PubSub.broadcast(@pubsub, full_topic, {@context, event, user})
    result
  end

  def broadcast({:ok, %FlarePiece{user_id: id} = flare} = result, event) do
    full_topic = user_subtopic(id)
    Phoenix.PubSub.broadcast(@pubsub, full_topic, {@context, event, flare})
    result
  end

  def broadcast(result, _, _, _), do: result

  def all_posts, do: Repo.all(post_with_first_commenter_id_query(Post))
  def all_comments, do: Repo.all(Comment)
  def all_users, do: Repo.all(User)

  def all_replies(this_blog: true), do: Repo.all(from Reply, where: [this_blog: true])
  def all_replies, do: Repo.all(Reply)

  def get_post(id), do: Repo.get(post_with_first_commenter_id_query(Post), id)
  def get_user(id) when is_integer(id), do: Repo.get(User, id)
  def get_user(id), do: Repo.get_by(User, name: id)

  def create_post(author_id, content) do
    BlogServer.call({:create_post, author_id, content})
  end

  def create_comment(author_id, post_id, content) do
    BlogServer.call({:create_comment, author_id, post_id, content})
  end

  def create_user(name, flare_piece_names \\ []) do
    pieces = Enum.map(flare_piece_names, &%{name: &1})
    params = %{name: name, flare_pieces: pieces}
    %User{} |> User.changeset(params) |> Repo.insert()
  end

  def update_post(post_id, params) do
    BlogServer.call({:update_post, post_id, Map.new(params)})
  end

  def update_comment(comment_id, content) do
    BlogServer.call({:update_comment, comment_id, content})
  end

  def update_flare(flare, params) do
    flare |> FlarePiece.changeset(params) |> Repo.update() |> broadcast([:user, :update])
  end

  def update_user(user, params) do
    user |> User.changeset(params) |> Repo.update() |> broadcast([:user, :update])
  end

  def forget_post(post_id), do: BlogServer.call({:forget_post, post_id})
  def delete_comment(comment_id), do: BlogServer.call({:delete_comment, comment_id})

  # If a feedback pid is registered for the current process, send it a message.
  def maybe_send(msg) do
    with pid when is_pid(pid) <- Process.get(:feedback_pid),
         do: send(pid, msg)

    :ok
  end

  def post_with_first_commenter_id_query(Post) do
    from p in Post,
      distinct: ^[asc: :id],
      left_join: c in assoc(p, :comments),
      select_merge: %{first_commenter_id: c.author_id}
  end
end
