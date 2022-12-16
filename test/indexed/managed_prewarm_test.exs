defmodule Indexed.ManagedPrewarmTest do
  use Indexed.TestCase
  alias Indexed.Test.Repo

  setup do
    start_supervised!({Phoenix.PubSub, name: Blog})
    :ok
  end

  defp paginate, do: BlogPrewarmingServer.paginate(:posts, order_by: [:inserted_at])
  defp entries, do: paginate().entries

  defp basic_setup do
    Repo.insert!(%Post{content: "Hello World"})
    Repo.insert!(%Post{content: "My post is the best."})

    pid = start_supervised!(BlogPrewarmingServer.child_spec(feedback_pid: self()))

    # GenServer.call to make sure it's done with its handle_continue.
    :sys.get_state(pid)
  end

  test "basic" do
    basic_setup()

    assert [
             %{content: "My post is the best."},
             %{content: "Hello World"}
           ] = entries()
  end
end
