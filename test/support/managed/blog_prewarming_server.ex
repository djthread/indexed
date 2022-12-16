defmodule BlogPrewarmingServer do
  @moduledoc """
  Managed-using GenServer with a namespace (named ETS tables)
  and prewarming only in init.
  """
  use GenServer
  use Indexed.Managed, namespace: :blog, repo: Indexed.Test.Repo

  managed :posts, Post, fields: [:inserted_at]

  def call(msg), do: GenServer.call(__MODULE__, msg)
  def run(fun), do: call({:run, fun})

  def child_spec(opts \\ []) do
    %{
      id: __MODULE__,
      start: {GenServer, :start_link, [__MODULE__, opts, [name: __MODULE__]]}
    }
  end

  @impl GenServer
  def init(_opts) do
    {:ok, prewarm(), {:continue, :warm}}
  end

  @impl GenServer
  def handle_continue(:warm, state) do
    {:noreply, warm(state, :posts, Blog.all_posts())}
  end
end
