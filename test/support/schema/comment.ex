defmodule Comment do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "comments" do
    belongs_to :author, User
    field :content, :string
    belongs_to :post, Post
    has_many :replies, Reply
    timestamps()
  end

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:author_id, :content, :post_id])
    |> validate_required([:author_id, :content, :post_id])
  end
end
