defmodule Reply do
  @moduledoc "A reply to a comment."
  use Ecto.Schema
  import Ecto.Changeset

  schema "replies" do
    belongs_to :comment, Comment
    field :content, :string
    field :this_blog, :boolean
  end

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:comment_id, :content])
    |> validate_required([:comment_id, :content])
  end
end
