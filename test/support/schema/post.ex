defmodule Post do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "posts" do
    belongs_to :author, User
    belongs_to :first_commenter, User, define_field: false
    field :first_commenter_id, :integer, virtual: true
    has_many :comments, Comment, on_replace: :delete
    field :content, :string
    timestamps()
  end

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:author_id, :content])
    |> validate_required([:author_id, :content])
    |> cast_assoc(:comments)
  end
end
