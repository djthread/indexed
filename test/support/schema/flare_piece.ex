defmodule FlarePiece do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  schema "flare_pieces" do
    belongs_to :user, User
    field :name, :string
  end

  @type t :: %__MODULE__{}

  def changeset(struct_or_changeset, params) do
    struct_or_changeset
    |> cast(params, [:name, :user_id])
    |> validate_required([:name])
  end
end
