defmodule Indexed.Test.Repo.Migrations.CreateCar do
  use Ecto.Migration

  def change do
    create table(:albums) do
      add(:artist, :string)
      add(:label, :string)
      add(:media, :string)
    end

    create table(:users) do
      add(:best_friend_id, references(:users))
      add(:name, :string)
      timestamps()
    end

    create table(:flare_pieces) do
      add(:name, :string)
      add(:user_id, references(:users))
    end

    create table(:posts) do
      add(:author_id, references(:users))
      add(:content, :string)
      timestamps()
    end

    create table(:comments) do
      add(:author_id, references(:users))
      add(:content, :string)
      add(:post_id, references(:posts))
      timestamps()
    end

    create table(:replies) do
      add(:comment_id, references(:comments))
      add(:content, :string)
      add(:this_blog, :boolean)
    end
  end
end
