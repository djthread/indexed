defmodule Indexed.Test.Repo do
  use Ecto.Repo, otp_app: :indexed, adapter: Ecto.Adapters.Postgres
end
