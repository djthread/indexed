import Config

config :indexed,
  ecto_repos: [Indexed.Test.Repo],
  repo: Indexed.Test.Repo

config :indexed, Indexed.Test.Repo,
  database: "indexed_test",
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "test/support/priv"

logger_level =
  case String.upcase(System.get_env("LOG_LEVEL", "WARN")) do
    "ERROR" -> :error
    "INFO" -> :info
    "DEBUG" -> :debug
    _ -> :warn
  end

config :logger, level: logger_level
