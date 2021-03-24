use Mix.Config

logger_level =
  case String.upcase(System.get_env("LOG_LEVEL", "WARN")) do
    "ERROR" -> :error
    "INFO" -> :info
    "DEBUG" -> :debug
    _ -> :warn
  end

config :logger, level: logger_level
