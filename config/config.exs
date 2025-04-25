import Config

# Override for improved log output.
config :logger,
  backends: [:console],
  truncate: :infinity,
  compile_time_purge_matching: [
    [level_lower_than: :info]
  ],
  handle_otp_reports: true,
  handle_sasl_reports: true
