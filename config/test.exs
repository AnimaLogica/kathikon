import Config

config :kathikon,
  # Stay on RAM copies even if an RPC test calls Node.start/1. Auto mode
  # switches to disc once the node is no longer nonode@nohost, and that
  # rebuild breaks the running dispatchers.
  mnesia_copies: :ram,
  poll_interval: 50,
  scheduler_interval: 50,
  cron_tick: false,
  prune_interval: 60_000,
  retention_period: 1,
  max_attempts: 3,
  queues: [
    default: [concurrency: 10],
    integration: [concurrency: 10],
    priority: [concurrency: 1]
  ]
