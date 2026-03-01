rule_v1 = [ # SHAP knobs
    ('max_parallel_workers_per_gather', '<=', 'max_worker_processes'),
    ('max_parallel_workers_per_gather', '<', 'max_parallel_workers'),
    ('max_parallel_maintenance_workers', '<', 'max_parallel_workers'),
    ('max_parallel_workers', '<=', 'max_worker_processes'),
    ('max_logical_replication_workers', '<', 'max_worker_processes'),
    ('shared_buffers', '>', 'max_wal_size'),
    ('join_collapse_limit', '>=', 'geqo_threshold'),
    ('from_collapse_limit', '>=', 'geqo_threshold')
]

rule_v2 = [ # GPT recommended top 10 knobs
    ('max_prepared_transactions', '>=', 'max_connections') ,
    ('superuser_reserved_connections', '<', 'max_connections'),
    ('effective_cache_size', '>=', 'shared_buffers'),
    ('wal_buffers', '<=', 'max_wal_size'),
    ('autovacuum_work_mem', '<=', 'shared_buffers')
]

rule_v3 = [ # knobs invovling all manually extracted rules + SHAP knobs
    ( "max_wal_senders", "<", "max_connections" ),
    ( "superuser_reserved_connections", "<", "max_connections" ),
    ( "shared_buffers", "<", "max_wal_size" ),
    ( "max_prepared_transactions", ">=", "max_connections" ),

    ( "max_parallel_workers_per_gather", "<=", "max_parallel_workers" ),
    ( "max_parallel_workers_per_gather", "<", "max_worker_processes" ),
    ( "max_parallel_maintenance_workers", "<", "max_parallel_workers" ),
    ( "max_parallel_workers", "<=", "max_worker_processes" ),
    ( "max_logical_replication_workers", "<", "max_worker_processes" ),
    ( "max_sync_workers_per_subscription", "<=", "max_logical_replication_workers" ),

    # ( "from_collapse_limit", ">=", "geqo_threshold" ),
    # ( "join_collapse_limit", ">=", "geqo_threshold" ),

    ( "vacuum_freeze_table_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_freeze_min_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_multixact_freeze_table_age", "<", "autovacuum_multixact_freeze_max_age" ),
    ( "vacuum_multixact_freeze_min_age", "<", "autovacuum_multixact_freeze_max_age" )
]

rule_v4 = [ # knobs invovling all manually extracted rules + SHAP knobs
    ( "max_wal_senders", "<", "max_connections" ),
    ( "superuser_reserved_connections", "<", "max_connections" ),
    ( "shared_buffers", "<=", "max_wal_size" ),
    ( "max_prepared_transactions", ">=", "max_connections" ),
    ("min_wal_size", "<=", "max_wal_size"), # implicit rule

    ( "max_parallel_workers_per_gather", "<=", "max_parallel_workers" ),
    ( "max_parallel_workers_per_gather", "<", "max_worker_processes" ),
    ( "max_parallel_maintenance_workers", "<", "max_parallel_workers" ),
    ( "max_parallel_workers", "<=", "max_worker_processes" ),
    ( "max_logical_replication_workers", "<", "max_worker_processes" ),
    ( "max_sync_workers_per_subscription", "<=", "max_logical_replication_workers" ),

    # ( "from_collapse_limit", ">=", "geqo_threshold" ),
    # ( "join_collapse_limit", ">=", "geqo_threshold" ),

    ( "vacuum_freeze_table_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_freeze_min_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_multixact_freeze_table_age", "<", "autovacuum_multixact_freeze_max_age" ),
    ( "vacuum_multixact_freeze_min_age", "<", "autovacuum_multixact_freeze_max_age" )
]

rule_v5 = [ # remove some constraints that are not very necessary
    # ( "max_wal_senders", "<", "max_connections" ),
    ( "superuser_reserved_connections", "<", "max_connections" ),
    # ( "shared_buffers", "<=", "max_wal_size" ),
    ( "max_prepared_transactions", ">=", "max_connections" ),
    ("min_wal_size", "<=", "max_wal_size"), # implicit rule

    ( "max_parallel_workers_per_gather", "<=", "max_parallel_workers" ),
    # ( "max_parallel_workers_per_gather", "<", "max_worker_processes" ),
    ( "max_parallel_maintenance_workers", "<", "max_parallel_workers" ),
    ( "max_parallel_workers", "<=", "max_worker_processes" ),
    # ( "max_logical_replication_workers", "<", "max_worker_processes" ),
    # ( "max_sync_workers_per_subscription", "<=", "max_logical_replication_workers" ),

    # ( "from_collapse_limit", ">=", "geqo_threshold" ),
    # ( "join_collapse_limit", ">=", "geqo_threshold" ),

    ( "vacuum_freeze_table_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_freeze_min_age", "<", "autovacuum_freeze_max_age" ),
    ( "vacuum_multixact_freeze_table_age", "<", "autovacuum_multixact_freeze_max_age" ),
    ( "vacuum_multixact_freeze_min_age", "<", "autovacuum_multixact_freeze_max_age" )
]


# (child, parent, active_values_for_parent)
conditional_activations = [
    ("max_prepared_transactions", "max_prepared_transactions_mode", {"enabled"}),
]

[
  ('max_wal_senders', 'control_max_wal_senders', {'0'}), 
  ('special_max_wal_senders', 'control_max_wal_senders', {'1'}), 
  ('max_prepared_transactions', 'control_max_prepared_transactions', {'0'}), 
  ('special_max_prepared_transactions', 'control_max_prepared_transactions', {'1'}), 
  ('max_sync_workers_per_subscription', 'control_max_sync_workers_per_subscription', {'0'}), 
  ('special_max_sync_workers_per_subscription', 'control_max_sync_workers_per_subscription', {'1'}), 
  ('wal_buffers', 'control_wal_buffers', {'0'}), 
  ('special_wal_buffers', 'control_wal_buffers', {'1'}), 
  ('backend_flush_after', 'control_backend_flush_after', {'0'}), 
  ('special_backend_flush_after', 'control_backend_flush_after', {'1'}), 
  ('max_parallel_workers_per_gather', 'control_max_parallel_workers_per_gather', {'0'}), 
  ('special_max_parallel_workers_per_gather', 'control_max_parallel_workers_per_gather', {'1'}), 
  ('wal_writer_flush_after', 'control_wal_writer_flush_after', {'0'}), 
  ('special_wal_writer_flush_after', 'control_wal_writer_flush_after', {'1'}), 
  ('checkpoint_flush_after', 'control_checkpoint_flush_after', {'0'}), 
  ('special_checkpoint_flush_after', 'control_checkpoint_flush_after', {'1'}), 
  ('commit_delay', 'control_commit_delay', {'0'}), 
  ('special_commit_delay', 'control_commit_delay', {'1'}), 
  ('bgwriter_flush_after', 'control_bgwriter_flush_after', {'0'}), 
  ('special_bgwriter_flush_after', 'control_bgwriter_flush_after', {'1'}), 
  ('bgwriter_lru_maxpages', 'control_bgwriter_lru_maxpages', {'0'}), 
  ('special_bgwriter_lru_maxpages', 'control_bgwriter_lru_maxpages', {'1'}), 
  ('effective_io_concurrency', 'control_effective_io_concurrency', {'0'}), 
  ('special_effective_io_concurrency', 'control_effective_io_concurrency', {'1'})
]
