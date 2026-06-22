# Documentation

This directory keeps the public engineering notes that are still useful for
understanding Pulsar Lite. Historical planning notes and stale implementation
walkthroughs are intentionally kept out of the main documentation set.

## Project Documents

- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)
- [Pulsar binary protocol](PULSAR_BINARY_PROTOCOL.md)

## Design Notes

- [Storage layer design](design/storage/storage.md)

## Apache Pulsar Comparison Notes

- [Exclusive subscription comparison](difference/exclusive_subscription_comparison.md)
- [Failover subscription comparison](difference/failover_subscription_comparison.md)
- [Shared subscription comparison](difference/shared_subscription_comparison.md)
- [Storage metadata comparison](difference/storage_metadata_comparison.md)

## Test and Performance Notes

- [Non-persistent test coverage](tests/non_persistent_test_coverage.md)
- [Non-persistent dispatcher optimization](perf/non_persistent_dispatcher_optimization.md)
- [Non-persistent topic fanout optimization](perf/non_persistent_topic_fanout_optimization.md)
- [Non-persistent copy path optimization](perf/non_persistent_copy_path_optimization.md)
- [Non-persistent E2E Pulsar perf matrix](perf/non_persistent_e2e_pulsar_perf_matrix.md)
- [Query and stats path optimization](perf/query_stats_path_optimization.md)
