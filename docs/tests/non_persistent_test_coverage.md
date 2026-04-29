# Non-Persistent Test Coverage

本文档记录当前 [tests/non_persist](/home/xtline/code/work/pulsar-lite/tests/non_persist) 已覆盖的功能性场景，便于后续补齐缺口和回归检查。

## 范围说明

- 测试目标：`non-persistent://public/default/...` 运行时语义
- 主要入口：官方 Python Pulsar client
- 当前覆盖重点：基础收发、订阅模式、动态 consumer、顺序、流控、断连重连、隔离性、已知未实现语义
- 当前 `tests/non_persist` 全集验证结果：`38 passed, 1 skipped`

## 基础实时语义

- 先发送后订阅，晚到 consumer 看不到 backlog
  - `test_non_persist_late_subscriber_sees_no_backlog`
  - 文件：[test_non_persist_basic.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_basic.py)
- `send_async` 成功回调数量与最终投递数量一致
  - `test_non_persist_send_async_delivers_and_preserves_message_metadata`
  - 文件：[test_non_persist_basic.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_basic.py)
- 消息属性保留：
  - `properties`
  - `partition_key`
  - `ordering_key`
  - 文件：[test_non_persist_basic.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_basic.py)

## 订阅模式基础语义

- `Exclusive` 已有 consumer 时拒绝第二个 consumer
  - `test_non_persist_exclusive_rejects_second_consumer`
- `Failover` standby 不抢 active 消息；active 关闭后 standby 接管
  - `test_non_persist_failover_promotes_standby_after_active_closes`
- `Shared` 消息会在在线 consumers 间分发
  - `test_non_persist_shared_distributes_messages_across_consumers`
- `KeyShared` 同 key 路由到同一个 consumer
  - `test_non_persist_key_shared_routes_same_key_to_same_consumer`
- 文件：[test_non_persist_subscription_modes.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_subscription_modes.py)

## KeyShared 策略语义

- `Sticky` range 路由到预期 consumer
  - `test_non_persist_key_shared_sticky_ranges_route_to_expected_consumer`
- `AutoSplit` 与 `Sticky` policy 不兼容时拒绝加入
  - `test_non_persist_key_shared_rejects_incompatible_policy_consumer`
- 文件：[test_non_persist_key_shared.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_key_shared.py)

## 动态 consumer 语义

- `Shared` 新增 consumer 后只接后续实时消息，并开始参与分摊
  - `test_non_persist_shared_new_consumer_only_sees_live_traffic_and_shares_load`
- `Shared` 移除一个 consumer 后，剩余 consumer 持续消费
  - `test_non_persist_shared_survivor_continues_after_other_consumer_closes`
- `KeyShared Sticky` 新增 consumer 后，已有 key 归属稳定，新 key 路由到新范围
  - `test_non_persist_key_shared_new_consumer_keeps_existing_key_stable_and_routes_new_key`
- `KeyShared Sticky` 移除一个 consumer 后，幸存 consumer 保持自身 range，不错误接管已移除 range
  - `test_non_persist_key_shared_sticky_survivor_keeps_own_range_and_does_not_take_removed_range`
- `KeyShared AutoSplit` 新增 consumer 后，live keys 开始按新切分范围分摊
  - `test_non_persist_key_shared_auto_split_new_consumer_shares_live_keys`
- `KeyShared AutoSplit` 移除一个 consumer 后，剩余 consumer 继续消费
  - `test_non_persist_key_shared_auto_split_survivor_continues_after_other_consumer_closes`
- 文件：[test_non_persist_dynamic_consumers.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_dynamic_consumers.py)

## 顺序性

- `Exclusive` 单 consumer FIFO
  - `test_non_persist_exclusive_preserves_send_order`
- `Exclusive` consumer handoff 后，后续消息仍 FIFO
  - `test_non_persist_exclusive_handoff_preserves_order_after_rejoin`
- `Failover` 每个 active epoch 内保持 FIFO
  - `test_non_persist_failover_preserves_order_within_active_epochs`
- `Shared` 单 consumer 场景下保持 FIFO
  - `test_non_persist_shared_single_consumer_preserves_send_order`
- `KeyShared Sticky` 同 key FIFO
  - `test_non_persist_key_shared_sticky_preserves_same_key_order`
- `KeyShared Sticky` 动态 join 后，同 key 后续消息仍 FIFO
  - `test_non_persist_key_shared_sticky_dynamic_join_preserves_same_key_order`
- `KeyShared AutoSplit` 同 key FIFO
  - `test_non_persist_key_shared_auto_split_preserves_same_key_order`
- `KeyShared AutoSplit` 动态 membership 变化后，同 key 仍 FIFO
  - `test_non_persist_key_shared_auto_split_dynamic_membership_preserves_same_key_order`
- 文件：[test_non_persist_ordering.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_ordering.py)

## Flow Control

- `Shared`：满 consumer 不继续收新消息
  - `test_non_persist_shared_flow_full_consumer_stops_receiving_new_messages`
- `Shared`：drain 本地缓存后恢复接收
  - `test_non_persist_shared_flow_consumer_resumes_after_buffer_is_drained`
- `Shared`：优先发给仍有接收容量的 consumer
  - `test_non_persist_shared_flow_prefers_consumers_with_available_receive_capacity`
- `Shared`：pre-FLOW drop 窗口测试已存在，但当前高层 Python client 下不可稳定复现，显式跳过
  - `test_non_persist_shared_flow_not_ready_drops_message_by_current_design`
- `Exclusive`：队列满时丢弃，drain 后恢复
  - `test_non_persist_exclusive_flow_drops_when_consumer_queue_is_full_and_recovers`
- `Failover`：active 满时不错误转发给 standby
  - `test_non_persist_failover_flow_does_not_reroute_to_standby_when_active_is_full`
- `KeyShared Sticky`：目标 consumer 满时，不错误把同 key 转发给别的 consumer
  - `test_non_persist_key_shared_flow_does_not_reroute_key_when_target_consumer_is_full`
- 文件：[test_non_persist_flow_control.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_flow_control.py)

## 断连 / 重连

- 外部 consumer 进程异常退出后，新 consumer 可以重新接管 subscription
  - `test_non_persist_shared_new_consumer_can_take_over_after_peer_process_is_killed`
- `Exclusive` 断开重连后，不会凭空出现 backlog，只能收到重新在线后的 live 消息
  - `test_non_persist_exclusive_reconnect_has_no_backlog_and_receives_new_live_messages`
- 文件：[test_non_persist_disconnect_reconnect.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_disconnect_reconnect.py)

## Ack / Redelivery 现有语义

- `Shared` 已 ack 消息在 owner close 后不重投
  - `test_non_persist_shared_acked_message_is_not_redelivered_after_owner_closes`
- `Shared` 未 ack 消息在 owner close 后，当前 non-persistent 语义下不会自动 redelivery 给 survivor
  - `test_non_persist_shared_disconnect_drops_unacked_message`
- 文件：[test_non_persist_ack_semantics.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_ack_semantics.py)

## 多 Producer 与隔离性

- 单 topic 多 producer 并发发送，最终消息全部可见
  - `test_non_persist_multi_producer_single_topic_delivers_all_messages`
- 同 topic 多 subscription 相互独立，同一条消息可被各自消费一次
  - `test_non_persist_same_topic_multiple_subscriptions_are_independent`
- 不同 topic 不串消息
  - `test_non_persist_different_topics_do_not_cross_deliver_messages`
- 同 topic 不同订阅模式互不影响
  - `test_non_persist_same_topic_different_subscription_modes_are_independent`
- 文件：[test_non_persist_isolation.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_isolation.py)

## 当前明确未生效 / 未完整支持的语义

- `negative_acknowledge()` 当前不会触发 non-persistent Shared redelivery
  - `test_non_persist_shared_negative_ack_does_not_trigger_redelivery`
- `unacked_messages_timeout_ms` 当前不会触发 non-persistent Shared redelivery
  - `test_non_persist_shared_ack_timeout_does_not_trigger_redelivery`
- `redeliver_unacknowledged_messages()` 当前不会触发 non-persistent Shared redelivery
  - `test_non_persist_shared_explicit_redelivery_command_does_not_redeliver`
- 文件：[test_non_persist_unsupported_semantics.py](/home/xtline/code/work/pulsar-lite/tests/non_persist/test_non_persist_unsupported_semantics.py)

## 当前已知边界

- 大多数 correctness 测试以 non-partitioned non-persistent topic 为主；`default_partitions > 0` 的场景在部分 ack/redelivery 测试里会跳过。
- `pre-FLOW` drop 窗口在高层 Python client 下不可稳定观测，因此当前保留显式 `skip`，而不是脆弱的时序断言。
- 当前文档仅记录 `tests/non_persist` 已覆盖场景，不包含 `tests/perf/*`、persistent 路径测试和 broker 内部 Rust 单元测试。
