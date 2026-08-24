/*
 * Broker Stats Module
 * Scrape-time aggregation plus re-exports of the metric handles defined in
 * the `pulsar-lite-metrics` crate.
 */

pub mod scrape;

pub use pulsar_lite_metrics::{
    get, init, parse_topic_labels, BrokerMetrics, SubscriptionMetrics, TopicLabels, TopicMetrics,
};
