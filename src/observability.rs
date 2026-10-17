use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use std::env;
use std::io;

#[derive(Clone, Debug)]
pub struct NodeMetrics {
    registry: Registry,
    term: IntGauge,
    state: IntGaugeVec,
    commit_index: IntGauge,
    last_applied: IntGauge,
    elections_total: IntCounter,
    leader_changes_total: IntCounter,
    writes_total: IntCounter,
    write_latency: Histogram,
    replication_lag: IntGaugeVec,
}

impl NodeMetrics {
    pub fn new(node_id: usize) -> io::Result<Self> {
        let registry = Registry::new_custom(None, Some(HashMapLabels::node(node_id)))
            .map_err(io::Error::other)?;

        let term = int_gauge("raft_term", "Current Raft term.")?;
        let state = IntGaugeVec::new(
            Opts::new("raft_state", "Current Raft role as one-hot gauges."),
            &["role"],
        )
        .map_err(io::Error::other)?;
        let commit_index = int_gauge("raft_commit_index", "Highest committed Raft log index.")?;
        let last_applied = int_gauge("raft_last_applied", "Highest applied Raft log index.")?;
        let elections_total = int_counter(
            "raft_elections_total",
            "Total elections started by this node.",
        )?;
        let leader_changes_total = int_counter(
            "raft_leader_changes_total",
            "Total observed leader changes.",
        )?;
        let writes_total = int_counter(
            "raft_writes_total",
            "Client writes whose handlers observed leader-side commit and apply.",
        )?;
        let write_latency = Histogram::with_opts(HistogramOpts::new(
            "raft_write_latency_seconds",
            "Latency for a client write to commit and apply on the leader.",
        ))
        .map_err(io::Error::other)?;
        let replication_lag = IntGaugeVec::new(
            Opts::new(
                "raft_replication_lag",
                "Leader-side commit_index minus follower match_index.",
            ),
            &["follower"],
        )
        .map_err(io::Error::other)?;
        for collector in [
            Box::new(term.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(state.clone()),
            Box::new(commit_index.clone()),
            Box::new(last_applied.clone()),
            Box::new(elections_total.clone()),
            Box::new(leader_changes_total.clone()),
            Box::new(writes_total.clone()),
            Box::new(write_latency.clone()),
            Box::new(replication_lag.clone()),
        ] {
            registry.register(collector).map_err(io::Error::other)?;
        }

        Ok(Self {
            registry,
            term,
            state,
            commit_index,
            last_applied,
            elections_total,
            leader_changes_total,
            writes_total,
            write_latency,
            replication_lag,
        })
    }

    pub fn set_raft_state(&self, term: u64, role: &str, commit_index: usize, last_applied: usize) {
        self.term.set(saturating_i64(term));
        for label in ["follower", "candidate", "leader"] {
            self.state
                .with_label_values(&[label])
                .set(i64::from(label == role));
        }
        self.commit_index.set(saturating_i64(commit_index));
        self.last_applied.set(saturating_i64(last_applied));
    }

    pub fn set_replication_lag(&self, follower: usize, lag: usize) {
        self.replication_lag
            .with_label_values(&[&follower.to_string()])
            .set(saturating_i64(lag));
    }

    pub fn inc_elections(&self) {
        self.elections_total.inc();
    }

    pub fn inc_leader_changes(&self) {
        self.leader_changes_total.inc();
    }

    pub fn observe_write(&self, seconds: f64) {
        self.writes_total.inc();
        self.write_latency.observe(seconds);
    }

    pub fn render(&self) -> io::Result<String> {
        let mut bytes = Vec::new();
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut bytes)
            .map_err(io::Error::other)?;
        String::from_utf8(bytes).map_err(io::Error::other)
    }
}

pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if env::var("RAFT_KV_LOG").is_ok_and(|value| value == "json") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .compact()
            .init();
    }
}

fn int_gauge(name: &str, help: &str) -> io::Result<IntGauge> {
    IntGauge::with_opts(Opts::new(name, help)).map_err(io::Error::other)
}

fn int_counter(name: &str, help: &str) -> io::Result<IntCounter> {
    IntCounter::with_opts(Opts::new(name, help)).map_err(io::Error::other)
}

fn saturating_i64(value: impl TryInto<i64>) -> i64 {
    value.try_into().unwrap_or(i64::MAX)
}

struct HashMapLabels;

impl HashMapLabels {
    fn node(node_id: usize) -> std::collections::HashMap<String, String> {
        [("node".to_string(), node_id.to_string())].into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_required_metrics() {
        let metrics = NodeMetrics::new(0).unwrap();
        metrics.set_raft_state(2, "leader", 4, 3);
        metrics.observe_write(0.01);
        let body = metrics.render().unwrap();

        for name in [
            "raft_term",
            "raft_state",
            "raft_commit_index",
            "raft_last_applied",
            "raft_elections_total",
            "raft_leader_changes_total",
            "raft_writes_total",
            "raft_write_latency_seconds",
        ] {
            assert!(body.contains(name), "missing metric {name}\n{body}");
        }
    }
}
