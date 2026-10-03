use raft_kv::{ClientRequest, Cluster, Command, NodeId, Role};
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io;
use std::path::Path;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("raft-demo: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    let docs = Path::new("docs");
    fs::create_dir_all(docs)?;

    let election = election_trace();
    let failover_svg = legacy_failover_trace();
    let replication = replication_table();
    let metrics = metrics_table();

    fs::write(docs.join("election.svg"), render_svg("Election", &election))?;
    fs::write(
        docs.join("failover.svg"),
        render_svg("Failover", &failover_svg),
    )?;
    fs::write(
        docs.join("raft-explorer.html"),
        render_explorer(&[
            election_scenario(),
            write_scenario(),
            failover_scenario(),
            partition_scenario(),
        ]),
    )?;
    fs::write(docs.join("cluster-dashboard.svg"), render_dashboard_svg())?;
    fs::write(docs.join("failover-story.svg"), render_failover_story_svg())?;
    fs::write(docs.join("log-ledger.svg"), render_log_ledger_svg())?;
    fs::write(docs.join("lsm-storage.svg"), render_lsm_storage_svg())?;
    fs::write(
        docs.join("observability-loop.svg"),
        render_observability_svg(),
    )?;
    fs::write(docs.join("replication.md"), &replication)?;
    fs::write(docs.join("metrics.md"), &metrics)?;
    update_readme("GUIDE.md", &replication, &metrics)?;
    Ok(())
}

fn election_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    sample_until(&mut cluster, 600, |cluster| cluster.leader().is_some())
}

fn legacy_failover_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    cluster.run_for(200);
    let old_leader = cluster.leader().expect("leader");
    let mut samples = vec![plain_sample(
        &cluster,
        0,
        Some(format!("kill node {old_leader}")),
    )];
    cluster.stop(old_leader);
    for offset in (50..=600).step_by(50) {
        cluster.run_for(50);
        samples.push(plain_sample(&cluster, offset, None));
    }
    samples
}

fn sample_until(
    cluster: &mut Cluster,
    deadline_ms: u64,
    done: impl Fn(&Cluster) -> bool,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    for time in (0..=deadline_ms).step_by(50) {
        samples.push(plain_sample(cluster, time, None));
        if done(cluster) {
            break;
        }
        cluster.run_for(50);
    }
    samples
}

fn plain_sample(cluster: &Cluster, time_ms: u64, note: Option<String>) -> Sample {
    Sample {
        time_ms,
        note,
        nodes: capture_nodes(cluster),
        client: None,
    }
}

fn capture_nodes(cluster: &Cluster) -> Vec<NodeSample> {
    let mut nodes: Vec<_> = cluster
        .nodes()
        .map(|(id, node)| NodeSample {
            id,
            role: node.role(),
            term: node.current_term(),
            voted_for: node.voted_for(),
            log: node
                .log()
                .iter()
                .map(|entry| command_label(&entry.command))
                .collect(),
            commit: node.commit_index() as u64,
            applied: node.last_applied() as u64,
            stopped: cluster.is_stopped(id),
        })
        .collect();
    nodes.sort_by_key(|node| node.id);
    nodes
}

fn replication_table() -> String {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader");
    let reply = cluster.propose(
        leader,
        ClientRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    );
    assert!(reply.success);
    assert!(cluster.run_until(1200, |cluster| {
        cluster
            .nodes()
            .all(|(_, node)| node.get("foo") == Some("bar".to_string()))
    }));

    let mut out = String::from(
        "| node | role | term | commit | applied | log | kv |\n|---:|---|---:|---:|---:|---|---|\n",
    );
    let mut ids: Vec<_> = cluster.node_ids().collect();
    ids.sort_unstable();
    for id in ids {
        let node = cluster.node(id);
        let log = node
            .log()
            .iter()
            .map(|entry| command_label(&entry.command))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!(
            "| {id} | {:?} | {} | {} | {} | [{}] | foo={} |\n",
            node.role(),
            node.current_term(),
            node.commit_index(),
            node.last_applied(),
            log,
            node.get("foo").unwrap_or("∅".to_string())
        ));
    }
    out
}

fn metrics_table() -> String {
    let mut election = Cluster::new(5);
    let election_ms = first_time_until(&mut election, 1000, |cluster| cluster.leader().is_some());

    let mut failover = Cluster::new(5);
    assert!(failover.run_until(600, |cluster| cluster.leader().is_some()));
    failover.run_for(200);
    let old_leader = failover.leader().expect("leader");
    failover.stop(old_leader);
    let failover_ms = first_time_until(&mut failover, 1000, |cluster| {
        cluster.leader().is_some_and(|leader| leader != old_leader)
    });

    let mut replication = Cluster::new(5);
    assert!(replication.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = replication.leader().expect("leader");
    let replication_started = replication.now();
    let _ = replication.propose(
        leader,
        ClientRequest::Set {
            key: "foo".to_string(),
            value: "bar".to_string(),
        },
    );
    let _ = first_time_until(&mut replication, 1000, |cluster| {
        cluster
            .nodes()
            .all(|(_, node)| node.get("foo") == Some("bar".to_string()))
    });
    let replication_ms = replication.now().saturating_sub(replication_started);

    format!(
        "| metric | value |\n|---|---:|\n| cluster size tested | 5 nodes |\n| election timeout | 150–300 ms |\n| heartbeat interval | 50 ms |\n| first leader elected | {election_ms} ms simulated |\n| failover after leader kill | {failover_ms} ms simulated |\n| write visible on all nodes | {replication_ms} ms simulated |\n| fault tolerance | 2 failed nodes in a 5-node cluster |\n| process-level TCP tests | 3 integration tests |\n"
    )
}

fn first_time_until(
    cluster: &mut Cluster,
    deadline_ms: u64,
    done: impl Fn(&Cluster) -> bool,
) -> u64 {
    let started = cluster.now();
    while cluster.now().saturating_sub(started) <= deadline_ms {
        if done(cluster) {
            return cluster.now().saturating_sub(started);
        }
        cluster.run_for(1);
    }
    cluster.now().saturating_sub(started)
}

fn command_label(command: &Command) -> String {
    match command {
        Command::Noop => "noop".to_string(),
        Command::Set { key, value } => format!("set {key}={value}"),
        Command::Delete { key } => format!("delete {key}"),
    }
}

fn committed_cluster() -> Cluster {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    let leader = cluster.leader().expect("leader");
    assert!(
        cluster
            .propose(
                leader,
                ClientRequest::Set {
                    key: "foo".to_string(),
                    value: "bar".to_string()
                }
            )
            .success
    );
    assert!(
        cluster
            .propose(
                leader,
                ClientRequest::Set {
                    key: "baz".to_string(),
                    value: "qux".to_string()
                }
            )
            .success
    );
    assert!(cluster.run_until(1400, |cluster| {
        cluster
            .nodes()
            .all(|(_, node)| node.get("baz") == Some("qux".to_string()))
    }));
    cluster
}

fn render_dashboard_svg() -> String {
    let cluster = committed_cluster();
    let leader = cluster.leader().expect("leader");
    let term = cluster.node(leader).current_term();
    let commit = cluster.node(leader).commit_index();
    let mut svg = svg_shell(960, 430, "raft-kv · live cluster snapshot");
    svg.push_str(&format!(
        r##"<text x="36" y="74" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">term {term}</text>
<text x="160" y="74" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">leader node-{leader}</text>
<text x="360" y="74" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">commit index {commit}</text>
"##
    ));
    let mut ids: Vec<_> = cluster.node_ids().collect();
    ids.sort_unstable();
    for (row, id) in ids.iter().enumerate() {
        let node = cluster.node(*id);
        let y = 112 + row as i32 * 54;
        let (fill, label) = role_style(node.role());
        svg.push_str(&format!(
            r##"<rect x="36" y="{}" width="888" height="40" rx="10" fill="#171a21" stroke="#30363d"/>
<text x="58" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">node-{id}</text>
<rect x="150" y="{}" width="92" height="24" rx="12" fill="{fill}"/>
<text x="166" y="{}" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12" font-weight="700">{label}</text>
<text x="278" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">log</text>
<text x="318" y="{}" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16">██████████</text>
<text x="520" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">commit {}</text>
<text x="650" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">foo={}</text>
<text x="770" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">baz={}</text>
"##,
            y, y + 25, y + 8, y + 24, y + 25, y + 25, y + 25, node.commit_index(), y + 25, node.get("foo").unwrap_or("∅".to_string()), y + 25, node.get("baz").unwrap_or("∅".to_string())
        ));
    }
    finish_svg(svg)
}

fn render_failover_story_svg() -> String {
    let mut svg = svg_shell(960, 360, "leader failure · election · recovery");
    let panels = [
        (
            "1",
            "steady state",
            "node-4 leads",
            "logs are aligned",
            "#3fb950",
        ),
        (
            "2",
            "leader crashes",
            "node-4 stops",
            "heartbeats expire",
            "#f85149",
        ),
        (
            "3",
            "new election",
            "node-3 asks",
            "majority votes",
            "#d29922",
        ),
        (
            "4",
            "recovered",
            "node-3 leads",
            "writes continue",
            "#3fb950",
        ),
    ];
    for (index, (num, title, line1, line2, color)) in panels.iter().enumerate() {
        let x = 36 + index as i32 * 226;
        svg.push_str(&format!(
            r##"<rect x="{x}" y="86" width="198" height="210" rx="16" fill="#171a21" stroke="#30363d"/>
<circle cx="{}" cy="122" r="18" fill="{color}"/>
<text x="{}" y="128" text-anchor="middle" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14" font-weight="700">{num}</text>
<text x="{}" y="168" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16" font-weight="700">{title}</text>
<text x="{}" y="206" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">{line1}</text>
<text x="{}" y="232" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">{line2}</text>
"##,
            x + 99, x + 99, x + 22, x + 22, x + 22
        ));
    }
    finish_svg(svg)
}

fn render_log_ledger_svg() -> String {
    let cluster = committed_cluster();
    let mut svg = svg_shell(960, 410, "replicated log ledger");
    let mut ids: Vec<_> = cluster.node_ids().collect();
    ids.sort_unstable();
    for (row, id) in ids.iter().enumerate() {
        let node = cluster.node(*id);
        let y = 88 + row as i32 * 58;
        let (fill, label) = role_style(node.role());
        svg.push_str(&format!(
            r##"<text x="38" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">node-{id}</text>
<rect x="120" y="{}" width="92" height="24" rx="12" fill="{fill}"/>
<text x="136" y="{}" fill="#0f1115" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12" font-weight="700">{label}</text>
"##,
            y + 22, y + 4, y + 20
        ));
        for (col, entry) in node.log().iter().enumerate() {
            let x = 248 + col as i32 * 150;
            svg.push_str(&format!(
                r##"<rect x="{x}" y="{y}" width="132" height="32" rx="8" fill="#21262d" stroke="#30363d"/>
<text x="{}" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">{}</text>
"##,
                x + 12,
                y + 21,
                escape(&command_label(&entry.command))
            ));
        }
        svg.push_str(&format!(
            r##"<text x="820" y="{}" fill="#3fb950" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14">commit ✓</text>
"##,
            y + 22
        ));
    }
    finish_svg(svg)
}

fn render_lsm_storage_svg() -> String {
    let mut svg = svg_shell(960, 430, "LSM storage path");
    svg.push_str(
        r##"<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#58a6ff"/></marker></defs>
"##,
    );
    let boxes = [
        ("Raft commit", "ordered command", 48, 98, "#21262d"),
        ("WAL fsync", "durable first", 250, 98, "#1f2a36"),
        ("memtable", "BTreeMap", 452, 98, "#173322"),
        ("SSTable", "sorted file", 654, 98, "#2d2438"),
        ("get key", "point read", 48, 264, "#21262d"),
        ("bloom filter", "skip misses", 452, 264, "#2d2a1f"),
        ("sparse index", "seek nearby", 654, 264, "#2d2a1f"),
    ];
    for (title, subtitle, x, y, fill) in boxes {
        svg.push_str(&format!(
            r##"<rect x="{x}" y="{y}" width="150" height="76" rx="14" fill="{fill}" stroke="#30363d"/>
<text x="{}" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="15" font-weight="700">{}</text>
<text x="{}" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">{}</text>
"##,
            x + 18,
            y + 32,
            escape(title),
            x + 18,
            y + 56,
            escape(subtitle)
        ));
    }
    for (x1, y1, x2, y2, label) in [
        (198, 136, 250, 136, "append"),
        (400, 136, 452, 136, "apply"),
        (602, 136, 654, 136, "flush"),
        (198, 302, 452, 302, "miss"),
        (602, 302, 654, 302, "maybe"),
        (727, 264, 727, 174, "read file"),
        (527, 264, 527, 174, ""),
    ] {
        draw_arrow(&mut svg, x1, y1, x2, y2, label);
    }
    svg.push_str(
        r##"<path d="M123 264 C123 220 500 220 527 174" fill="none" stroke="#3fb950" stroke-width="2" stroke-dasharray="6 6"/>
<path d="M527 264 C527 226 692 220 727 174" fill="none" stroke="#d29922" stroke-width="2" stroke-dasharray="6 6"/>
<text x="52" y="388" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">writes flow left to right · reads start at memtable, then bloom/index/SSTable</text>
"##,
    );
    finish_svg(svg)
}

fn render_observability_svg() -> String {
    let mut svg = svg_shell(960, 390, "live observability loop");
    svg.push_str(
        r##"<defs><marker id="obs-arrow" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8 Z" fill="#58a6ff"/></marker></defs>
"##,
    );
    for (node, y) in [
        ("raft-node 0", 100),
        ("raft-node 1", 170),
        ("raft-node 2", 240),
    ] {
        svg.push_str(&format!(
            r##"<rect x="48" y="{y}" width="150" height="52" rx="13" fill="#21262d" stroke="#30363d"/>
<text x="66" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14" font-weight="700">{}</text>
<text x="66" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">/metrics + logs</text>
"##,
            y + 22,
            escape(node),
            y + 40
        ));
    }
    let boxes = [
        ("Prometheus", "scrapes every 2s", 336, 154, "#1f2a36"),
        ("Grafana", "preloaded dashboard", 572, 154, "#2d2438"),
    ];
    for (title, subtitle, x, y, fill) in boxes {
        svg.push_str(&format!(
            r##"<rect x="{x}" y="{y}" width="170" height="82" rx="16" fill="{fill}" stroke="#30363d"/>
<text x="{}" y="{}" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="16" font-weight="700">{}</text>
<text x="{}" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">{}</text>
"##,
            x + 20,
            y + 34,
            escape(title),
            x + 20,
            y + 58,
            escape(subtitle)
        ));
    }
    svg.push_str(
        r##"<rect x="780" y="96" width="130" height="198" rx="16" fill="#11161d" stroke="#30363d"/>
<text x="802" y="126" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="14" font-weight="700">panels</text>
<text x="802" y="154" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">role map</text>
<text x="802" y="180" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">term</text>
<text x="802" y="206" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">commit index</text>
<text x="802" y="232" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">replication lag</text>
<text x="802" y="258" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">compactions</text>
"##,
    );
    for (x1, y1, x2, y2, label) in [
        (198, 126, 336, 178, "/metrics"),
        (198, 196, 336, 196, "/metrics"),
        (198, 266, 336, 214, "/metrics"),
        (506, 196, 572, 196, "PromQL"),
        (742, 196, 780, 196, ""),
    ] {
        draw_obs_arrow(&mut svg, x1, y1, x2, y2, label);
    }
    svg.push_str(
        r##"<path d="M198 306 C320 342 555 342 702 248" fill="none" stroke="#8b949e" stroke-width="2" stroke-dasharray="6 6"/>
<text x="52" y="342" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">logs stay local by default · set RAFT_KV_LOG=json for structured JSON</text>
"##,
    );
    finish_svg(svg)
}

fn draw_obs_arrow(svg: &mut String, x1: i32, y1: i32, x2: i32, y2: i32, label: &str) {
    svg.push_str(&format!(
        r##"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#58a6ff" stroke-width="2" marker-end="url(#obs-arrow)"/>
"##
    ));
    if !label.is_empty() {
        svg.push_str(&format!(
            r##"<text x="{}" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">{}</text>
"##,
            (x1 + x2) / 2 - 22,
            (y1 + y2) / 2 - 8,
            escape(label)
        ));
    }
}

fn draw_arrow(svg: &mut String, x1: i32, y1: i32, x2: i32, y2: i32, label: &str) {
    svg.push_str(&format!(
        r##"<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#58a6ff" stroke-width="2" marker-end="url(#arrow)"/>
"##
    ));
    if !label.is_empty() {
        svg.push_str(&format!(
            r##"<text x="{}" y="{}" fill="#8b949e" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">{}</text>
"##,
            (x1 + x2) / 2 - 18,
            (y1 + y2) / 2 - 8,
            escape(label)
        ));
    }
}

fn svg_shell(width: i32, height: i32, title: &str) -> String {
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" rx="18" fill="#0f1115"/>
<rect x="18" y="18" width="{}" height="{}" rx="14" fill="#11161d" stroke="#30363d"/>
<text x="36" y="48" fill="#e6e1d9" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="20" font-weight="700">{}</text>
"##,
        width - 36,
        height - 36,
        escape(title)
    )
}

fn finish_svg(mut svg: String) -> String {
    svg.push_str("</svg>\n");
    svg
}

fn role_style(role: Role) -> (&'static str, &'static str) {
    match role {
        Role::Follower => ("#8b949e", "FOLLOWER"),
        Role::Candidate => ("#d29922", "CANDIDATE"),
        Role::Leader => ("#3fb950", "LEADER"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientStatus {
    Waiting,
    Committed,
    Refused,
}

impl ClientStatus {
    fn label(self) -> &'static str {
        match self {
            ClientStatus::Waiting => "waiting",
            ClientStatus::Committed => "committed",
            ClientStatus::Refused => "refused",
        }
    }
}

#[derive(Clone, Debug)]
struct ClientView {
    label: String,
    status: ClientStatus,
    elapsed_ms: u64,
    detail: Option<String>,
}

#[derive(Clone, Debug)]
struct ClientOp {
    label: String,
    status: ClientStatus,
    sent_at: u64,
    done_ms: Option<u64>,
    detail: Option<String>,
}

impl ClientOp {
    fn view(&self, now_ms: u64) -> ClientView {
        ClientView {
            label: self.label.clone(),
            status: self.status,
            elapsed_ms: self
                .done_ms
                .unwrap_or_else(|| now_ms.saturating_sub(self.sent_at)),
            detail: self.detail.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct Sample {
    time_ms: u64,
    note: Option<String>,
    nodes: Vec<NodeSample>,
    client: Option<ClientView>,
}

#[derive(Clone, Debug)]
struct NodeSample {
    id: NodeId,
    role: Role,
    term: u64,
    voted_for: Option<NodeId>,
    log: Vec<String>,
    commit: u64,
    applied: u64,
    stopped: bool,
}

struct Scenario {
    name: &'static str,
    description: &'static str,
    samples: Vec<Sample>,
}

/// Records one sample per visible state change. Times are simulated
/// milliseconds relative to the start of the scenario, so every trace begins
/// at 0 ms however long the cluster took to settle first.
struct Trace {
    start_ms: u64,
    samples: Vec<Sample>,
    client: Option<ClientOp>,
}

impl Trace {
    fn new(cluster: &Cluster) -> Self {
        Self {
            start_ms: cluster.now(),
            samples: Vec::new(),
            client: None,
        }
    }

    fn sample(&self, cluster: &Cluster, note: Option<String>) -> Sample {
        Sample {
            time_ms: cluster.now().saturating_sub(self.start_ms),
            note,
            nodes: capture_nodes(cluster),
            client: self.client.as_ref().map(|op| op.view(cluster.now())),
        }
    }

    fn observe(&mut self, cluster: &Cluster) {
        let next = self.sample(cluster, None);
        if self
            .samples
            .last()
            .is_none_or(|last| visible_change(last, &next))
        {
            self.samples.push(next);
        }
    }

    fn start(&mut self, cluster: &Cluster, note: impl Into<String>) {
        let next = self.sample(cluster, Some(note.into()));
        self.samples.push(next);
    }

    fn note(&mut self, cluster: &Cluster, note: impl Into<String>) {
        let next = self.sample(cluster, Some(note.into()));
        self.samples.push(next);
    }

    fn step(&mut self, cluster: &mut Cluster) {
        cluster.step();
        self.observe(cluster);
    }

    fn run_for(&mut self, cluster: &mut Cluster, duration_ms: u64) {
        let deadline = cluster.now() + duration_ms;
        while cluster.now() < deadline {
            self.step(cluster);
        }
    }

    fn run_until(
        &mut self,
        cluster: &mut Cluster,
        deadline_ms: u64,
        mut done: impl FnMut(&Cluster) -> bool,
    ) -> bool {
        while cluster.now() <= deadline_ms {
            if done(cluster) {
                return true;
            }
            self.step(cluster);
        }
        done(cluster)
    }

    fn send_client(&mut self, cluster: &Cluster, label: &str, note: impl Into<String>) {
        self.client = Some(ClientOp {
            label: label.to_string(),
            status: ClientStatus::Waiting,
            sent_at: cluster.now(),
            done_ms: None,
            detail: None,
        });
        self.note(cluster, note);
    }

    fn finish_client(
        &mut self,
        cluster: &Cluster,
        status: ClientStatus,
        detail: Option<String>,
        note: impl Into<String>,
    ) {
        if let Some(op) = self.client.as_mut() {
            op.status = status;
            op.done_ms = Some(cluster.now().saturating_sub(op.sent_at));
            op.detail = detail;
        }
        self.note(cluster, note);
    }
}

fn visible_change(previous: &Sample, next: &Sample) -> bool {
    if previous.nodes.len() != next.nodes.len() {
        return true;
    }
    let nodes_changed = previous.nodes.iter().zip(&next.nodes).any(|(old, new)| {
        old.role != new.role
            || old.term != new.term
            || old.voted_for != new.voted_for
            || old.log != new.log
            || old.commit != new.commit
            || old.applied != new.applied
            || old.stopped != new.stopped
    });
    if nodes_changed {
        return true;
    }
    match (previous.client.as_ref(), next.client.as_ref()) {
        (None, None) => false,
        (Some(old), Some(new)) => {
            old.label != new.label || old.status != new.status || old.detail != new.detail
        }
        _ => true,
    }
}

fn set(key: &str, value: &str) -> ClientRequest {
    ClientRequest::Set {
        key: key.to_string(),
        value: value.to_string(),
    }
}

fn settle(cluster: &mut Cluster) -> bool {
    cluster.run_until(1_200, |cluster| {
        cluster.leader().is_some() && cluster.nodes().all(|(_, node)| node.last_applied() >= 1)
    })
}

fn committed_write(
    trace: &mut Trace,
    cluster: &mut Cluster,
    leader: NodeId,
    request: ClientRequest,
    label: &str,
) -> bool {
    trace.send_client(
        cluster,
        label,
        format!("client sends {label} to node-{leader}"),
    );
    let Ok(write) = cluster.begin_write(leader, request) else {
        trace.finish_client(
            cluster,
            ClientStatus::Refused,
            Some("the leader refused it".to_string()),
            format!("{label} was refused"),
        );
        return false;
    };
    let index = write.index;
    let deadline = cluster.now() + 1_000;
    let committed = trace.run_until(cluster, deadline, |cluster| {
        cluster.node(leader).write_committed_and_applied(write)
    });
    if !committed {
        trace.finish_client(
            cluster,
            ClientStatus::Refused,
            Some("no majority committed it".to_string()),
            format!("{label} did not commit inside the wait window"),
        );
        return false;
    }
    trace.finish_client(
        cluster,
        ClientStatus::Committed,
        None,
        format!("a majority stored {label}; node-{leader} applied it at index {index} and answered the client"),
    );
    let deadline = cluster.now() + 1_000;
    let applied_everywhere = trace.run_until(cluster, deadline, |cluster| {
        cluster
            .nodes()
            .all(|(_, node)| node.last_applied() >= index)
    });
    if applied_everywhere {
        trace.note(
            cluster,
            format!("all five nodes have applied {label} to their own state machines"),
        );
    }
    true
}

fn election_scenario() -> Scenario {
    let mut cluster = Cluster::new(5);
    let mut trace = Trace::new(&cluster);
    trace.start(
        &cluster,
        "five followers, no leader; every election timer is running",
    );
    let elected = trace.run_until(&mut cluster, 600, |cluster| cluster.leader().is_some());
    if elected {
        let leader = cluster.leader().expect("leader after the election");
        let term = cluster.node(leader).current_term();
        trace.note(
            &cluster,
            format!("node-{leader} wins a majority and becomes leader in term {term}"),
        );
        trace.run_for(&mut cluster, 150);
        if cluster.nodes().all(|(_, node)| node.last_applied() >= 1) {
            trace.note(
                &cluster,
                "the leader's noop entry has committed and been applied on every node",
            );
        }
    }
    Scenario {
        name: "Election",
        description: "Five followers with no leader. The first node to time out asks for votes and wins a majority in a new term.",
        samples: trace.samples,
    }
}

fn write_scenario() -> Scenario {
    let mut cluster = Cluster::new(5);
    assert!(
        cluster.run_until(600, |cluster| cluster.leader().is_some()),
        "no leader for the write scenario"
    );
    assert!(
        settle(&mut cluster),
        "the cluster did not settle before the write"
    );
    let leader = cluster.leader().expect("settled leader");
    let mut trace = Trace::new(&cluster);
    trace.start(
        &cluster,
        format!("node-{leader} leads; the election noop is committed and applied everywhere"),
    );
    assert!(
        committed_write(
            &mut trace,
            &mut cluster,
            leader,
            set("foo", "bar"),
            "set foo=bar"
        ),
        "the write scenario must commit"
    );
    Scenario {
        name: "Write",
        description: "A client sends set foo=bar to the leader. The entry reaches the log, replicates, commits once a majority holds it, and every node applies it to its own data.",
        samples: trace.samples,
    }
}

fn failover_scenario() -> Scenario {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    assert!(settle(&mut cluster));
    let first_leader = cluster.leader().expect("first leader");
    let mut trace = Trace::new(&cluster);
    trace.start(
        &cluster,
        format!("node-{first_leader} leads; the election noop is committed"),
    );
    assert!(committed_write(
        &mut trace,
        &mut cluster,
        first_leader,
        set("foo", "bar"),
        "set foo=bar"
    ));
    cluster.stop(first_leader);
    trace.note(
        &cluster,
        format!("node-{first_leader} is killed; it stops sending and answering until it is switched back on"),
    );
    let deadline = cluster.now() + 1_200;
    let elected = trace.run_until(&mut cluster, deadline, |cluster| {
        cluster
            .leader()
            .is_some_and(|leader| leader != first_leader)
    });
    assert!(elected, "no successor leader after the kill");
    let new_leader = cluster.leader().expect("successor leader");
    let new_term = cluster.node(new_leader).current_term();
    trace.note(
        &cluster,
        format!("node-{new_leader} is elected in term {new_term}; every committed entry is still present"),
    );
    assert!(committed_write(
        &mut trace,
        &mut cluster,
        new_leader,
        set("baz", "qux"),
        "set baz=qux"
    ));
    cluster.restart(first_leader);
    trace.note(
        &cluster,
        format!("node-{first_leader} is switched back on and fetches the entries it missed"),
    );
    let deadline = cluster.now() + 2_000;
    let caught_up = trace.run_until(&mut cluster, deadline, |cluster| {
        cluster.nodes().all(|(_, node)| {
            node.get("foo") == Some("bar".to_string()) && node.get("baz") == Some("qux".to_string())
        })
    });
    assert!(caught_up, "the restarted node did not catch up");
    trace.note(&cluster, "every node agrees on both writes");
    Scenario {
        name: "Failover",
        description: "The leader is killed after a committed write. The rest elect a new leader, a second write commits, and the old node catches up when it returns.",
        samples: trace.samples,
    }
}

fn partition_scenario() -> Scenario {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    assert!(settle(&mut cluster));
    let isolated = cluster.leader().expect("leader before the partition");
    let mut trace = Trace::new(&cluster);
    let term = cluster.node(isolated).current_term();
    trace.start(
        &cluster,
        format!("node-{isolated} leads in term {term}; the election noop is committed"),
    );

    let connected: Vec<NodeId> = (0..5).filter(|id| *id != isolated).collect();
    cluster.partition(&[vec![isolated], connected]);
    trace.note(
        &cluster,
        format!("node-{isolated} is cut off; every link between it and the other four is dropped"),
    );

    trace.send_client(
        &cluster,
        "set lost=v",
        format!("client sends set lost=v to node-{isolated}"),
    );
    let write = cluster
        .begin_write(isolated, set("lost", "v"))
        .expect("the isolated leader accepts its own write");
    trace.note(
        &cluster,
        format!(
            "node-{isolated} appends it at index {}, with no one to replicate to",
            write.index
        ),
    );

    let deadline = cluster.now() + 1_000;
    let elected = trace.run_until(&mut cluster, deadline, |cluster| {
        cluster
            .nodes()
            .any(|(id, node)| id != isolated && node.role() == Role::Leader)
    });
    assert!(elected, "the majority side did not elect a leader");
    let new_leader = cluster
        .nodes()
        .find(|(id, node)| *id != isolated && node.role() == Role::Leader)
        .map(|(id, _)| id)
        .expect("majority leader");
    let new_term = cluster.node(new_leader).current_term();
    trace.note(
        &cluster,
        format!("the four connected nodes elect node-{new_leader} in term {new_term}"),
    );
    trace.finish_client(
        &cluster,
        ClientStatus::Refused,
        Some("no majority, never committed".to_string()),
        "set lost=v can never commit on the isolated node, so the client gets no answer",
    );

    assert!(committed_write(
        &mut trace,
        &mut cluster,
        new_leader,
        set("kept", "k"),
        "set kept=k"
    ));

    cluster.heal();
    trace.note(
        &cluster,
        format!("the split heals; node-{isolated} sees term {new_term} and its stranded entry is replaced"),
    );
    let deadline = cluster.now() + 2_000;
    let converged = trace.run_until(&mut cluster, deadline, |cluster| {
        cluster.nodes().all(|(_, node)| {
            node.get("kept") == Some("k".to_string()) && node.get("lost").is_none()
        })
    });
    assert!(converged, "the cluster did not converge after the heal");
    trace.note(
        &cluster,
        "every node agrees on set kept=k, and set lost=v never happened",
    );
    Scenario {
        name: "Partition",
        description: "The leader is cut off from the other four. A write it accepts never commits; the majority elects a new leader; when the split heals, the stranded entry is overwritten.",
        samples: trace.samples,
    }
}

const EXPLORER_TEMPLATE: &str = r##"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>raft-kv · trace explorer</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'%3E%3Crect width='16' height='16' rx='3' fill='%230f1115'/%3E%3Ccircle cx='8' cy='8' r='3' fill='%233fb950'/%3E%3C/svg%3E">
<style>
  :root{color-scheme:dark;--bg:#0f1115;--panel:#171a21;--panel-2:#11161d;--line:#30363d;--line-soft:#21262d;--text:#e6e1d9;--prose:#c3cad2;--muted:#8b949e;--green:#3fb950;--yellow:#d29922;--red:#f85149;--sans:ui-sans-serif,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;--mono:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font:15px/1.6 var(--sans);-webkit-font-smoothing:antialiased}
  main{max-width:1080px;margin:0 auto;padding:44px 22px 90px}
  .kicker{margin:0 0 6px;font:600 11.5px/1 var(--mono);letter-spacing:.14em;text-transform:uppercase;color:var(--muted)}
  h1{margin:0 0 10px;font-size:30px;line-height:1.2;letter-spacing:-.014em;font-weight:650}
  .intro{margin:0 0 26px;max-width:66ch;color:var(--prose)}
  .tabs{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 10px}
  .tab,.control{font:13.5px/1 var(--sans);color:var(--prose);background:transparent;border:1px solid var(--line);border-radius:7px;padding:9px 13px;cursor:pointer}
  .control{padding:8px 12px}
  .tab:hover,.control:hover{border-color:#545c66;color:var(--text)}
  .tab.active{background:#21262d;border-color:#6e7781;color:var(--text);font-weight:600}
  .control:disabled{opacity:.4;cursor:default}
  .tab:focus-visible,.control:focus-visible,input[type=range]:focus-visible{outline:2px solid #58a6ff;outline-offset:2px}
  .description{margin:0 0 16px;color:var(--muted);font-size:14px;min-height:22px}
  .transport{display:flex;align-items:center;gap:10px;flex-wrap:wrap;background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:10px 12px;margin:0 0 12px}
  .transport label{font:12px/1 var(--mono);color:var(--muted)}
  input[type=range]{width:120px;accent-color:#8b949e}
  .speed-label{font:12px/1 var(--mono);color:var(--muted)}
  .counters{margin-left:auto;display:flex;gap:18px;font:12.5px/1 var(--mono);color:var(--muted)}
  .counters b{color:var(--text);font-weight:600}
  .step-event{margin:0 0 14px;padding:11px 14px;border-left:3px solid #6e7781;background:#131820;border-radius:0 7px 7px 0;font-size:14px;min-height:46px;display:flex;align-items:center}
  .board{display:grid;grid-template-columns:minmax(0,1fr) 302px;gap:14px;align-items:start}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:9px;padding:14px}
  .panel-title{margin:0 0 10px;font:600 10.5px/1 var(--mono);letter-spacing:.12em;text-transform:uppercase;color:var(--muted)}
  .client{display:flex;align-items:center;gap:10px;padding:9px 12px;border:1px solid var(--line);border-radius:7px;background:var(--panel-2);margin:0 0 12px;font:12.5px/1.5 var(--mono)}
  .client.hidden{display:none}
  .client .k{color:var(--muted)}
  .client .st{margin-left:auto;text-align:right}
  .client.waiting{border-left:3px solid var(--yellow)}
  .client.waiting .st{color:var(--yellow)}
  .client.committed{border-left:3px solid var(--green)}
  .client.committed .st{color:var(--green)}
  .client.refused{border-left:3px solid var(--red)}
  .client.refused .st{color:var(--red)}
  .node,.ruler-row{display:grid;grid-template-columns:126px 148px minmax(0,1fr) 100px;gap:12px;align-items:center;padding:9px 10px;border:1px solid var(--line-soft);border-radius:8px;background:var(--panel-2)}
  .node + .node{margin-top:6px}
  .node.is-stopped{opacity:.55}
  .ruler-row{border-color:transparent;background:transparent;padding:0 10px 3px;color:var(--muted);font:11px/1 var(--mono)}
  .ident{display:flex;align-items:center;gap:8px;min-width:0}
  .nid{font:13px/1 var(--mono)}
  .chip{font:700 10px/1 var(--mono);letter-spacing:.08em;padding:4px 7px;border-radius:999px;color:#0f1115;background:var(--muted);white-space:nowrap}
  .chip.leader{background:var(--green)}
  .chip.candidate{background:var(--yellow)}
  .chip.follower{background:#8b949e}
  .chip.stopped{background:var(--red)}
  .meta{font:11.5px/1.5 var(--mono);color:var(--muted);min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .cells{display:flex;flex-wrap:wrap;gap:5px;padding:1px 0}
  .cell{flex:0 0 96px;height:26px;display:flex;align-items:center;padding:0 8px;border-radius:5px;border:1px solid var(--line);background:transparent;font:11.5px/1 var(--mono);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .cell.committed{border-color:var(--green)}
  .cell.applied{border-color:var(--green);background:rgba(63,185,80,.14)}
  .cell.ruler{border-color:transparent;justify-content:center;padding:0;color:var(--muted)}
  .empty{font:11.5px/26px var(--mono);color:var(--muted)}
  .counts{text-align:right;font:11.5px/1.6 var(--mono);color:var(--muted)}
  .counts span{display:block}
  .counts b{color:var(--text);font-weight:600}
  .legend{display:flex;gap:16px;flex-wrap:wrap;margin:12px 0 0;padding-top:10px;border-top:1px solid var(--line-soft);font:11.5px/1.5 var(--mono);color:var(--muted)}
  .legend span{display:flex;align-items:center;gap:6px}
  .sw{width:12px;height:12px;border-radius:4px;border:1px solid var(--line);flex:none}
  .sw.committed{border-color:var(--green)}
  .sw.applied{border-color:var(--green);background:rgba(63,185,80,.14)}
  .events-panel{max-height:660px;display:flex;flex-direction:column}
  .events{overflow-y:auto;flex:1;min-height:140px}
  .event{display:grid;grid-template-columns:62px 1fr;gap:10px;padding:7px 0;border-bottom:1px solid var(--line-soft);font-size:12.5px;line-height:1.45;color:var(--muted)}
  .event:last-child{border-bottom:0}
  .event time{font:11.5px/1.45 var(--mono);color:#6e7781}
  .event.note{color:var(--text)}
  .footnote{margin:16px 0 0;color:var(--muted);font-size:12.5px;max-width:72ch}
  @media (max-width:980px){
    .board{grid-template-columns:1fr}
    .events-panel{max-height:none}
  }
  @media (max-width:760px){
    .node{grid-template-columns:1fr auto;grid-template-areas:"ident counts" "meta meta" "cells cells"}
    .node .ident{grid-area:ident}
    .node .meta{grid-area:meta}
    .node .cells{grid-area:cells}
    .node .counts{grid-area:counts}
    .ruler-row{display:none}
    .counters{margin-left:0;width:100%}
  }
</style>
</head>
<body>
<main>
  <p class="kicker">raft-kv · simulator traces</p>
  <h1>Watch a Raft cluster agree</h1>
  <p class="intro">Every frame on this page is recorded by the Rust simulator in this repository, the same code the tests run. Step through an election, a write, a leader failure, and a partition. Times are simulated milliseconds, not wall-clock.</p>
  <nav class="tabs" id="scenarios" aria-label="Scenarios"></nav>
  <p class="description" id="description"></p>
  <div class="transport">
    <button type="button" class="control" id="reset" title="Back to the first sample">Reset</button>
    <button type="button" class="control" id="back" title="Previous sample">Back</button>
    <button type="button" class="control" id="play" title="Play or pause">Play</button>
    <button type="button" class="control" id="step" title="Next sample">Step</button>
    <label for="speed">Speed</label>
    <input id="speed" type="range" min="100" max="1600" value="600" step="100">
    <span class="speed-label" id="speedLabel">600 ms per step</span>
    <div class="counters"><span>time <b id="time">0 ms</b></span><span>step <b id="position">1 / 1</b></span></div>
  </div>
  <p class="step-event" id="currentEvent">initial state</p>
  <div class="board">
    <section class="panel">
      <div class="client hidden" id="clientStrip"></div>
      <div id="ruler"></div>
      <div id="nodes"></div>
      <div class="legend">
        <span><span class="sw" aria-hidden="true"></span>stored on this node</span>
        <span><span class="sw committed" aria-hidden="true"></span>committed by a majority</span>
        <span><span class="sw applied" aria-hidden="true"></span>applied to the state machine</span>
      </div>
    </section>
    <aside class="panel events-panel">
      <h2 class="panel-title">Event log</h2>
      <div class="events" id="events"></div>
    </aside>
  </div>
  <p class="footnote">In these traces, stop and restart keep a node's memory, so a restarted node only has to fetch what it missed. Recovery from a real crash is covered by the process-level tests in this repository.</p>
</main>
<script>
const traces = /*__TRACES__*/;
let scenario = 0, index = 0, playing = false, timer = null;
const $ = id => document.getElementById(id);
const esc = text => String(text).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));

const clientText = client => {
  if (client.status === 'waiting') return 'waiting for a majority';
  if (client.status === 'committed') return client.elapsed_ms === 0 ? 'committed under 1 ms' : `committed in ${client.elapsed_ms} ms`;
  return client.detail || 'no answer';
};

function eventFor(sample, previous) {
  if (sample.note) return sample.note;
  if (!previous) return 'initial state';
  const changes = [];
  sample.nodes.forEach((node, i) => {
    const old = previous.nodes[i];
    if (!old) return;
    if (node.stopped && !old.stopped) { changes.push(`node-${node.id} stops`); return; }
    if (!node.stopped && old.stopped) { changes.push(`node-${node.id} starts again`); return; }
    if (node.role !== old.role) {
      if (node.role === 'Candidate') changes.push(`node-${node.id} campaigns in term ${node.term}`);
      else if (node.role === 'Leader') changes.push(`node-${node.id} becomes leader in term ${node.term}`);
      else changes.push(`node-${node.id} steps down to follower`);
    } else if (node.term !== old.term) {
      changes.push(`node-${node.id} moves to term ${node.term}`);
    }
    node.log.forEach((entry, at) => {
      if (old.log[at] !== undefined && old.log[at] !== entry) changes.push(`node-${node.id} replaces entry ${at + 1} with ${entry}`);
    });
    if (node.log.length > old.log.length) changes.push(`node-${node.id} appends ${node.log.slice(old.log.length).join(', ')}`);
    if (node.voted_for !== old.voted_for && node.voted_for !== null && node.role !== 'Leader' && node.role !== 'Candidate') changes.push(`node-${node.id} votes for node-${node.voted_for}`);
    if (node.commit !== old.commit) changes.push(`node-${node.id} commits through ${node.commit}`);
    else if (node.applied !== old.applied) changes.push(`node-${node.id} applies through ${node.applied}`);
  });
  if (sample.client && (!previous.client || sample.client.label !== previous.client.label || sample.client.status !== previous.client.status)) {
    changes.push(`client ${sample.client.label}: ${clientText(sample.client)}`);
  }
  return changes.join(' · ') || 'heartbeats keep the nodes aligned';
}

function traceWidth(trace) {
  return Math.max(0, ...trace.samples.map(sample => sample.nodes.reduce((max, node) => Math.max(max, node.log.length), 0)));
}

function nodeHtml(node) {
  const cells = node.log.length
    ? node.log.map((entry, at) => {
        const position = at + 1;
        const state = position <= node.applied ? 'applied' : position <= node.commit ? 'committed' : 'stored';
        return `<span class="cell ${state}" title="index ${position}: ${esc(entry)}">${esc(entry)}</span>`;
      }).join('')
    : '<span class="empty">no entries</span>';
  const chip = node.stopped
    ? '<span class="chip stopped">STOPPED</span>'
    : `<span class="chip ${node.role.toLowerCase()}">${node.role.toUpperCase()}</span>`;
  const hold = node.role === 'Leader' || node.voted_for === null
    ? `term ${node.term}`
    : `term ${node.term} · voted node-${node.voted_for}`;
  const label = `node-${node.id}, ${node.stopped ? 'stopped' : node.role.toLowerCase()}, term ${node.term}, ${node.log.length} log entries, commit ${node.commit}, applied ${node.applied}`;
  return `<article class="node${node.stopped ? ' is-stopped' : ''}" aria-label="${esc(label)}">
    <div class="ident"><span class="nid">node-${node.id}</span>${chip}</div>
    <div class="meta">${hold}</div>
    <div class="cells">${cells}</div>
    <div class="counts"><span>commit <b>${node.commit}</b></span><span>applied <b>${node.applied}</b></span></div>
  </article>`;
}

function renderTabs() {
  $('scenarios').innerHTML = traces.map((trace, i) =>
    `<button type="button" class="tab${i === scenario ? ' active' : ''}" data-scenario="${i}" aria-pressed="${i === scenario}">${esc(trace.name)}</button>`
  ).join('');
}

function render() {
  const trace = traces[scenario];
  const sample = trace.samples[index];
  const previous = trace.samples[index - 1];

  $('description').textContent = trace.description;
  $('currentEvent').textContent = eventFor(sample, previous);
  $('time').textContent = `${sample.time_ms} ms`;
  $('position').textContent = `${index + 1} / ${trace.samples.length}`;
  $('back').disabled = index === 0;
  $('step').disabled = index === trace.samples.length - 1;
  $('play').textContent = playing ? 'Pause' : 'Play';

  const strip = $('clientStrip');
  if (sample.client) {
    strip.className = `client ${sample.client.status}`;
    strip.innerHTML = `<span class="k">client</span><span>${esc(sample.client.label)}</span><span class="st">${esc(clientText(sample.client))}</span>`;
  } else {
    strip.className = 'client hidden';
    strip.innerHTML = '';
  }

  const width = traceWidth(trace);
  $('ruler').innerHTML = width
    ? `<div class="ruler-row"><div class="ident"></div><div class="meta">index</div><div class="cells">${Array.from({ length: width }, (_, i) => `<span class="cell ruler">${i + 1}</span>`).join('')}</div><div class="counts"></div></div>`
    : '';
  $('nodes').innerHTML = sample.nodes.map(nodeHtml).join('');
  $('events').innerHTML = trace.samples.slice(0, index + 1).map((entry, i) => {
    const text = eventFor(entry, trace.samples[i - 1]);
    return `<div class="event${entry.note ? ' note' : ''}"><time>${entry.time_ms} ms</time><span>${esc(text)}</span></div>`;
  }).reverse().join('');
}

function stop() {
  if (timer) { clearInterval(timer); timer = null; }
  playing = false;
  $('play').textContent = 'Play';
}

function advance() {
  if (index < traces[scenario].samples.length - 1) {
    index += 1;
    render();
  } else {
    stop();
  }
}

function back() {
  if (index > 0) {
    index -= 1;
    render();
  }
}

function play() {
  if (playing) { stop(); return; }
  if (index === traces[scenario].samples.length - 1) index = 0;
  playing = true;
  timer = setInterval(advance, Number($('speed').value));
  render();
}

function select(next) {
  stop();
  scenario = next;
  index = 0;
  renderTabs();
  render();
}

$('scenarios').onclick = event => {
  const button = event.target.closest('button[data-scenario]');
  if (!button) return;
  select(Number(button.dataset.scenario));
};
$('play').onclick = play;
$('step').onclick = advance;
$('back').onclick = back;
$('reset').onclick = () => { stop(); index = 0; render(); };
$('speed').oninput = event => {
  $('speedLabel').textContent = `${event.target.value} ms per step`;
  if (playing) {
    clearInterval(timer);
    timer = setInterval(advance, Number(event.target.value));
  }
};
document.onkeydown = event => {
  if (event.target && event.target.closest && event.target.closest('button, input, select, textarea')) return;
  if (event.key === ' ') { event.preventDefault(); play(); }
  else if (event.key === 'ArrowRight') { event.preventDefault(); advance(); }
  else if (event.key === 'ArrowLeft') { event.preventDefault(); back(); }
};
$('speedLabel').textContent = `${$('speed').value} ms per step`;
renderTabs();
render();
</script>
</body>
</html>
"##;

fn render_explorer(scenarios: &[Scenario]) -> String {
    let data = scenarios
        .iter()
        .map(scenario_json)
        .collect::<Vec<_>>()
        .join(",");
    EXPLORER_TEMPLATE.replace("/*__TRACES__*/", &format!("[{data}]"))
}

fn scenario_json(scenario: &Scenario) -> String {
    let samples = scenario
        .samples
        .iter()
        .map(sample_json)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"name\":\"{}\",\"description\":\"{}\",\"samples\":[{}]}}",
        json_escape(scenario.name),
        json_escape(scenario.description),
        samples
    )
}

fn sample_json(sample: &Sample) -> String {
    let nodes = sample
        .nodes
        .iter()
        .map(node_json)
        .collect::<Vec<_>>()
        .join(",");
    let client = match &sample.client {
        Some(client) => format!(
            "{{\"label\":\"{}\",\"status\":\"{}\",\"elapsed_ms\":{},\"detail\":{}}}",
            json_escape(&client.label),
            client.status.label(),
            client.elapsed_ms,
            json_optional(client.detail.as_deref())
        ),
        None => "null".to_string(),
    };
    format!(
        "{{\"time_ms\":{},\"note\":{},\"nodes\":[{}],\"client\":{}}}",
        sample.time_ms,
        json_optional(sample.note.as_deref()),
        nodes,
        client
    )
}

fn node_json(node: &NodeSample) -> String {
    let log = node
        .log
        .iter()
        .map(|entry| format!("\"{}\"", json_escape(entry)))
        .collect::<Vec<_>>()
        .join(",");
    let voted_for = node
        .voted_for
        .map_or_else(|| "null".to_string(), |id| id.to_string());
    format!(
        "{{\"id\":{},\"role\":\"{:?}\",\"term\":{},\"voted_for\":{},\"log\":[{}],\"commit\":{},\"applied\":{},\"stopped\":{}}}",
        node.id, node.role, node.term, voted_for, log, node.commit, node.applied, node.stopped
    )
}

fn json_optional(value: Option<&str>) -> String {
    match value {
        Some(text) => format!("\"{}\"", json_escape(text)),
        None => "null".to_string(),
    }
}

fn json_escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            '<' => escaped.push_str("\\u003c"),
            '>' => escaped.push_str("\\u003e"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            character if character.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn render_svg(title: &str, samples: &[Sample]) -> String {
    let node_count = samples.first().map_or(0, |sample| sample.nodes.len());
    let width = 920;
    let left = 88;
    let top = 72;
    let cell_w = 56;
    let cell_h = 34;
    let row_gap = 14;
    let height = top + node_count as i32 * (cell_h + row_gap) + 48;
    let mut svg = format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="#101114"/>
<text x="24" y="34" fill="#f4f1ea" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="20" font-weight="700">raft-kv · {title}</text>
<text x="24" y="56" fill="#9ca3af" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="12">green leader · yellow candidate · gray follower</text>
"##
    );
    for node in 0..node_count {
        let y = top + node as i32 * (cell_h + row_gap);
        svg.push_str(&format!(
            r##"<text x="24" y="{}" fill="#d1d5db" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13">node {node}</text>
"##,
            y + 22
        ));
    }
    for (column, sample) in samples.iter().enumerate() {
        let x = left + column as i32 * cell_w;
        svg.push_str(&format!(
            r##"<text x="{}" y="66" fill="#6b7280" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="10">{}ms</text>
"##,
            x, sample.time_ms
        ));
        if let Some(note) = &sample.note {
            svg.push_str(&format!(
                r##"<text x="{}" y="{}" fill="#f87171" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11">{}</text>
"##,
                x,
                height - 18,
                escape(note)
            ));
        }
        for (row, node) in sample.nodes.iter().enumerate() {
            let y = top + row as i32 * (cell_h + row_gap);
            let (fill, label) = match node.role {
                Role::Follower => ("#374151", "F"),
                Role::Candidate => ("#d97706", "C"),
                Role::Leader => ("#16a34a", "L"),
            };
            svg.push_str(&format!(
                r##"<rect x="{x}" y="{y}" width="44" height="28" rx="6" fill="{fill}"/>
<text x="{}" y="{}" fill="#fff7ed" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="13" font-weight="700">{label}</text>
"##,
                x + 17,
                y + 19
            ));
        }
    }
    svg.push_str("</svg>\n");
    svg
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn update_readme(path: &str, replication: &str, metrics: &str) -> io::Result<()> {
    let Ok(readme) = fs::read_to_string(path) else {
        return Ok(());
    };
    let readme = replace_section(&readme, "replication", replication);
    let readme = replace_section(&readme, "metrics", metrics);
    fs::write(path, readme)
}

fn replace_section(readme: &str, name: &str, content: &str) -> String {
    let start = format!("<!-- {name}:start -->");
    let end = format!("<!-- {name}:end -->");
    let Some(start_index) = readme.find(&start) else {
        return readme.to_string();
    };
    let Some(end_index) = readme.find(&end) else {
        return readme.to_string();
    };
    let before = &readme[..start_index + start.len()];
    let after = &readme[end_index..];
    format!("{before}\n{}\n{after}", content.trim_end())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::process::{Command as ProcessCommand, Stdio};

    fn all_scenarios() -> Vec<Scenario> {
        vec![
            election_scenario(),
            write_scenario(),
            failover_scenario(),
            partition_scenario(),
        ]
    }

    fn assert_sane_trace(samples: &[Sample]) {
        assert!(!samples.is_empty());
        assert_eq!(samples[0].time_ms, 0, "every trace starts at 0 ms");
        assert!(
            samples
                .windows(2)
                .all(|window| window[0].time_ms <= window[1].time_ms),
            "sample times only move forward"
        );
        assert!(samples.iter().all(|sample| sample.nodes.len() == 5));
    }

    fn last_sample(scenario: &Scenario) -> &Sample {
        scenario.samples.last().expect("scenario has samples")
    }

    fn holds(sample: &Sample, entry: &str) -> bool {
        sample
            .nodes
            .iter()
            .any(|node| node.log.iter().any(|logged| logged == entry))
    }

    #[test]
    fn explorer_page_contains_every_scenario_and_control() {
        let html = render_explorer(&all_scenarios());

        assert!(html.starts_with("<!doctype html>"));
        assert_eq!(html.matches("<script>").count(), 1);
        assert_eq!(html.matches("</script>").count(), 1);
        for name in ["Election", "Write", "Failover", "Partition"] {
            assert!(
                html.contains(&format!("\"name\":\"{name}\"")),
                "scenario {name} is missing from the page"
            );
        }
        for control in [
            "id=\"scenarios\"",
            "id=\"play\"",
            "id=\"step\"",
            "id=\"back\"",
            "id=\"reset\"",
            "id=\"speed\"",
        ] {
            assert!(
                html.contains(control),
                "control {control} is missing from the page"
            );
        }
        assert!(!html.contains("<script src="));
        assert!(!html.contains("<link rel=\"stylesheet\""));
    }

    #[test]
    fn election_scenario_ends_with_one_leader() {
        let scenario = election_scenario();
        assert_sane_trace(&scenario.samples);
        assert!(
            scenario
                .samples
                .iter()
                .any(|sample| sample.nodes.iter().any(|node| node.role == Role::Candidate)),
            "the trace shows a candidate before the election is decided"
        );
        assert_eq!(
            last_sample(&scenario)
                .nodes
                .iter()
                .filter(|node| node.role == Role::Leader)
                .count(),
            1
        );
    }

    #[test]
    fn write_scenario_holds_the_entry_before_a_majority_commits_it() {
        let scenario = write_scenario();
        assert_sane_trace(&scenario.samples);

        let held_uncommitted = scenario.samples.iter().any(|sample| {
            sample.nodes.iter().any(|node| {
                let Some(position) = node.log.iter().position(|entry| entry == "set foo=bar")
                else {
                    return false;
                };
                node.commit < (position as u64) + 1
            })
        });
        assert!(
            held_uncommitted,
            "a node holds the entry before a majority commits it"
        );

        assert!(
            scenario.samples.iter().any(|sample| sample
                .client
                .as_ref()
                .is_some_and(|client| client.status == ClientStatus::Committed)),
            "the client is answered once the write commits"
        );

        let applied_everywhere = last_sample(&scenario).nodes.iter().all(|node| {
            node.log
                .iter()
                .position(|entry| entry == "set foo=bar")
                .is_some_and(|position| node.applied > (position as u64))
        });
        assert!(applied_everywhere, "every node applies the write");
    }

    #[test]
    fn failover_scenario_restarts_the_killed_node_and_catches_up() {
        let scenario = failover_scenario();
        assert_sane_trace(&scenario.samples);

        let stopped_sample = scenario
            .samples
            .iter()
            .find(|sample| sample.nodes.iter().any(|node| node.stopped))
            .expect("the failover trace kills a node");
        let killed = stopped_sample
            .nodes
            .iter()
            .find(|node| node.stopped)
            .expect("stopped node")
            .id;
        assert!(
            last_sample(&scenario)
                .nodes
                .iter()
                .all(|node| !node.stopped),
            "the killed node is running again at the end"
        );

        let caught_up = last_sample(&scenario)
            .nodes
            .iter()
            .find(|node| node.id == killed)
            .expect("the killed node is in the final sample");
        assert!(caught_up.log.iter().any(|entry| entry == "set foo=bar"));
        assert!(caught_up.log.iter().any(|entry| entry == "set baz=qux"));
        assert!(caught_up.applied as usize >= caught_up.log.len());
    }

    #[test]
    fn partition_scenario_overwrites_the_stranded_entry() {
        let scenario = partition_scenario();
        assert_sane_trace(&scenario.samples);

        assert!(
            scenario
                .samples
                .iter()
                .any(|sample| holds(sample, "set lost=v")),
            "the isolated leader logs the entry"
        );
        assert!(
            scenario.samples.iter().any(|sample| sample
                .client
                .as_ref()
                .is_some_and(|client| client.status == ClientStatus::Refused)),
            "the client waiting on the isolated leader is refused"
        );

        let final_sample = last_sample(&scenario);
        assert!(
            final_sample
                .nodes
                .iter()
                .all(|node| !node.log.iter().any(|entry| entry == "set lost=v")),
            "the stranded entry is replaced once the split heals"
        );
        assert!(
            final_sample
                .nodes
                .iter()
                .all(|node| node.log.iter().any(|entry| entry == "set kept=k")),
            "the majority's write is on every node"
        );
    }

    #[test]
    fn json_escape_covers_embedded_string_delimiters() {
        assert_eq!(
            json_escape("quote\" slash\\ line\nreturn\r tab\t </script>\u{2028}"),
            "quote\\\" slash\\\\ line\\nreturn\\r tab\\t \\u003c/script\\u003e\\u2028"
        );
    }

    #[test]
    fn generated_explorer_passes_runtime_probe() {
        let html = render_explorer(&all_scenarios());
        let script = r#"
const fs = require('fs'), vm = require('vm');
const html = fs.readFileSync(0, 'utf8');
const source = html.match(/<script>([\s\S]*)<\/script>/)[1];
const elements = new Map();
const element = () => ({ value: '', textContent: '', innerHTML: '', className: '', disabled: false, onclick: null, oninput: null, options: [], add() {} });
for (const id of ['scenarios', 'description', 'clientStrip', 'ruler', 'nodes', 'events', 'time', 'position', 'currentEvent', 'play', 'step', 'back', 'reset', 'speed', 'speedLabel']) elements.set(id, element());
elements.get('speed').value = '600';
const timers = new Set();
const context = {
  document: { getElementById: id => elements.get(id) },
  setInterval(fn, ms) { const timer = { ms, fn }; timers.add(timer); return timer; },
  clearInterval(timer) { timers.delete(timer); }
};
vm.runInNewContext(source, context);
const play = elements.get('play'), position = elements.get('position');
if (timers.size !== 0 || play.textContent !== 'Play') throw new Error('not paused at initialization');
if (!/^1 \/ \d+$/.test(position.textContent)) throw new Error('initial position: ' + position.textContent);
if (elements.get('time').textContent !== '0 ms') throw new Error('first sample time: ' + elements.get('time').textContent);
elements.get('speed').value = '1000';
elements.get('speed').oninput({ target: elements.get('speed') });
if (timers.size !== 0 || play.textContent !== 'Play') throw new Error('paused speed change started playback');
elements.get('play').onclick();
if (timers.size !== 1 || [...timers][0].ms !== 1000 || play.textContent !== 'Pause') throw new Error('play did not start at the slider speed');
elements.get('play').onclick();
if (timers.size !== 0 || play.textContent !== 'Play') throw new Error('pause did not stop playback');
elements.get('step').onclick();
if (!position.textContent.startsWith('2 / ')) throw new Error('step did not advance: ' + position.textContent);
elements.get('back').onclick();
if (!position.textContent.startsWith('1 / ')) throw new Error('back did not return: ' + position.textContent);
elements.get('step').onclick();
elements.get('reset').onclick();
if (!position.textContent.startsWith('1 / ')) throw new Error('reset did not return: ' + position.textContent);
elements.get('scenarios').onclick({ target: { closest: () => ({ dataset: { scenario: '3' } }) } });
if (!/^1 \/ \d+$/.test(position.textContent)) throw new Error('scenario switch did not reset the step');
if (!elements.get('description').textContent) throw new Error('scenario switch did not update the description');
console.log('trace explorer runtime probe passed');
"#;
        let mut child = match ProcessCommand::new("node")
            .args(["-e", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("skipping explorer runtime probe: Node.js is not installed");
                return;
            }
            Err(error) => panic!("failed to start Node.js explorer runtime probe: {error}"),
        };
        child
            .stdin
            .take()
            .expect("node stdin")
            .write_all(html.as_bytes())
            .expect("write explorer HTML to node");
        let output = child.wait_with_output().expect("wait for node");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
