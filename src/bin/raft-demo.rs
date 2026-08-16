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
    let failover = failover_trace();
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
        render_explorer(&[("Election", &election), ("Failover", &failover)]),
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
    update_readme("README.md", &replication, &metrics)?;
    Ok(())
}

fn election_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    sample_until(&mut cluster, 600, |cluster| cluster.leader().is_some())
}

fn failover_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    cluster.run_for(200);
    let old_leader = cluster.leader().expect("leader");
    cluster.stop(old_leader);
    let mut samples = vec![sample(
        &cluster,
        cluster.now(),
        Some(format!("kill node {old_leader}")),
    )];
    for _ in (50..=600).step_by(50) {
        cluster.run_for(50);
        samples.push(sample(&cluster, cluster.now(), None));
    }
    samples
}

fn legacy_failover_trace() -> Vec<Sample> {
    let mut cluster = Cluster::new(5);
    assert!(cluster.run_until(600, |cluster| cluster.leader().is_some()));
    cluster.run_for(200);
    let old_leader = cluster.leader().expect("leader");
    let mut samples = vec![sample(&cluster, 0, Some(format!("kill node {old_leader}")))];
    cluster.stop(old_leader);
    for offset in (50..=600).step_by(50) {
        cluster.run_for(50);
        samples.push(sample(&cluster, offset, None));
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
        samples.push(sample(cluster, time, None));
        if done(cluster) {
            break;
        }
        cluster.run_for(50);
    }
    samples
}

fn sample(cluster: &Cluster, time_ms: u64, note: Option<String>) -> Sample {
    let mut roles: Vec<_> = cluster
        .nodes()
        .map(|(id, node)| (id, node.role()))
        .collect();
    roles.sort_by_key(|(id, _)| *id);
    Sample {
        time_ms,
        roles,
        note,
        nodes: cluster
            .nodes()
            .map(|(id, node)| NodeSample {
                id,
                term: node.current_term(),
                log_len: node.log().len(),
                commit: node.commit_index() as u64,
                applied: node.last_applied() as u64,
                stopped: cluster.is_stopped(id),
            })
            .collect(),
    }
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

#[derive(Clone, Debug)]
struct Sample {
    time_ms: u64,
    roles: Vec<(NodeId, Role)>,
    note: Option<String>,
    nodes: Vec<NodeSample>,
}

#[derive(Clone, Debug)]
struct NodeSample {
    id: NodeId,
    term: u64,
    log_len: usize,
    commit: u64,
    applied: u64,
    stopped: bool,
}

fn render_explorer(scenarios: &[(&str, &[Sample])]) -> String {
    let data = scenarios
        .iter()
        .map(|(name, samples)| {
            format!(
                "{{\"name\":\"{}\",\"samples\":[{}]}}",
                json_escape(name),
                samples
                    .iter()
                    .map(sample_json)
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        r##"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>raft-kv trace explorer</title><style>
:root{{color-scheme:dark;--bg:#0f1115;--panel:#171a21;--line:#30363d;--text:#e6e1d9;--muted:#8b949e;--green:#3fb950;--yellow:#d29922;--red:#f85149;--blue:#58a6ff}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px ui-monospace,SFMono-Regular,Menlo,monospace}}main{{max-width:1180px;margin:auto;padding:28px 20px}}h1{{font-size:24px;margin:0 0 6px}}p{{color:var(--muted);margin:0 0 22px}}.toolbar,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:14px;margin-bottom:14px}}button,select,input{{font:inherit;color:var(--text);background:#21262d;border:1px solid var(--line);border-radius:7px;padding:7px 10px}}button{{cursor:pointer}}button:hover{{border-color:var(--blue)}}input{{vertical-align:middle;width:180px}}.summary{{display:flex;gap:24px;flex-wrap:wrap;color:var(--muted);margin-top:12px}}.summary b{{color:var(--text)}}.nodes{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px}}.node{{border:1px solid var(--line);border-radius:10px;padding:12px;background:#11161d}}.node h2{{font-size:15px;margin:0 0 10px}}.role{{display:inline-block;border-radius:20px;padding:3px 8px;font-size:11px;font-weight:bold;color:#0f1115}}.leader{{background:var(--green)}}.candidate{{background:var(--yellow)}}.follower{{background:var(--muted)}}.stopped{{background:var(--red)}}.stats{{color:var(--muted);line-height:1.8}}.stats b{{color:var(--text)}}.events{{max-height:260px;overflow:auto}}.event{{padding:7px 0;border-bottom:1px solid #21262d}}.event:last-child{{border:0}}.event time{{color:var(--blue);display:inline-block;width:75px}}.legend{{color:var(--muted);font-size:12px}}
</style></head><body><main><h1>raft-kv / trace explorer</h1><p>Deterministic simulator traces. Move through time to inspect role, term, log, commit, and failover causality.</p>
<div class="toolbar"><label>Scenario <select id="scenario"></select></label> <button id="play">Play</button> <button id="step">Step</button> <button id="reset">Reset</button> <label>Speed <input id="speed" type="range" min="100" max="1600" value="600" step="100"></label><span id="speedLabel">600 ms/step</span><div class="summary"><span>time <b id="time">0 ms</b></span><span>sample <b id="position">0 / 0</b></span><span>event <b id="currentEvent">initial state</b></span></div></div>
<section class="panel"><div id="nodes" class="nodes"></div></section><section class="panel"><h2>Causal events</h2><div id="events" class="events"></div><div class="legend">Terms advance during elections. Commit and applied indexes show majority progress and state-machine application.</div></section>
</main><script>
const traces=[{data}];let scenario=0,index=0,timer=null;
const $=id=>document.getElementById(id), role=(name,stopped)=>stopped?'stopped':name.toLowerCase();
const escapeHtml=text=>String(text).replace(/[&<>"']/g,char=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[char]));
function eventFor(sample,previous){{if(sample.note)return sample.note;if(!previous)return'initial state';let changes=[];sample.nodes.forEach((node,i)=>{{let old=previous.nodes[i];if(node.stopped&&!old.stopped)changes.push('node-'+node.id+' stops');if(!node.stopped&&old.stopped)changes.push('node-'+node.id+' restarts');if(node.term>old.term)changes.push('node-'+node.id+' enters term '+node.term);if(node.log_len>old.log_len)changes.push('node-'+node.id+' appends log entry');if(node.commit>old.commit)changes.push('node-'+node.id+' commits through '+node.commit);}});return changes.join(' · ')||'heartbeats keep the cluster aligned';}}
function render(){{let trace=traces[scenario],sample=trace.samples[index],previous=trace.samples[index-1];$('time').textContent=sample.time_ms+' ms';$('position').textContent=(index+1)+' / '+trace.samples.length;$('currentEvent').textContent=eventFor(sample,previous);$('nodes').innerHTML=sample.nodes.map((node,i)=>{{let roleName=sample.roles[i][1],label=node.stopped?'STOPPED':roleName.toUpperCase();return `<article class="node"><h2>node-${{node.id}} <span class="role ${{role(roleName,node.stopped)}}">${{label}}</span></h2><div class="stats">term <b>${{node.term}}</b><br>log <b>${{node.log_len}} entries</b><br>commit <b>${{node.commit}}</b><br>applied <b>${{node.applied}}</b></div></article>`}}).join('');$('events').innerHTML=trace.samples.slice(0,index+1).map((s,i)=>`<div class="event"><time>${{s.time_ms}} ms</time>${{escapeHtml(eventFor(s,trace.samples[i-1]))}}</div>`).reverse().join('');}}
function reset(){{index=0;render()}}function step(){{if(index<traces[scenario].samples.length-1){{index++;render()}}else stop()}}function stop(){{if(timer){{clearInterval(timer);timer=null;$('play').textContent='Play'}}}}function schedule(){{if(timer){{clearInterval(timer);timer=setInterval(step,Number($('speed').value))}}}}function play(){{if(timer){{stop();return}}$('play').textContent='Pause';timer=setInterval(step,Number($('speed').value))}}
traces.forEach((trace,i)=>$('scenario').add(new Option(trace.name,i)));$('scenario').onchange=()=>{{stop();scenario=Number($('scenario').value);reset()}};$('play').onclick=play;$('step').onclick=step;$('reset').onclick=()=>{{stop();reset()}};$('speed').oninput=e=>{{$('speedLabel').textContent=e.target.value+' ms/step';schedule()}};render();
</script></body></html>
"##
    )
}

fn sample_json(sample: &Sample) -> String {
    let roles = sample
        .roles
        .iter()
        .map(|(id, role)| format!("[{},\"{:?}\"]", id, role))
        .collect::<Vec<_>>()
        .join(",");
    let nodes = sample
        .nodes
        .iter()
        .map(|node| format!("{{\"id\":{},\"term\":{},\"log_len\":{},\"commit\":{},\"applied\":{},\"stopped\":{}}}", node.id, node.term, node.log_len, node.commit, node.applied, node.stopped))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"time_ms\":{},\"roles\":[{}],\"nodes\":[{}],\"note\":{}}}",
        sample.time_ms,
        roles,
        nodes,
        sample
            .note
            .as_deref()
            .map(|note| format!("\"{}\"", json_escape(note)))
            .unwrap_or_else(|| "null".to_string())
    )
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
    let node_count = samples.first().map_or(0, |sample| sample.roles.len());
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
        for (row, (_, role)) in sample.roles.iter().enumerate() {
            let y = top + row as i32 * (cell_h + row_gap);
            let (fill, label) = match role {
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

    #[test]
    fn explorer_contains_both_scenarios_and_controls() {
        let election = election_trace();
        let failover = failover_trace();
        let html = render_explorer(&[("Election", &election), ("Failover", &failover)]);

        assert!(html.starts_with("<!doctype html>"));
        assert_eq!(html.matches("<script>").count(), 1);
        assert_eq!(html.matches("</script>").count(), 1);
        assert!(html.contains("\"name\":\"Election\""));
        assert!(html.contains("\"name\":\"Failover\""));
        assert!(html.contains("id=\"play\""));
        assert!(html.contains("id=\"speed\""));
        assert!(!html.contains("<script src="));
        assert!(!html.contains("<link rel=\"stylesheet\""));
    }

    #[test]
    fn failover_trace_records_real_stop_state_and_monotonic_time() {
        let samples = failover_trace();
        assert!(
            samples
                .windows(2)
                .all(|window| { window[0].time_ms < window[1].time_ms })
        );
        assert!(samples[0].nodes.iter().any(|node| node.stopped));
        assert!(
            samples[0]
                .note
                .as_deref()
                .is_some_and(|note| note.contains("kill node"))
        );
        assert!(samples.iter().skip(1).any(|sample| {
            sample
                .nodes
                .iter()
                .any(|node| node.term > samples[0].nodes[node.id].term)
        }));
    }

    #[test]
    fn json_escape_covers_embedded_string_delimiters() {
        assert_eq!(
            json_escape("quote\" slash\\ line\nreturn\r tab\t </script>\u{2028}"),
            "quote\\\" slash\\\\ line\\nreturn\\r tab\\t \\u003c/script\\u003e\\u2028"
        );
    }

    #[test]
    fn generated_explorer_passes_paused_speed_runtime_probe() {
        let election = election_trace();
        let failover = failover_trace();
        let html = render_explorer(&[("Election", &election), ("Failover", &failover)]);
        let script = r#"
const fs = require('fs'), vm = require('vm');
const html = fs.readFileSync(0, 'utf8');
const source = html.match(/<script>([\s\S]*)<\/script>/)[1];
const elements = new Map();
for (const id of ['scenario', 'play', 'step', 'reset', 'speed', 'speedLabel', 'time', 'position', 'currentEvent', 'nodes', 'events']) elements.set(id, {value: id === 'speed' ? '600' : '', textContent: id === 'play' ? 'Play' : '', innerHTML: '', options: [], add(option) { this.options.push(option); }});
const timers = new Set();
const context = {document: {getElementById: id => elements.get(id)}, Option: function(text, value) { return {text, value}; }, setInterval(fn, ms) { const timer = {ms}; timers.add(timer); return timer; }, clearInterval(timer) { timers.delete(timer); }};
vm.runInNewContext(source, context);
if (timers.size !== 0 || elements.get('play').textContent !== 'Play') throw new Error('not paused at initialization');
elements.get('speed').value = '1000';
elements.get('speed').oninput({target: elements.get('speed')});
if (timers.size !== 0 || elements.get('play').textContent !== 'Play') throw new Error('paused speed change started playback');
elements.get('play').onclick();
if (timers.size !== 1 || [...timers][0].ms !== 1000 || elements.get('play').textContent !== 'Pause') throw new Error('play state inconsistent');
console.log('paused speed runtime probe passed');
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
