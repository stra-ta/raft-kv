use raft_kv::lsm::{LsmOptions, LsmTree};
use raft_kv::net::{WireMessage, read_frame, write_peer_frame, write_reply_frame};
use raft_kv::observability::{NodeMetrics, init_tracing};
use raft_kv::storage::{PersistedState, load_node_with_state_machine, save_node};
use raft_kv::{ClientRequest, Node, NodeId, PendingRead, Role};
use std::collections::HashMap;
use std::env;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_millis(800);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_millis(75);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("raft-node: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> io::Result<()> {
    init_tracing();
    let config = Config::from_args(env::args().collect())?;
    let ids: Vec<_> = config.peers.keys().copied().collect();
    let mut peer_ids: Vec<_> = ids.into_iter().filter(|&id| id != config.id).collect();
    peer_ids.sort_unstable();
    let lsm = LsmTree::open(config.lsm_dir(), LsmOptions::default())?;
    let node = load_node_with_state_machine(&config.state_path, config.id, peer_ids, lsm)?;
    let metrics = NodeMetrics::new(config.id)?;
    let shared = Arc::new(Mutex::new(Runtime::new(
        node,
        metrics.clone(),
        config.snapshot_threshold,
    )));
    let transport = PeerTransport::new(&config.peers);
    {
        let mut runtime = shared.lock().expect("node mutex poisoned");
        runtime.refresh_metrics();
    }
    if let Some(metrics_addr) = config.metrics_addr {
        thread::spawn(move || serve_metrics(metrics_addr, metrics));
    } else {
        tracing::warn!(
            node = config.id,
            "metrics listener disabled: no metrics address"
        );
    }
    let ticker = Arc::clone(&shared);
    let ticker_transport = transport.clone();
    let tick_config = config.clone();
    thread::spawn(move || {
        loop {
            thread::sleep(Duration::from_millis(10));
            let messages = {
                let mut runtime = ticker.lock().expect("node mutex poisoned");
                let now = runtime.started.elapsed().as_millis() as u64;
                let messages = runtime.node.tick(now);
                if let Err(err) = runtime.persist_if_changed(&tick_config.state_path) {
                    tracing::error!(error = %err, "persist tick failed");
                }
                runtime.refresh_metrics();
                messages
            };
            send_all(&ticker_transport, messages);
        }
    });

    let bind = config
        .peers
        .get(&config.id)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing self address"))?;
    let listener = TcpListener::bind(bind)?;
    tracing::info!(node = config.id, raft_addr = %bind, metrics_addr = ?config.metrics_addr, "raft node listening");
    for stream in listener.incoming() {
        let stream = stream?;
        let shared = Arc::clone(&shared);
        let request_config = config.clone();
        let request_transport = transport.clone();
        thread::spawn(move || {
            if let Err(err) = handle_connection(stream, shared, request_config, request_transport) {
                tracing::error!(error = %err, "connection failed");
            }
        });
    }
    Ok(())
}

fn handle_connection(
    mut stream: TcpStream,
    shared: Arc<Mutex<Runtime>>,
    config: Config,
    transport: PeerTransport,
) -> io::Result<()> {
    loop {
        let wire = match read_frame(&mut stream) {
            Ok(wire) => wire,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(err) => return Err(err),
        };
        match wire {
            WireMessage::Peer(message) => {
                let replies = {
                    let mut runtime = shared.lock().expect("node mutex poisoned");
                    let now = runtime.started.elapsed().as_millis() as u64;
                    let replies = runtime.node.handle_message(message.from, message.rpc, now);
                    if let Err(err) = runtime.persist_if_changed(&config.state_path) {
                        tracing::error!(error = %err, "persist peer message failed");
                    }
                    runtime.refresh_metrics();
                    replies
                };
                send_all(&transport, replies);
            }
            WireMessage::ClientRequest(request) => {
                let is_write = matches!(
                    request,
                    ClientRequest::Set { .. } | ClientRequest::Delete { .. }
                );
                let started = Instant::now();
                let reply = if is_write {
                    let (write, messages) = {
                        let mut runtime = shared.lock().expect("node mutex poisoned");
                        let (write, messages) = match runtime.node.start_client_write(request) {
                            Ok(write) => write,
                            Err(reply) => return write_reply_frame(&mut stream, reply),
                        };
                        if let Err(err) = runtime.persist_if_changed(&config.state_path) {
                            tracing::error!(error = %err, "persist client write failed");
                            let reply = raft_kv::ClientReply {
                                success: false,
                                leader_id: Some(config.id),
                                response: None,
                            };
                            return write_reply_frame(&mut stream, reply);
                        }
                        runtime.refresh_metrics();
                        (write, messages)
                    };
                    send_all(&transport, messages);
                    let reply = wait_for_write(&shared, &config, write, started);
                    if reply.success {
                        let mut runtime = shared.lock().expect("node mutex poisoned");
                        runtime
                            .metrics
                            .observe_write(started.elapsed().as_secs_f64());
                        runtime.refresh_metrics();
                    }
                    reply
                } else if let ClientRequest::Get { key } = request {
                    let (read, messages) = {
                        let mut runtime = shared.lock().expect("node mutex poisoned");
                        let (read, messages) = match runtime.node.start_client_read() {
                            Ok(read) => read,
                            Err(reply) => return write_reply_frame(&mut stream, reply),
                        };
                        runtime.refresh_metrics();
                        (read, messages)
                    };
                    send_all(&transport, messages);
                    wait_for_read(&shared, &config, read, &key, started)
                } else {
                    let (reply, messages) = {
                        let mut runtime = shared.lock().expect("node mutex poisoned");
                        let (mut reply, messages) = runtime.node.handle_client_request(request);
                        if let Err(err) = runtime.persist_if_changed(&config.state_path) {
                            tracing::error!(error = %err, "persist client request failed");
                            reply.success = false;
                            reply.response = None;
                        }
                        runtime.refresh_metrics();
                        (reply, messages)
                    };
                    send_all(&transport, messages);
                    reply
                };
                write_reply_frame(&mut stream, reply)?;
                return Ok(());
            }
            WireMessage::ClientReply(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected ClientReply from peer or client",
                ));
            }
        }
    }
}

fn wait_for_write(
    shared: &Arc<Mutex<Runtime>>,
    config: &Config,
    write: raft_kv::PendingWrite,
    started: Instant,
) -> raft_kv::ClientReply {
    while started.elapsed() < CLIENT_WRITE_TIMEOUT {
        {
            let mut runtime = shared.lock().expect("node mutex poisoned");
            if runtime.node.write_committed_and_applied(write) {
                return raft_kv::ClientReply {
                    success: true,
                    leader_id: Some(config.id),
                    response: Some("committed".to_string()),
                };
            }
            if runtime.node.role() != Role::Leader || runtime.node.current_term() != write.term {
                return raft_kv::ClientReply {
                    success: false,
                    leader_id: runtime.node.leader_id(),
                    response: None,
                };
            }
            runtime.refresh_metrics();
        }
        thread::sleep(Duration::from_millis(10));
    }
    let mut runtime = shared.lock().expect("node mutex poisoned");
    runtime.refresh_metrics();
    raft_kv::ClientReply {
        success: false,
        leader_id: runtime.node.leader_id(),
        response: None,
    }
}

fn wait_for_read(
    shared: &Arc<Mutex<Runtime>>,
    config: &Config,
    read: PendingRead,
    key: &str,
    started: Instant,
) -> raft_kv::ClientReply {
    while started.elapsed() < CLIENT_WRITE_TIMEOUT {
        {
            let mut runtime = shared.lock().expect("node mutex poisoned");
            if runtime.node.read_committed_and_applied(read) {
                let response = runtime.node.read_value(key).ok().flatten();
                runtime.node.finish_read(read);
                runtime.refresh_metrics();
                return raft_kv::ClientReply {
                    success: true,
                    leader_id: Some(config.id),
                    response,
                };
            }
            if runtime.node.role() != Role::Leader || runtime.node.current_term() != read.term {
                runtime.node.finish_read(read);
                runtime.refresh_metrics();
                return raft_kv::ClientReply {
                    success: false,
                    leader_id: runtime.node.leader_id(),
                    response: None,
                };
            }
            runtime.refresh_metrics();
        }
        thread::sleep(Duration::from_millis(10));
    }
    let mut runtime = shared.lock().expect("node mutex poisoned");
    runtime.node.finish_read(read);
    runtime.refresh_metrics();
    raft_kv::ClientReply {
        success: false,
        leader_id: runtime.node.leader_id(),
        response: None,
    }
}

#[derive(Clone)]
struct PeerTransport {
    senders: Arc<HashMap<NodeId, SyncSender<raft_kv::Message>>>,
}

impl PeerTransport {
    fn new(peers: &HashMap<NodeId, String>) -> Self {
        let capacity = env::var("RAFT_KV_PEER_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(256)
            .clamp(1, 16_384);
        let mut senders = HashMap::new();
        for (&peer, addr) in peers {
            let Ok(addr) = addr.parse::<SocketAddr>() else {
                tracing::warn!(peer, addr, "invalid peer address; transport disabled");
                continue;
            };
            let (sender, receiver) = mpsc::sync_channel(capacity);
            thread::spawn(move || peer_worker(peer, addr, receiver));
            senders.insert(peer, sender);
        }
        Self {
            senders: Arc::new(senders),
        }
    }

    fn enqueue(&self, message: raft_kv::Message) {
        let Some(sender) = self.senders.get(&message.to) else {
            tracing::warn!(peer = message.to, "dropping message for unknown peer");
            return;
        };
        match sender.try_send(message) {
            Ok(()) => {}
            Err(TrySendError::Full(message)) => {
                tracing::warn!(
                    peer = message.to,
                    "peer queue full; message deferred to next heartbeat"
                );
            }
            Err(TrySendError::Disconnected(message)) => {
                tracing::warn!(peer = message.to, "peer transport stopped; message dropped");
            }
        }
    }
}

fn send_all(transport: &PeerTransport, messages: Vec<raft_kv::Message>) {
    for message in messages {
        transport.enqueue(message);
    }
}

fn peer_worker(peer: NodeId, addr: SocketAddr, receiver: Receiver<raft_kv::Message>) {
    let mut stream: Option<TcpStream> = None;
    let mut backoff = Duration::from_millis(10);
    while let Ok(message) = receiver.recv() {
        loop {
            if stream.is_none() {
                match TcpStream::connect_timeout(&addr, PEER_CONNECT_TIMEOUT) {
                    Ok(candidate) => {
                        let _ = candidate.set_write_timeout(Some(PEER_CONNECT_TIMEOUT));
                        stream = Some(candidate);
                        backoff = Duration::from_millis(10);
                    }
                    Err(err) => {
                        tracing::debug!(peer, %addr, error = %err, "peer connect failed; backing off");
                        thread::sleep(backoff);
                        backoff = (backoff * 2).min(Duration::from_millis(500));
                        continue;
                    }
                }
            }
            let result = stream
                .as_mut()
                .expect("stream initialized above")
                .pipe_write(message.clone());
            match result {
                Ok(()) => break,
                Err(err) => {
                    tracing::debug!(peer, %addr, error = %err, "peer send failed; reconnecting");
                    stream = None;
                }
            }
        }
        // Drain already queued messages in deterministic FIFO order while the
        // persistent connection is healthy. New messages remain bounded by
        // the SyncSender capacity.
        while let Ok(next) = receiver.try_recv() {
            if stream
                .as_mut()
                .expect("stream remains healthy while draining")
                .pipe_write(next.clone())
                .is_err()
            {
                stream = None;
                // Requeueing is intentionally avoided: the next heartbeat
                // reconstructs the current replication request from state.
                break;
            }
        }
    }
}

trait StreamWritePeer {
    fn pipe_write(&mut self, message: raft_kv::Message) -> io::Result<()>;
}

impl StreamWritePeer for TcpStream {
    fn pipe_write(&mut self, message: raft_kv::Message) -> io::Result<()> {
        write_peer_frame(self, message)
    }
}

fn serve_metrics(addr: SocketAddr, metrics: NodeMetrics) {
    let listener = match TcpListener::bind(addr) {
        Ok(listener) => listener,
        Err(err) => {
            tracing::warn!(%addr, error = %err, "metrics listener disabled");
            return;
        }
    };
    tracing::info!(%addr, "metrics listener started");
    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                if let Err(err) = handle_metrics_connection(&mut stream, &metrics) {
                    tracing::warn!(error = %err, "metrics request failed");
                }
            }
            Err(err) => tracing::warn!(error = %err, "metrics accept failed"),
        }
    }
}

fn handle_metrics_connection(stream: &mut TcpStream, metrics: &NodeMetrics) -> io::Result<()> {
    let mut request = [0; 1024];
    let read = stream.read(&mut request)?;
    let request = String::from_utf8_lossy(&request[..read]);
    let (status, body, content_type) = if request.starts_with("GET /metrics ") {
        (
            "200 OK",
            metrics.render()?,
            "text/plain; version=0.0.4; charset=utf-8",
        )
    } else {
        (
            "404 Not Found",
            "not found\n".to_string(),
            "text/plain; charset=utf-8",
        )
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )?;
    stream.flush()
}

struct Runtime {
    node: Node<LsmTree>,
    started: Instant,
    last_saved: PersistedState,
    metrics: NodeMetrics,
    last_role: Role,
    last_leader_id: Option<NodeId>,
    snapshot_threshold: usize,
}

impl Runtime {
    fn new(node: Node<LsmTree>, metrics: NodeMetrics, snapshot_threshold: usize) -> Self {
        let last_saved = PersistedState::from_node(&node);
        let last_role = node.role();
        let last_leader_id = node.leader_id();
        Self {
            node,
            started: Instant::now(),
            last_saved,
            metrics,
            last_role,
            last_leader_id,
            snapshot_threshold,
        }
    }

    /// Persists if durable state differs from the last saved snapshot.
    /// Compares fields directly — no log allocation on the fast path.
    fn persist_if_changed(&mut self, path: &Path) -> io::Result<()> {
        if self.snapshot_threshold > 0
            && self
                .node
                .last_applied()
                .saturating_sub(self.node.snapshot_index())
                >= self.snapshot_threshold
            && self.node.last_applied() <= self.node.commit_index()
        {
            let index = self.node.last_applied();
            self.node.compact_to(index)?;
        }
        let state = PersistedState::from_node(&self.node);
        let needs_save = state != self.last_saved;
        if needs_save {
            save_node(path, &self.node)?;
            self.last_saved = state;
        }
        Ok(())
    }

    fn refresh_metrics(&mut self) {
        let role = self.node.role();
        if role == Role::Candidate && self.last_role != Role::Candidate {
            self.metrics.inc_elections();
        }
        if self.node.leader_id() != self.last_leader_id && self.node.leader_id().is_some() {
            self.metrics.inc_leader_changes();
        }
        self.last_role = role;
        self.last_leader_id = self.node.leader_id();

        self.metrics.set_raft_state(
            self.node.current_term(),
            self.node.role_label(),
            self.node.commit_index(),
            self.node.last_applied(),
        );
        for (peer, lag) in self.node.replication_lag_by_peer() {
            self.metrics.set_replication_lag(peer, lag);
        }
        self.metrics.set_lsm_state(
            self.node.state_machine().memtable_size_bytes(),
            self.node.state_machine().sstable_count(),
        );
        self.metrics
            .set_lsm_compactions_total(self.node.state_machine().compactions_total());
    }
}

#[derive(Clone, Debug)]
struct Config {
    id: NodeId,
    peers: HashMap<NodeId, String>,
    state_path: PathBuf,
    metrics_addr: Option<SocketAddr>,
    snapshot_threshold: usize,
}

impl Config {
    fn lsm_dir(&self) -> PathBuf {
        let mut path = self.state_path.clone();
        let name = self
            .state_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("node.bin");
        path.set_file_name(format!("{name}.lsm"));
        path
    }

    fn from_args(args: Vec<String>) -> io::Result<Self> {
        if args.len() < 5 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: raft-node <id> <state-file> <id=addr>...",
            ));
        }
        let id = args[1]
            .parse()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid node id"))?;
        let state_path = PathBuf::from(&args[2]);
        let mut peers = HashMap::new();
        for peer in &args[3..] {
            let (id, addr) = peer.split_once('=').ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "peer must be id=addr")
            })?;
            let id = id
                .parse()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid peer id"))?;
            peers.insert(id, addr.to_string());
        }
        let self_addr = peers
            .get(&id)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing self address"))?;
        let metrics_addr = metrics_addr(self_addr)?;
        let snapshot_threshold = env::var("RAFT_KV_SNAPSHOT_THRESHOLD")
            .ok()
            .map(|value| {
                value.parse::<usize>().map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "invalid RAFT_KV_SNAPSHOT_THRESHOLD",
                    )
                })
            })
            .transpose()?
            .unwrap_or(64);
        Ok(Self {
            id,
            peers,
            state_path,
            metrics_addr,
            snapshot_threshold,
        })
    }
}

fn metrics_addr(self_addr: &str) -> io::Result<Option<SocketAddr>> {
    if let Ok(addr) = env::var("RAFT_KV_METRICS_ADDR") {
        return addr.parse().map(Some).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid RAFT_KV_METRICS_ADDR")
        });
    }
    let mut addr: SocketAddr = self_addr
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid self address"))?;
    let Some(port) = addr.port().checked_add(1000) else {
        return Ok(None);
    };
    addr.set_port(port);
    Ok(Some(addr))
}
