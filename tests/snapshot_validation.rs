use raft_kv::{SNAPSHOT_FORMAT_VERSION, Snapshot, StateSnapshot};

fn valid_snapshot() -> Snapshot {
    Snapshot {
        version: SNAPSHOT_FORMAT_VERSION,
        last_included_index: 2,
        last_included_term: 1,
        state: StateSnapshot {
            version: SNAPSHOT_FORMAT_VERSION,
            last_applied: 2,
            data: vec![("a".to_string(), "1".to_string()), ("b".to_string(), "2".to_string())],
        },
    }
}

fn assert_invalid_data(snapshot: Snapshot) {
    let error = snapshot
        .validate()
        .expect_err("malformed snapshot must be rejected");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn valid_snapshot_passes_validation() {
    assert!(valid_snapshot().validate().is_ok());
}

#[test]
fn snapshot_version_mismatch_is_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.version = SNAPSHOT_FORMAT_VERSION + 1;
    assert_invalid_data(snapshot);
}

#[test]
fn state_version_mismatch_is_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.state.version = SNAPSHOT_FORMAT_VERSION + 1;
    assert_invalid_data(snapshot);
}

#[test]
fn zero_boundary_snapshot_is_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.last_included_index = 0;
    snapshot.state.last_applied = 0;
    assert_invalid_data(snapshot);
}

#[test]
fn state_index_mismatch_is_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.state.last_applied = snapshot.last_included_index + 1;
    assert_invalid_data(snapshot);
}

#[test]
fn unsorted_keys_are_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.state.data = vec![
        ("b".to_string(), "2".to_string()),
        ("a".to_string(), "1".to_string()),
    ];
    assert_invalid_data(snapshot);
}

#[test]
fn duplicate_keys_are_rejected() {
    let mut snapshot = valid_snapshot();
    snapshot.state.data = vec![
        ("a".to_string(), "1".to_string()),
        ("a".to_string(), "2".to_string()),
    ];
    assert_invalid_data(snapshot);
}
