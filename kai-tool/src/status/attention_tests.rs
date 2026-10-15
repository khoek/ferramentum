use super::*;

fn row(state: TurnState, turn: &str, at_ms: i64) -> Row {
    Row {
        identity: ProcessIdentity {
            pid: 10,
            start_ticks: 100,
        },
        pid: 10,
        tty: None,
        cwd: None,
        thread_id: Some("main".into()),
        thread_name: None,
        run_time_ms: None,
        state,
        agents: None,
        token_usage: None,
        last_finished_at: Some(at_ms / 1000),
        input_requested_at: None,
        exited_at: None,
        detail: None,
        completion: Some(Completion {
            turn_id: Some(turn.into()),
            at_ms: Some(at_ms),
        }),
    }
}

fn observe(attention: &mut Attention, row: &Row, at: u64) {
    attention.observe(std::slice::from_ref(row), at);
}

#[test]
fn old_ready_sessions_are_a_baseline_and_only_new_completions_get_a_marker() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Ready, "old", 1000);
    observe(&mut attention, &row, 2000);
    assert!(!attention.unseen(&row));
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: Some(2100),
    });
    observe(&mut attention, &row, 2200); // Entire turn occurred between dashboard refreshes.
    assert!(attention.unseen(&row));
    attention.acknowledge(row.identity, 2300);
    observe(&mut attention, &row, 2400);
    assert!(!attention.unseen(&row)); // Duplicate records and refreshes cannot relight it.
    row.completion.as_mut().unwrap().at_ms = Some(2400);
    observe(&mut attention, &row, 2400);
    assert!(!attention.unseen(&row)); // Same turn re-emitted with a later log timestamp.
    row.completion = Some(Completion {
        turn_id: Some("fast".into()),
        at_ms: Some(2401),
    });
    observe(&mut attention, &row, 2500);
    assert!(attention.unseen(&row)); // Different completion within the same second.
}

#[test]
fn initial_unknown_is_not_an_activity_baseline() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Unknown, "older", 500);
    observe(&mut attention, &row, 2000);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("old".into()),
        at_ms: Some(1000),
    });
    observe(&mut attention, &row, 2100);
    assert!(!attention.unseen(&row));
    row.state = TurnState::Working;
    observe(&mut attention, &row, 2200);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: Some(2300),
    });
    observe(&mut attention, &row, 2400);
    assert!(attention.unseen(&row));
}

#[test]
fn focus_before_a_finish_does_not_acknowledge_it_but_a_visit_before_discovery_does() {
    for (focus_at, unread) in [(2099, true), (2101, false)] {
        let mut attention = Attention::default();
        let mut row = row(TurnState::Working, "old", 1000);
        observe(&mut attention, &row, 2000);
        attention.focused(Focused {
            at_ms: focus_at,
            identities: vec![row.identity],
        });
        row.state = TurnState::Ready;
        row.completion = Some(Completion {
            turn_id: Some("new".into()),
            at_ms: Some(2100),
        });
        observe(&mut attention, &row, 2200);
        assert_eq!(attention.unseen(&row), unread);
        attention.focused(Focused {
            at_ms: 2102,
            identities: vec![row.identity],
        });
        assert!(!attention.unseen(&row));
    }
}

#[test]
fn stale_focus_results_do_not_acknowledge_a_later_completion() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Working, "old", 1000);
    observe(&mut attention, &row, 2000);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: Some(2500),
    });
    observe(&mut attention, &row, 3000);
    attention.focused(Focused {
        at_ms: 2400,
        identities: vec![row.identity],
    });
    assert!(attention.unseen(&row));
}

#[test]
fn temporary_unknown_hides_but_preserves_unread_and_new_work_clears_it() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Working, "old", 1000);
    observe(&mut attention, &row, 2000);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: Some(2100),
    });
    observe(&mut attention, &row, 2200);
    assert!(attention.unseen(&row));
    let completion = row.completion.take();
    row.state = TurnState::Unknown;
    observe(&mut attention, &row, 2300);
    assert!(!attention.unseen(&row));
    row.completion = completion;
    row.state = TurnState::Ready;
    observe(&mut attention, &row, 2400);
    assert!(attention.unseen(&row));
    row.state = TurnState::Working;
    observe(&mut attention, &row, 2500);
    assert!(!attention.unseen(&row));
    for state in [
        TurnState::Interrupted,
        TurnState::Error,
        TurnState::NeedsInput,
    ] {
        row.state = state;
        observe(&mut attention, &row, 2600);
        assert!(!attention.unseen(&row));
    }
}

#[test]
fn thread_switches_pid_reuse_and_closed_windows_reset_tracking() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Working, "old", 1000);
    observe(&mut attention, &row, 2000);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: Some(2100),
    });
    observe(&mut attention, &row, 2200);
    assert!(attention.unseen(&row));
    row.thread_id = Some("resumed".into());
    observe(&mut attention, &row, 2300);
    assert!(!attention.unseen(&row));
    row.identity.start_ticks += 1;
    observe(&mut attention, &row, 2400);
    assert!(!attention.unseen(&row));
    assert_eq!(attention.entries.len(), 1);
    attention.observe(&[], 2500);
    assert!(attention.entries.is_empty());
}

#[test]
fn no_timestamp_requires_a_visit_after_discovery() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Working, "old", 1000);
    observe(&mut attention, &row, 2000);
    attention.acknowledge(row.identity, 2100);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("new".into()),
        at_ms: None,
    });
    observe(&mut attention, &row, 2200);
    assert!(attention.unseen(&row));
    attention.acknowledge(row.identity, 2300);
    assert!(!attention.unseen(&row));
}

#[test]
fn manual_mark_read_does_not_acknowledge_a_completion_that_has_not_been_displayed() {
    let mut attention = Attention::default();
    let mut row = row(TurnState::Working, "old", 1000);
    observe(&mut attention, &row, 2000);
    row.state = TurnState::Ready;
    row.completion = Some(Completion {
        turn_id: Some("first".into()),
        at_ms: Some(2100),
    });
    observe(&mut attention, &row, 2200);
    attention.mark_read(row.identity);
    assert!(!attention.unseen(&row));
    row.completion = Some(Completion {
        turn_id: Some("second".into()),
        at_ms: Some(2201),
    });
    observe(&mut attention, &row, 2300);
    assert!(attention.unseen(&row));
}
