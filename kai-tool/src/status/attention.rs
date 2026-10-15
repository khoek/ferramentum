//! Dashboard-local unread completions. Opening a dashboard establishes a baseline;
//! it must not invent an interaction history for already-ready or resumed threads.
use super::{
    observer::Row,
    process::ProcessIdentity,
    transcript::{Completion, TurnState},
};
use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[derive(Clone)]
pub(super) struct Target {
    pub identity: ProcessIdentity,
    #[cfg(target_os = "macos")]
    pub tty: Option<String>,
}

impl From<&Row> for Target {
    fn from(row: &Row) -> Self {
        Self {
            identity: row.identity,
            #[cfg(target_os = "macos")]
            tty: row.tty.clone(),
        }
    }
}

pub(super) struct Focused {
    // Taken before the desktop query, so a slow response cannot acknowledge a newer turn.
    pub at_ms: u64,
    pub identities: Vec<ProcessIdentity>,
}

struct Entry {
    established: bool,
    thread: Option<String>,
    completion: Option<Completion>,
    state: TurnState,
    unseen_since: Option<u64>,
    focused_at: Option<u64>,
}

impl Entry {
    fn baseline(row: &Row) -> Self {
        Self {
            established: row.state != TurnState::Unknown,
            thread: row.thread_id.clone(),
            completion: row.completion.clone(),
            state: row.state,
            unseen_since: None,
            focused_at: None,
        }
    }
}

#[derive(Default)]
pub(super) struct Attention {
    entries: HashMap<ProcessIdentity, Entry>,
}

impl Attention {
    pub(super) fn observe(&mut self, rows: &[Row], observed_at: u64) {
        self.entries
            .retain(|identity, _| rows.iter().any(|row| row.identity == *identity));
        for row in rows {
            let entry = self
                .entries
                .entry(row.identity)
                .or_insert_with(|| Entry::baseline(row));
            if row.thread_id != entry.thread || !entry.established {
                *entry = Entry::baseline(row);
                continue;
            }
            if row.state == TurnState::Ready {
                let changed = match (&row.completion, &entry.completion) {
                    (Some(new), Some(previous)) => match (&new.turn_id, &previous.turn_id) {
                        (Some(new_id), Some(previous_id)) => new_id != previous_id,
                        _ => new != previous,
                    },
                    (Some(_), None) => true,
                    _ => false,
                };
                let completed = changed
                    || (row.completion.is_none()
                        && matches!(entry.state, TurnState::Working | TurnState::NeedsInput));
                if completed {
                    let at = row
                        .completion
                        .as_ref()
                        .and_then(|value| value.at_ms)
                        .and_then(|value| u64::try_from(value).ok())
                        .unwrap_or(observed_at)
                        .min(observed_at);
                    entry.unseen_since = Some(at);
                }
            } else if row.state != TurnState::Unknown {
                entry.unseen_since = None;
            }
            // An unread completion survives temporary discovery failures, but is hidden
            // while the row's state is unknown. Duplicate log records cannot relight it.
            if row.completion.is_some() {
                entry.completion.clone_from(&row.completion);
            }
            entry.state = row.state;
            if entry
                .focused_at
                .zip(entry.unseen_since)
                .is_some_and(|(focus, end)| focus >= end)
            {
                entry.unseen_since = None;
            }
        }
    }

    pub(super) fn focused(&mut self, update: Focused) {
        for identity in update.identities {
            if let Some(entry) = self.entries.get_mut(&identity) {
                entry.focused_at = Some(entry.focused_at.unwrap_or_default().max(update.at_ms));
                if entry.unseen_since.is_some_and(|end| update.at_ms >= end) {
                    entry.unseen_since = None;
                }
            }
        }
    }

    pub(super) fn acknowledge(&mut self, identity: ProcessIdentity, at_ms: u64) {
        self.focused(Focused {
            at_ms,
            identities: vec![identity],
        });
    }

    /// Mark only the completion already displayed. A newer completion discovered
    /// on the next refresh must not inherit a manual acknowledgement of this one.
    pub(super) fn mark_read(&mut self, identity: ProcessIdentity) {
        if let Some(entry) = self.entries.get_mut(&identity) {
            entry.unseen_since = None;
        }
    }

    pub(super) fn unseen(&self, row: &Row) -> bool {
        row.state == TurnState::Ready
            && self
                .entries
                .get(&row.identity)
                .is_some_and(|entry| entry.unseen_since.is_some())
    }
}

#[cfg(test)]
#[path = "attention_tests.rs"]
mod tests;
