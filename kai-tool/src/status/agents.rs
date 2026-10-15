//! Counts refer to currently open, attributable subagent logs, including descendants.
use super::transcript::{TurnState, View};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(super) struct Summary {
    pub total: usize,
    pub running: usize,
    pub ready: usize,
    pub interrupted: usize,
    pub error: usize,
    pub unknown: usize,
    pub complete: bool,
}

impl Summary {
    pub(super) fn label(&self) -> String {
        format!(
            "{} active{}",
            self.running,
            if self.complete && self.unknown == 0 {
                ""
            } else {
                " ?"
            }
        )
    }

    pub(super) fn description(&self) -> String {
        let mut parts = vec![format!("{} running", self.running)];
        for (count, label) in [
            (self.ready, "ready"),
            (self.interrupted, "interrupted"),
            (self.error, "error"),
            (self.unknown, "unknown"),
        ] {
            if count > 0 {
                parts.push(format!("{count} {label}"));
            }
        }
        if !self.complete {
            parts.push("some agent logs could not be associated".into());
        }
        parts.join(" · ")
    }
}

pub(super) fn summarize(root: &str, children: Vec<View>, complete: bool) -> Summary {
    let mut summary = Summary {
        complete,
        ..Summary::default()
    };
    let mut by_id = HashMap::<String, View>::new();
    for child in children {
        if let Some(previous) = by_id.get_mut(&child.id) {
            if previous.parent_id != child.parent_id
                || previous.root_id != child.root_id
                || previous.state != child.state
            {
                previous.state = TurnState::Unknown;
                summary.complete = false;
            }
        } else {
            by_id.insert(child.id.clone(), child);
        }
    }
    for child in by_id.values() {
        match belongs_to(child, root, &by_id) {
            Some(true) => (),
            Some(false) => continue,
            None => {
                summary.complete = false;
                continue;
            }
        }
        summary.total += 1;
        match child.state {
            TurnState::Working => summary.running += 1,
            TurnState::Ready => summary.ready += 1,
            TurnState::Interrupted => summary.interrupted += 1,
            TurnState::Error => summary.error += 1,
            TurnState::Unknown | TurnState::NeedsInput => summary.unknown += 1,
        }
    }
    summary
}

fn belongs_to<'a>(
    mut child: &'a View,
    root: &str,
    children: &'a HashMap<String, View>,
) -> Option<bool> {
    // session_id identifies the root even if an intermediate parent's log closed.
    // Older builds need the parent chain. The bound rejects cycles without guessing.
    for _ in 0..=children.len() {
        if let Some(root_id) = &child.root_id {
            return Some(root_id == root);
        }
        let parent = child.parent_id.as_deref()?;
        if parent == root {
            return Some(true);
        }
        child = children.get(parent)?;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child(id: &str, parent: &str, state: TurnState) -> View {
        View {
            id: id.into(),
            parent_id: Some(parent.into()),
            state,
            ..View::default()
        }
    }

    #[test]
    fn counts_nested_agents_once_and_does_not_require_the_parent_to_be_running() {
        let a = child("a", "root", TurnState::Ready);
        let b = child("b", "a", TurnState::Working);
        let mut other = child("other", "elsewhere", TurnState::Working);
        other.root_id = Some("another-root".into());
        let summary = summarize(
            "root",
            vec![
                a.clone(),
                a,
                b.clone(),
                b,
                other,
                child("c", "root", TurnState::Interrupted),
                child("d", "root", TurnState::Error),
            ],
            true,
        );
        assert_eq!(
            summary,
            Summary {
                total: 4,
                running: 1,
                ready: 1,
                interrupted: 1,
                error: 1,
                complete: true,
                ..Summary::default()
            }
        );
    }

    #[test]
    fn uses_root_metadata_and_reports_unknown_or_unattributable_agents() {
        let mut nested = child("nested", "closed-parent", TurnState::Working);
        nested.root_id = Some("root".into());
        let summary = summarize(
            "root",
            vec![
                nested,
                child("unknown", "root", TurnState::Unknown),
                child("orphan", "missing", TurnState::Working),
                child("cycle-a", "cycle-b", TurnState::Working),
                child("cycle-b", "cycle-a", TurnState::Working),
            ],
            true,
        );
        assert_eq!(summary.running, 1);
        assert_eq!(summary.unknown, 1);
        assert_eq!(summary.total, 2);
        assert!(!summary.complete);
        assert_eq!(summary.label(), "1 active ?");
    }
}
