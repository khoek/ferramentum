use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, TransactionBehavior, params};

const STATE_DATABASE: &str = "state_5.sqlite";

/// Repairs rollout paths left by the legacy temporary-home supervisor.
///
/// Older Kai versions put sessions below a disposable `.agent-*` home while storing absolute
/// paths in Codex's state database. New supervised runs keep the canonical `CODEX_HOME`, but this
/// startup repair retargets rows whose persistent rollout still exists, while leaving ambiguous or
/// missing files untouched.
pub(crate) fn normalize_rollout_paths(
    sqlite_home: &Path,
    credentials_home: &Path,
    codex_home: &Path,
) -> Result<usize> {
    let database = sqlite_home.join(STATE_DATABASE);
    if !database.is_file() {
        return Ok(0);
    }

    let mut connection = Connection::open(&database)
        .with_context(|| format!("could not open Codex state database {}", database.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .context("could not configure Codex state database lock timeout")?;
    let has_threads: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'threads')",
            [],
            |row| row.get(0),
        )
        .context("could not inspect Codex state database schema")?;
    if !has_threads {
        return Ok(0);
    }

    let mut updates = Vec::new();
    {
        let mut query = connection
            .prepare("SELECT id, rollout_path FROM threads")
            .context("could not inspect Codex rollout paths")?;
        let rows = query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .context("could not read Codex rollout paths")?;
        for row in rows {
            let (thread_id, rollout_path) = row.context("could not decode Codex rollout path")?;
            let replacement =
                stale_supervised_rollout_path(&rollout_path, credentials_home, codex_home);
            if let Some(replacement) = replacement
                .filter(|path| path != &rollout_path)
                .filter(|path| Path::new(path).is_file())
            {
                updates.push((thread_id, rollout_path, replacement));
            }
        }
    }

    if updates.is_empty() {
        return Ok(0);
    }

    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .with_context(|| {
            format!(
                "could not begin Codex state database update {}",
                database.display()
            )
        })?;
    let mut changed = 0;
    for (thread_id, previous_path, rollout_path) in updates {
        let updated = transaction
            .execute(
                "UPDATE threads SET rollout_path = ?1 WHERE id = ?2 AND rollout_path = ?3",
                params![rollout_path, thread_id, previous_path],
            )
            .context("could not persist normalized Codex rollout path")?;
        changed += updated;
    }
    transaction
        .commit()
        .context("could not commit normalized Codex rollout paths")?;
    Ok(changed)
}

fn stale_supervised_rollout_path(
    path: &str,
    credentials_home: &Path,
    codex_home: &Path,
) -> Option<String> {
    let relative = Path::new(path).strip_prefix(credentials_home).ok()?;
    let mut components = relative.components();
    let Component::Normal(name) = components.next()? else {
        return None;
    };
    if !name.to_string_lossy().starts_with(".agent-") {
        return None;
    }
    if components.clone().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return None;
    }
    let remainder = components.collect::<PathBuf>();
    if remainder.as_os_str().is_empty() {
        return None;
    }
    codex_home.join(remainder).to_str().map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rusqlite::Connection;
    use tempfile::tempdir;

    use super::*;

    fn database(root: &Path) -> PathBuf {
        let sqlite_home = root.join("sqlite");
        fs::create_dir_all(&sqlite_home).unwrap();
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE threads (id TEXT PRIMARY KEY, rollout_path TEXT NOT NULL);",
            )
            .unwrap();
        connection.close().unwrap();
        sqlite_home
    }

    #[test]
    fn normalizes_stale_supervised_prefixes() {
        let root = tempdir().unwrap();
        let sqlite_home = database(root.path());
        let credentials_home = root.path().join("credentials");
        let codex_home = root.path().join("codex");
        fs::create_dir_all(codex_home.join("sessions")).unwrap();
        fs::write(codex_home.join("sessions/stale.jsonl"), "session").unwrap();
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, ?2)",
                params![
                    "stale",
                    credentials_home
                        .join(".agent-old/sessions/stale.jsonl")
                        .to_str()
                        .unwrap()
                ],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, ?2)",
                params!["other", "/somewhere/else/sessions/other.jsonl"],
            )
            .unwrap();
        connection.close().unwrap();

        assert_eq!(
            normalize_rollout_paths(&sqlite_home, &credentials_home, &codex_home,).unwrap(),
            1
        );

        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        let rows = connection
            .prepare("SELECT id, rollout_path FROM threads ORDER BY id")
            .unwrap()
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "other".to_owned(),
                    "/somewhere/else/sessions/other.jsonl".to_owned()
                ),
                (
                    "stale".to_owned(),
                    codex_home
                        .join("sessions/stale.jsonl")
                        .to_str()
                        .unwrap()
                        .to_owned()
                ),
            ]
        );
    }

    #[test]
    fn missing_state_database_is_a_noop() {
        let root = tempdir().unwrap();
        assert_eq!(
            normalize_rollout_paths(
                &root.path().join("sqlite"),
                &root.path().join("credentials"),
                &root.path().join("codex"),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn state_database_without_threads_table_is_a_noop() {
        let root = tempdir().unwrap();
        let sqlite_home = root.path().join("sqlite");
        fs::create_dir_all(&sqlite_home).unwrap();
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        connection
            .execute_batch("CREATE TABLE unrelated (value TEXT NOT NULL);")
            .unwrap();
        connection.close().unwrap();

        assert_eq!(
            normalize_rollout_paths(
                &sqlite_home,
                &root.path().join("credentials"),
                &root.path().join("codex"),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn ignores_non_agent_temporary_directories() {
        let root = tempdir().unwrap();
        let sqlite_home = database(root.path());
        let credentials_home = root.path().join("credentials");
        let codex_home = root.path().join("codex");
        let path = credentials_home.join(".quota-current/sessions/x.jsonl");
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, ?2)",
                params!["thread", path.to_str().unwrap()],
            )
            .unwrap();
        connection.close().unwrap();

        assert_eq!(
            normalize_rollout_paths(&sqlite_home, &credentials_home, &codex_home).unwrap(),
            0
        );
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        let path: String = connection
            .query_row("SELECT rollout_path FROM threads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            path,
            credentials_home
                .join(".quota-current/sessions/x.jsonl")
                .to_str()
                .unwrap()
        );
    }

    #[test]
    fn leaves_stale_path_untouched_when_no_persistent_rollout_exists() {
        let root = tempdir().unwrap();
        let sqlite_home = database(root.path());
        let credentials_home = root.path().join("credentials");
        let codex_home = root.path().join("codex");
        let stale_path = credentials_home.join(".agent-old/sessions/missing.jsonl");
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        connection
            .execute(
                "INSERT INTO threads VALUES (?1, ?2)",
                params!["thread", stale_path.to_str().unwrap()],
            )
            .unwrap();
        connection.close().unwrap();

        assert_eq!(
            normalize_rollout_paths(&sqlite_home, &credentials_home, &codex_home).unwrap(),
            0
        );
        let connection = Connection::open(sqlite_home.join(STATE_DATABASE)).unwrap();
        let path: String = connection
            .query_row("SELECT rollout_path FROM threads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(path, stale_path.to_str().unwrap());
    }
}
