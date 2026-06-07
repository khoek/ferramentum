use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use jiff::{Timestamp, tz::TimeZone};

pub trait UnixTimestampFormat {
    fn format_full(self) -> String;
    fn format_compact(self) -> String;
    fn age(self) -> String;
}

impl UnixTimestampFormat for u64 {
    fn format_full(self) -> String {
        format_timestamp(self, "%F %T %Z")
    }

    fn format_compact(self) -> String {
        format_timestamp(self, "%m-%d %H:%M")
    }

    fn age(self) -> String {
        let now = unix_timestamp();
        if self == now {
            "now".to_owned()
        } else if self > now {
            format!("in {}", human_duration(self - now))
        } else {
            format!("{} ago", human_duration(now - self))
        }
    }
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn format_unix_time(timestamp: u64) -> String {
    timestamp.format_full()
}

pub fn format_unix_time_compact(timestamp: u64) -> String {
    timestamp.format_compact()
}

pub fn event_age(timestamp: u64) -> String {
    timestamp.age()
}

pub fn system_time_to_unix(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
}

pub fn file_modified_unix(path: &Path) -> Option<u64> {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(system_time_to_unix)
}

pub fn human_duration(seconds: u64) -> String {
    match seconds {
        0 => "now".to_owned(),
        1..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m{}s", seconds / 60, seconds % 60),
        _ => format!("{}h{}m", seconds / 3600, seconds % 3600 / 60),
    }
}

fn format_timestamp(timestamp: u64, format: &str) -> String {
    let Ok(second) = i64::try_from(timestamp) else {
        return timestamp.to_string();
    };
    Timestamp::from_second(second)
        .map(|timestamp| {
            timestamp
                .to_zoned(TimeZone::system())
                .strftime(format)
                .to_string()
        })
        .unwrap_or_else(|_| timestamp.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_short_durations() {
        assert_eq!(human_duration(0), "now");
        assert_eq!(human_duration(59), "59s");
        assert_eq!(human_duration(61), "1m1s");
        assert_eq!(human_duration(3661), "1h1m");
    }
}
