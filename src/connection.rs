//! Connection management for TWS communication

use time::OffsetDateTime;
use time_tz::Tz;

pub mod common;

pub use common::StartupMessage;

/// The accounts of a `managedAccounts` message: its comma-separated list,
/// without empty entries or surrounding blanks.
pub(crate) fn split_accounts(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|account| !account.is_empty())
        .map(str::to_owned)
        .collect()
}

/// Metadata about the connection to TWS
#[derive(Default, Clone, Debug)]
pub struct ConnectionMetadata {
    /// Next order ID to use for placing orders
    pub next_order_id: i32,
    /// Client ID for this connection
    pub client_id: i32,
    /// Server version (TWS version)
    pub server_version: i32,
    /// Comma-separated list of managed accounts
    pub managed_accounts: String,
    /// Connection time
    pub connection_time: Option<OffsetDateTime>,
    /// Server time zone
    pub time_zone: Option<&'static Tz>,
}

#[cfg(feature = "sync")]
pub mod sync;

#[cfg(feature = "async")]
pub mod r#async;

#[cfg(test)]
mod split_accounts_tests {
    use super::split_accounts;

    #[test]
    fn the_list_drops_blanks_and_empty_entries_and_keeps_the_order() {
        assert_eq!(split_accounts(" DU2, ,DU1,"), ["DU2", "DU1"]);
        assert!(split_accounts("").is_empty());
        assert!(split_accounts(" , ").is_empty());
    }
}
