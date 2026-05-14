use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct LogEntry {
    pub timestamp: DateTime<Utc>,
    pub account_id: String,
    pub amount: f64,
    #[serde(rename = "type")]
    pub entry_type: EntryType,
    pub currency: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EntryType {
    Deposit,
    Withdrawal,
    Transfer,
    Fee,
}

#[derive(Debug, Default, Clone)]
pub struct AccountStats {
    pub total_deposits: f64,
    pub total_withdrawals: f64,
    pub transaction_count: u64,
}
