use dashmap::DashMap;

use crate::message::{AccountStats, EntryType, LogEntry};

pub struct Aggregator {
    stats: DashMap<String, AccountStats>,
}

impl Aggregator {
    pub fn new() -> Self {
        Self {
            stats: DashMap::new(),
        }
    }

    pub fn process(&self, entry: &LogEntry) {
        let mut stats = self.stats.entry(entry.account_id.clone()).or_default();
        stats.transaction_count += 1;

        match &entry.entry_type {
            EntryType::Deposit => {
                stats.total_deposits += entry.amount;
            }
            EntryType::Withdrawal => {
                stats.total_withdrawals += entry.amount;
            }
            EntryType::Transfer | EntryType::Fee => {}
        }
    }

    pub fn account_count(&self) -> usize {
        self.stats.len()
    }

    pub fn get_stats(&self, account_id: &str) -> Option<AccountStats> {
        self.stats.get(account_id).map(|stats| stats.clone())
    }
}
