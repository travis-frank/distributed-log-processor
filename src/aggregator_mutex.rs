// Intentionally uses a global Mutex<HashMap> instead of DashMap.
// Every write acquires the same lock, under 4+ threads all workers queue here.
// Used to benchmark lock contention vs DashMap sharding.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::message::{AccountStats, EntryType, LogEntry};

pub struct MutexAggregator {
    stats: Mutex<HashMap<String, AccountStats>>,
}

impl MutexAggregator {
    pub fn new() -> Self {
        Self {
            stats: Mutex::new(HashMap::new()),
        }
    }

    pub fn process(&self, entry: &LogEntry) {
        let mut stats = self.stats.lock().expect("mutex poisoned");
        let account_stats = stats.entry(entry.account_id.clone()).or_default();
        account_stats.transaction_count += 1;

        match entry.entry_type {
            EntryType::Deposit => {
                account_stats.total_deposits += entry.amount;
            }
            EntryType::Withdrawal => {
                account_stats.total_withdrawals += entry.amount;
            }
            EntryType::Transfer | EntryType::Fee => {}
        }
    }

    pub fn account_count(&self) -> usize {
        let stats = self.stats.lock().expect("mutex poisoned");
        stats.len()
    }
}
