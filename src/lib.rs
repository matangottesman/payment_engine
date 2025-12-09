use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use tracing::warn;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("failed to read csv: {0}")]
    Csv(#[from] csv::Error),
    #[error("failed to open input {path:?}")]
    OpenFile {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Deserialize)]
struct RawRecord {
    #[serde(rename = "type")]
    kind: String,
    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TxType {
    fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_lowercase().as_str() {
            "deposit" => Some(Self::Deposit),
            "withdrawal" => Some(Self::Withdrawal),
            "dispute" => Some(Self::Dispute),
            "resolve" => Some(Self::Resolve),
            "chargeback" => Some(Self::Chargeback),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct TransactionInput {
    client: u16,
    tx: u32,
    amount: Option<Decimal>,
}

#[derive(Debug, Clone)]
struct Account {
    available: Decimal,
    held: Decimal,
    locked: bool,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            locked: false,
        }
    }
}

impl Account {
    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostedKind {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Clone)]
struct StoredTransaction {
    client: u16,
    amount: Decimal,
    kind: PostedKind,
    disputed: bool,
    charged_back: bool,
}

#[derive(Default)]
pub struct Engine {
    accounts: HashMap<u16, Account>,
    transactions: HashMap<u32, StoredTransaction>,
}

impl Engine {
    pub fn process_reader<R: Read>(&mut self, reader: R) -> Result<(), EngineError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_reader(reader);

        for (row, record) in csv_reader.deserialize::<RawRecord>().enumerate() {
            match record {
                Ok(raw) => self.process_record(raw),
                Err(err) => warn!(row = row + 1, error = %err, "Skipping malformed row"),
            }
        }

        Ok(())
    }

    pub fn write_accounts<W: Write>(&self, writer: W) -> Result<(), EngineError> {
        #[derive(serde::Serialize)]
        struct AccountRow {
            client: u16,
            available: String,
            held: String,
            total: String,
            locked: bool,
        }

        let mut rows: Vec<_> = self.accounts.iter().collect();
        rows.sort_by_key(|(client, _)| **client);

        let mut csv_writer = csv::Writer::from_writer(writer);
        for (client, account) in rows {
            let row = AccountRow {
                client: *client,
                available: format_decimal(account.available),
                held: format_decimal(account.held),
                total: format_decimal(account.total()),
                locked: account.locked,
            };
            csv_writer.serialize(row)?;
        }
        csv_writer.flush()?;
        Ok(())
    }

    fn process_record(&mut self, raw: RawRecord) {
        let Some(kind) = TxType::from_str(&raw.kind) else {
            warn!(tx = raw.tx, kind = raw.kind, "Unknown transaction type");
            return;
        };

        let tx = TransactionInput {
            client: raw.client,
            tx: raw.tx,
            amount: raw.amount,
        };

        match kind {
            TxType::Deposit => self.deposit(tx),
            TxType::Withdrawal => self.withdraw(tx),
            TxType::Dispute => self.dispute(tx),
            TxType::Resolve => self.resolve(tx),
            TxType::Chargeback => self.chargeback(tx),
        }
    }

    fn deposit(&mut self, tx: TransactionInput) {
        let Some(amount) = tx.amount else {
            warn!(tx = tx.tx, "Deposit missing amount");
            return;
        };

        if self.transactions.contains_key(&tx.tx) {
            warn!(tx = tx.tx, "Duplicate transaction id ignored");
            return;
        }

        let account = self.accounts.entry(tx.client).or_default();
        if account.locked {
            return;
        }

        account.available += amount;
        self.transactions.insert(
            tx.tx,
            StoredTransaction {
                client: tx.client,
                amount,
                kind: PostedKind::Deposit,
                disputed: false,
                charged_back: false,
            },
        );
    }

    fn withdraw(&mut self, tx: TransactionInput) {
        let Some(amount) = tx.amount else {
            warn!(tx = tx.tx, "Withdrawal missing amount");
            return;
        };

        if self.transactions.contains_key(&tx.tx) {
            warn!(tx = tx.tx, "Duplicate transaction id ignored");
            return;
        }

        let account = self.accounts.entry(tx.client).or_default();
        if account.locked {
            return;
        }

        if account.available < amount {
            return;
        }

        account.available -= amount;
        self.transactions.insert(
            tx.tx,
            StoredTransaction {
                client: tx.client,
                amount,
                kind: PostedKind::Withdrawal,
                disputed: false,
                charged_back: false,
            },
        );
    }

    fn dispute(&mut self, tx: TransactionInput) {
        let Some(stored) = self.transactions.get_mut(&tx.tx) else {
            return;
        };
        if stored.client != tx.client || stored.kind != PostedKind::Deposit || stored.disputed {
            return;
        }

        let Some(account) = self.accounts.get_mut(&tx.client) else {
            return;
        };
        if account.locked {
            return;
        }

        account.available -= stored.amount;
        account.held += stored.amount;
        stored.disputed = true;
    }

    fn resolve(&mut self, tx: TransactionInput) {
        let Some(stored) = self.transactions.get_mut(&tx.tx) else {
            return;
        };
        if stored.client != tx.client || stored.kind != PostedKind::Deposit || !stored.disputed || stored.charged_back {
            return;
        }

        let Some(account) = self.accounts.get_mut(&tx.client) else {
            return;
        };
        if account.locked {
            return;
        }

        account.held -= stored.amount;
        account.available += stored.amount;
        stored.disputed = false;
    }

    fn chargeback(&mut self, tx: TransactionInput) {
        let Some(stored) = self.transactions.get_mut(&tx.tx) else {
            return;
        };
        if stored.client != tx.client || stored.kind != PostedKind::Deposit || !stored.disputed || stored.charged_back {
            return;
        }

        let Some(account) = self.accounts.get_mut(&tx.client) else {
            return;
        };
        if account.locked {
            return;
        }

        account.held -= stored.amount;
        account.locked = true;
        stored.disputed = false;
        stored.charged_back = true;
    }
}

fn format_decimal(value: Decimal) -> String {
    format!("{:.4}", value.round_dp(4))
}

pub fn run_from_reader<R: Read, W: Write>(reader: R, writer: W) -> Result<(), EngineError> {
    let mut engine = Engine::default();
    engine.process_reader(reader)?;
    engine.write_accounts(writer)
}

pub fn run_from_path<P: AsRef<Path>>(path: P) -> Result<(), EngineError> {
    let file = File::open(path.as_ref()).map_err(|source| EngineError::OpenFile {
        path: path.as_ref().to_path_buf(),
        source,
    })?;
    run_from_reader(file, io::stdout())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    fn raw(kind: &str, client: u16, tx: u32, amount: Option<&str>) -> RawRecord {
        RawRecord {
            kind: kind.to_string(),
            client,
            tx,
            amount: amount.map(|v| Decimal::from_str(v).unwrap()),
        }
    }

    #[test]
    fn deposit_and_withdraw() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("1.0")));
        engine.process_record(raw("withdrawal", 1, 2, Some("0.5")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(5, 1));
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    #[test]
    fn dispute_and_resolve_cycle() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("resolve", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(20, 1));
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    #[test]
    fn chargeback_locks_account() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("3.5")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("chargeback", 1, 1, None));
        engine.process_record(raw("deposit", 1, 2, Some("1.0")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn skips_insufficient_withdrawal() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("1.0")));
        engine.process_record(raw("withdrawal", 1, 2, Some("2.0")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::new(10, 1));
    }

    #[test]
    fn writes_csv_output() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 2, 1, Some("1.2")));

        let mut buffer = Vec::new();
        engine.write_accounts(&mut buffer).unwrap();

        let output = String::from_utf8(buffer).unwrap();
        assert!(output.contains("client,available,held,total,locked"));
        assert!(output.contains("2,1.2000,0.0000,1.2000,false"));
    }
}
