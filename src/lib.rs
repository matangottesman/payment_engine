use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
};

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tracing::warn;

type ClientId = u16;
type TransactionId = u32;

#[derive(Default)]
pub struct Engine {
    accounts: HashMap<ClientId, Account>,
}

#[derive(Debug, Clone, Default)]
struct Account {
    available: Decimal,
    held: Decimal,
    locked: bool,
    transactions: HashMap<TransactionId, Transaction>,
}

#[derive(Debug, Clone)]
enum Transaction {
    Deposit { amount: Decimal, state: TransactionState },
    // Based on spec wording, assuming that withdrawals cannot be disputed.
    Withdrawal { amount: Decimal },
}

#[derive(Debug, Clone)]
enum TransactionState {
    Normal,
    Disputed,
    Resolved,
    ChargedBack,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("failed to read csv: {0}")]
    Csv(#[from] csv::Error),
    #[error("failed to open input {path:?}")]
    OpenFile {
        path: PathBuf,
        #[source]
        file_error: io::Error,
    },
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
enum InputTransaction {
    Deposit(InputFundTransaction),
    Withdrawal(InputFundTransaction),
    Dispute(InputDepositAlterTransaction),
    Resolve(InputDepositAlterTransaction),
    Chargeback(InputDepositAlterTransaction),
}

#[derive(Debug, Deserialize)]
struct InputFundTransaction {
    client: ClientId,
    tx: TransactionId,
    amount: Decimal,
}

#[derive(Debug, Deserialize)]
struct InputDepositAlterTransaction {
    client: ClientId,
    tx: TransactionId,
}

impl Account {
    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

impl Engine {
    #[must_use] pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_transactions<R: Read>(&mut self, reader: R) -> Result<(), EngineError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .from_reader(reader);

        for (row, record) in csv_reader.deserialize::<InputTransaction>().enumerate() {
            match record {
                Ok(raw) => self.process_record(raw),
                Err(err) => warn!(row = row + 1, error = %err, "Skipping malformed row"),
            }
        }

        Ok(())
    }

    pub fn apply_transactions_from_file(&mut self, path: PathBuf) -> Result<(), EngineError> {
        let file = File::open(&path).map_err(|error| EngineError::OpenFile {
            path,
            file_error: error,
        })?;
        self.apply_transactions(file)
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

    fn process_record(&mut self, input_transaction: InputTransaction) {
        match input_transaction {
            InputTransaction::Deposit(tx) => self.deposit(tx),
            InputTransaction::Withdrawal(tx) => self.withdraw(tx),
            InputTransaction::Dispute(tx) => self.dispute(tx),
            InputTransaction::Resolve(tx) => self.resolve(tx),
            InputTransaction::Chargeback(tx) => self.chargeback(tx),
        }
    }

    fn deposit(&mut self, tx: InputFundTransaction) {
        let account = self.accounts.entry(tx.client).or_default();
        if account.transactions.contains_key(&tx.tx) {
            warn!(tx = tx.tx, "Duplicate transaction id ignored");
            return;
        }

        let account = self.accounts.entry(tx.client).or_default();
        if account.locked {
            return;
        }

        let amount = tx.amount;
        account.available += amount;
        account.transactions.insert(
            tx.tx,
            Transaction::Deposit {
                amount,
                state: TransactionState::Normal,
            },
        );
    }

    fn withdraw(&mut self, tx: InputFundTransaction) {
        let account = self.accounts.entry(tx.client).or_default();
        if account.transactions.contains_key(&tx.tx) {
            warn!(tx = tx.tx, "Duplicate transaction id ignored");
            return;
        }

        let account = self.accounts.entry(tx.client).or_default();
        if account.locked {
            return;
        }

        let amount = tx.amount;
        if account.available < amount {
            return;
        }

        account.available -= amount;
        account.transactions.insert(tx.tx, Transaction::Withdrawal { amount });
    }

    fn dispute(&mut self, input_tx: InputDepositAlterTransaction) {
        let Some(account) = self.accounts.get_mut(&input_tx.client) else {
            // Client doesn't exist
            return;
        };

        let Some(stored_tx) = account.transactions.get_mut(&input_tx.tx) else {
            return;
        };

        let Transaction::Deposit { amount, state } = stored_tx else {
            // Withdrawals cannot be disputed
            return;
        };

        if account.locked {
            return;
        }

        account.available -= *amount;
        account.held += *amount;
        *state = TransactionState::Disputed;
    }

    fn resolve(&mut self, input_tx: InputDepositAlterTransaction) {
        let Some(account) = self.accounts.get_mut(&input_tx.client) else {
            // Client doesn't exist
            return;
        };

        let Some(stored_tx) = account.transactions.get_mut(&input_tx.tx) else {
            return;
        };

        let Transaction::Deposit { amount, state } = stored_tx else {
            // Withdrawals cannot be disputed
            return;
        };

        if account.locked {
            return;
        }

        account.held -= *amount;
        account.available += *amount;
        *state = TransactionState::Resolved;
    }

    fn chargeback(&mut self, input_tx: InputDepositAlterTransaction) {
        let Some(account) = self.accounts.get_mut(&input_tx.client) else {
            // Client doesn't exist
            return;
        };

        let Some(stored_tx) = account.transactions.get_mut(&input_tx.tx) else {
            return;
        };

        let Transaction::Deposit { amount, state } = stored_tx else {
            // Withdrawals cannot be disputed
            return;
        };

        if account.locked {
            return;
        }

        account.held -= *amount;
        account.locked = true;
        *state = TransactionState::ChargedBack;
    }
}

fn format_decimal(value: Decimal) -> String {
    format!("{:.4}", value.round_dp(4))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use rust_decimal::Decimal;

    use super::*;

    fn raw(kind: &str, client: u16, tx: u32, amount: Option<&str>) -> InputTransaction {
        InputTransaction {
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
