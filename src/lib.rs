use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
};

use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tracing::{error, warn};

type ClientId = u16;
type TransactionId = u32;

#[derive(Default)]
pub struct Engine {
    accounts: HashMap<ClientId, Account>,
}

#[derive(Default)]
struct Account {
    available: Decimal,
    held: Decimal,
    locked: bool,
    transactions: HashMap<TransactionId, Transaction>,
}

enum Transaction {
    Deposit(Deposit),
    Withdrawal(Withdrawal),
}

struct Deposit {
    amount: Decimal,
    state: TransactionState,
}

// Based on spec wording, assuming that withdrawals cannot be disputed, and therefore don't require
// a state.
struct Withdrawal {
    amount: Decimal,
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

    #[error("input transaction validation error: {0}")]
    InputValidation(String),
}

#[derive(Debug)]
enum InputTransaction {
    Deposit(TransactionIds, Decimal),
    Withdrawal(TransactionIds, Decimal),
    Dispute(TransactionIds),
    Resolve(TransactionIds),
    Chargeback(TransactionIds),
}

#[derive(Debug)]
struct TransactionIds {
    client: ClientId,
    tx: TransactionId,
}

#[derive(Debug, Deserialize)]
struct RawInputTransaction {
    #[serde(rename = "type")]
    tx_type: String,
    client: ClientId,
    tx: TransactionId,
    amount: Option<Decimal>,
}

impl Account {
    fn total(&self) -> Decimal {
        self.available + self.held
    }
}

impl Engine {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply_transactions<R: Read>(&mut self, reader: R) -> Result<(), EngineError> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .flexible(true)
            .has_headers(true)
            .from_reader(reader);
        for (line, record) in csv_reader.deserialize::<RawInputTransaction>().enumerate() {
            match record {
                Ok(raw) => self.process_record(raw.try_into()?),
                Err(err) => warn!(line, error = %err, "Skipping malformed row"),
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
            client: ClientId,
            available: String,
            held: String,
            total: String,
            locked: bool,
        }

        let mut csv_writer = csv::Writer::from_writer(writer);
        for (client, account) in &self.accounts {
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
            InputTransaction::Deposit(TransactionIds { client, tx }, amount) => self.deposit(client, tx, amount),
            InputTransaction::Withdrawal(TransactionIds { client, tx }, amount) => self.withdraw(client, tx, amount),
            InputTransaction::Dispute(TransactionIds { client, tx }) => self.dispute(client, tx),
            InputTransaction::Resolve(TransactionIds { client, tx }) => self.resolve(client, tx),
            InputTransaction::Chargeback(TransactionIds { client, tx }) => self.chargeback(client, tx),
        }
    }

    fn deposit(&mut self, client_id: ClientId, tx_id: TransactionId, amount: Decimal) {
        let account = self.accounts.entry(client_id).or_default();
        if account.locked {
            return;
        }

        // This doesn't handle duplicate transactions across clients, but that scenario isn't described
        if account.transactions.contains_key(&tx_id) {
            return;
        }

        account.available += amount;
        account.transactions.insert(
            tx_id,
            Transaction::Deposit(Deposit {
                amount,
                state: TransactionState::Normal,
            }),
        );
    }

    fn withdraw(&mut self, client_id: ClientId, tx_id: TransactionId, amount: Decimal) {
        let Some(account) = self.get_unlocked_account(client_id) else {
            return;
        };

        if account.transactions.contains_key(&tx_id) {
            return;
        }

        if account.available < amount {
            return;
        }

        account.available -= amount;
        account
            .transactions
            .insert(tx_id, Transaction::Withdrawal(Withdrawal { amount }));
    }

    fn dispute(&mut self, client_id: ClientId, tx_id: TransactionId) {
        let Some(account) = self.get_unlocked_account(client_id) else {
            return;
        };
        let Some(Transaction::Deposit(deposit)) = account.transactions.get_mut(&tx_id) else {
            return;
        };

        if !matches!(deposit.state, TransactionState::Normal) {
            return;
        }

        let amount = deposit.amount;
        account.available -= amount;
        account.held += amount;
        deposit.state = TransactionState::Disputed;
    }

    fn resolve(&mut self, client_id: ClientId, tx_id: TransactionId) {
        let Some(account) = self.get_unlocked_account(client_id) else {
            return;
        };
        let Some(Transaction::Deposit(deposit)) = account.transactions.get_mut(&tx_id) else {
            return;
        };

        if !matches!(deposit.state, TransactionState::Disputed) {
            return;
        }

        let amount = deposit.amount;
        account.held -= amount;
        account.available += amount;
        deposit.state = TransactionState::Resolved;
    }

    fn chargeback(&mut self, client_id: ClientId, tx_id: TransactionId) {
        let Some(account) = self.get_unlocked_account(client_id) else {
            return;
        };
        let Some(Transaction::Deposit(deposit)) = account.transactions.get_mut(&tx_id) else {
            return;
        };

        if !matches!(deposit.state, TransactionState::Disputed) {
            return;
        }

        account.held -= deposit.amount;
        account.locked = true;
        deposit.state = TransactionState::ChargedBack;
    }

    fn get_unlocked_account(&mut self, client_id: ClientId) -> Option<&mut Account> {
        let account = self.accounts.get_mut(&client_id)?;
        if account.locked {
            return None;
        }
        Some(account)
    }
}

impl TryFrom<RawInputTransaction> for InputTransaction {
    type Error = EngineError;
    fn try_from(raw: RawInputTransaction) -> Result<Self, Self::Error> {
        let RawInputTransaction {
            tx_type,
            client,
            tx,
            amount,
        } = raw;
        let ids = TransactionIds { client, tx };
        let get_amount = || {
            amount.ok_or_else(|| EngineError::InputValidation(format!("Deposit/Withdrawal (tx {tx}) missing amount")))
        };

        match tx_type.as_str() {
            "deposit" => Ok(Self::Deposit(ids, get_amount()?)),
            "withdrawal" => Ok(Self::Withdrawal(ids, get_amount()?)),
            "dispute" => Ok(Self::Dispute(ids)),
            "resolve" => Ok(Self::Resolve(ids)),
            "chargeback" => Ok(Self::Chargeback(ids)),
            _ => Err(EngineError::InputValidation(format!(
                "Unknown transaction type: {tx_type}"
            ))),
        }
    }
}

fn format_decimal(value: Decimal) -> String {
    value.round_dp(4).normalize().to_string()
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
