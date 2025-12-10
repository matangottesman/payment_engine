use std::{
    collections::HashMap,
    fs::File,
    io::{self, Read, Write},
    path::PathBuf,
};
use std::collections::HashSet;
use rust_decimal::Decimal;
use serde::Deserialize;
use thiserror::Error;
use tracing::{error, warn};

type ClientId = u16;
type TransactionId = u32;

#[derive(Default)]
pub struct Engine {
    accounts: HashMap<ClientId, Account>,
    transaction_ids_processed: HashSet<TransactionId>,
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
            let raw_input = match record {
                Ok(r) => r,
                Err(err) => {
                    warn!(line, error = %err, "Skipping malformed transaction row");
                    continue;
                }
            };
            let input = match raw_input.try_into() {
                Ok(tx) => tx,
                Err(err) => {
                    warn!(line, error = %err, "Skipping invalid transaction conversion from raw input");
                    continue;
                }
            };

            self.process_record(input);
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
        let Some(account) = self.get_unlocked_account_or_default(client_id, tx_id) else {
            return;
        };

        account.available += amount;
        account.transactions.insert(
            tx_id,
            Transaction::Deposit(Deposit {
                amount,
                state: TransactionState::Normal,
            }),
        );
        self.transaction_ids_processed.insert(tx_id);
    }

    fn withdraw(&mut self, client_id: ClientId, tx_id: TransactionId, amount: Decimal) {
        let Some(account) = self.get_unlocked_account_or_default(client_id, tx_id) else {
            return;
        };

        if account.available < amount {
            return;
        }

        account.available -= amount;
        account
            .transactions
            .insert(tx_id, Transaction::Withdrawal(Withdrawal { amount }));
        self.transaction_ids_processed.insert(tx_id);
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

    fn get_unlocked_account_or_default(&mut self, client_id: ClientId, tx_id: TransactionId) -> Option<&mut Account> {
        let account = self.accounts.entry(client_id).or_default();
        if account.locked {
            return None;
        }
        if self.transaction_ids_processed.contains(&tx_id) {
            return None;
        }
        Some(account)
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

    #[test]
    fn deposit_and_withdraw() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("1.24")));
        engine.process_record(raw("withdrawal", 1, 2, Some("0.5")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("0.74").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    #[test]
    fn dispute_and_resolve_cycle() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("deposit", 1, 2, Some("1.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("resolve", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("3.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    #[test]
    fn chargeback_locks_account() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("3.5")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("deposit", 1, 2, Some("5.0")));
        engine.process_record(raw("chargeback", 1, 1, None));
        engine.process_record(raw("deposit", 1, 3, Some("1.0")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("5.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(account.locked);
    }

    #[test]
    fn withdrawal_before_any_deposit_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("withdrawal", 1, 1, Some("1.0")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert!(account.transactions.is_empty());
    }

    #[test]
    fn skips_insufficient_withdrawal() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("1.0")));
        engine.process_record(raw("deposit", 1, 3, Some("1.0")));
        engine.process_record(raw("withdrawal", 1, 2, Some("2.01")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
    }

    #[test]
    fn withdrawal_does_not_use_held_funds() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("5.0")));
        engine.process_record(raw("deposit", 1, 5, Some("2.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("withdrawal", 1, 2, Some("3.0")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::from_str("5.0").unwrap());
        assert!(!account.locked);
    }

    #[test]
    fn disputing_already_disputed_transaction_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("dispute", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::from_str("2.0").unwrap());
        let Transaction::Deposit(deposit) = account.transactions.get(&1).unwrap() else {
            panic!("expected deposit transaction");
        };
        assert!(matches!(deposit.state, TransactionState::Disputed));
    }

    #[test]
    fn resolve_not_in_dispute_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("resolve", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        let Transaction::Deposit(deposit) = account.transactions.get(&1).unwrap() else {
            panic!("expected deposit transaction");
        };
        assert!(matches!(deposit.state, TransactionState::Normal));
    }

    #[test]
    fn chargeback_not_in_dispute_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("chargeback", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
        let Transaction::Deposit(deposit) = account.transactions.get(&1).unwrap() else {
            panic!("expected deposit transaction");
        };
        assert!(matches!(deposit.state, TransactionState::Normal));
    }

    #[test]
    fn dispute_or_resolution_on_withdrawal_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("withdrawal", 1, 2, Some("1.0")));
        engine.process_record(raw("dispute", 1, 2, None));
        engine.process_record(raw("resolve", 1, 2, None));
        engine.process_record(raw("chargeback", 1, 2, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("1.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
        let Transaction::Withdrawal(withdrawal) = account.transactions.get(&2).unwrap() else {
            panic!("expected withdrawal transaction");
        };
        assert_eq!(withdrawal.amount, Decimal::from_str("1.0").unwrap());
    }

    #[test]
    fn dispute_or_resolution_on_missing_transaction_is_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("2.0")));
        engine.process_record(raw("dispute", 1, 98, None));
        engine.process_record(raw("resolve", 1, 99, None));
        engine.process_record(raw("chargeback", 1, 99, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("2.0").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
        assert!(matches!(
            account.transactions.get(&1),
            Some(Transaction::Deposit(_))
        ));
    }

    #[test]
    fn all_transaction_types_are_ignored_on_locked_account() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("3.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("chargeback", 1, 1, None));

        engine.process_record(raw("deposit", 1, 2, Some("1.0")));
        engine.process_record(raw("withdrawal", 1, 3, Some("1.0")));
        engine.process_record(raw("dispute", 1, 1, None));
        engine.process_record(raw("resolve", 1, 1, None));
        engine.process_record(raw("chargeback", 1, 1, None));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::ZERO);
        assert_eq!(account.held, Decimal::ZERO);
        assert!(account.locked);
        assert_eq!(account.transactions.len(), 1);
        let Transaction::Deposit(deposit) = account.transactions.get(&1).unwrap() else {
            panic!("expected deposit transaction");
        };
        assert!(matches!(deposit.state, TransactionState::ChargedBack));
    }

    #[test]
    fn duplicate_transactions_ids_ignored() {
        let mut engine = Engine::default();
        engine.process_record(raw("deposit", 1, 1, Some("1.24")));
        engine.process_record(raw("withdrawal", 1, 2, Some("0.5")));
        engine.process_record(raw("deposit", 1, 2, Some("5")));

        let account = engine.accounts.get(&1).unwrap();
        assert_eq!(account.available, Decimal::from_str("0.74").unwrap());
        assert_eq!(account.held, Decimal::ZERO);
        assert!(!account.locked);
    }

    fn raw(kind: &str, client: ClientId, tx: TransactionId, amount: Option<&str>) -> InputTransaction {
        RawInputTransaction {
            tx_type: kind.to_string(),
            client,
            tx,
            amount: amount.map(|v| Decimal::from_str(v).expect("Incorrect decimal string")),
        }
            .try_into()
            .expect("Raw transaction failed to convert into InputTransaction")
    }
}
