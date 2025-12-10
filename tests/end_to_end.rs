use std::{collections::HashMap, str::FromStr};

use payments_engine::Engine;
use rust_decimal::Decimal;
use serde::Deserialize;

const SAMPLE_TRANSACTIONS: &str = include_str!("sample_transactions.csv");

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct AccountRow {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

#[test]
fn processes_sample_transactions_end_to_end() {
    let mut engine = Engine::new();

    let mut output = Vec::new();
    engine
        .apply_transactions(SAMPLE_TRANSACTIONS.as_bytes())
        .expect("engine accepts sample csv");
    engine.write_accounts(&mut output).expect("engine emits accounts");

    let actual_accounts = read_accounts(&output);
    assert_eq!(actual_accounts, expected_accounts());
}

fn dec(value: &str) -> Decimal {
    Decimal::from_str(value).expect("literal decimal parses")
}

fn account(client: u16, available: &str, held: &str, total: &str, locked: bool) -> AccountRow {
    AccountRow {
        client,
        available: dec(available),
        held: dec(held),
        total: dec(total),
        locked,
    }
}

fn read_accounts(output: &[u8]) -> HashMap<u16, AccountRow> {
    let mut reader = csv::Reader::from_reader(output);
    reader
        .deserialize::<AccountRow>()
        .map(|row| {
            let row = row.expect("account row should deserialize");
            (row.client, row)
        })
        .collect()
}

fn expected_accounts() -> HashMap<u16, AccountRow> {
    let mut accounts = HashMap::new();
    accounts.insert(1, account(1, "6.5", "0", "6.5", false));
    accounts.insert(2, account(2, "-500", "250", "-250", true));
    accounts.insert(3, account(3, "1", "20", "21", false));
    accounts.insert(4, account(4, "0.5", "3.1234", "3.6234", true));
    accounts.insert(5, account(5, "0.5", "0", "0.5", false));
    accounts
}
