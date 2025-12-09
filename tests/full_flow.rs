use payments_engine::run_from_reader;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
struct AccountRow {
    client: u16,
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

#[test]
fn processes_end_to_end_csv() {
    let input = r#"type,client,tx,amount
deposit,1,1,5.0
deposit,1,2,2.5
withdrawal,1,3,1.0
dispute,1,1,
resolve,1,1,
dispute,1,2,
chargeback,1,2,
deposit,1,4,1.0
deposit,2,5,3.0
withdrawal,2,6,1.0
"#;

    let mut output = Vec::new();
    run_from_reader(input.as_bytes(), &mut output).expect("engine run should succeed");

    let mut reader = csv::Reader::from_reader(output.as_slice());
    let mut accounts = HashMap::new();
    for record in reader.deserialize::<AccountRow>() {
        let row = record.expect("row deserializes");
        accounts.insert(row.client, row);
    }

    let a1 = accounts.get(&1).expect("client 1 exists");
    assert_eq!(a1.available, Decimal::new(4, 0));
    assert_eq!(a1.held, Decimal::ZERO);
    assert_eq!(a1.total, Decimal::new(4, 0));
    assert!(a1.locked);

    let a2 = accounts.get(&2).expect("client 2 exists");
    assert_eq!(a2.available, Decimal::new(2, 0));
    assert_eq!(a2.held, Decimal::ZERO);
    assert_eq!(a2.total, Decimal::new(2, 0));
    assert!(!a2.locked);
}
