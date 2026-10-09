//! Reads `fixtures/paradex/signing/paradex-vectors.tsv`, the file `ParadexHashOracle.java`
//! writes from the Java FueledByChaiTrading signer. Every value in it is synthetic (the
//! directory's `SYNTHETIC` file says where each comes from), and the account and signing key
//! the tests use are read from its header so that no key-shaped value sits in this crate.

#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use fbc_venue_paradex::sign::{Felt, ParadexSigner, StarkKey};

pub mod secrets;

pub fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/signing/paradex-vectors.tsv")
}

/// One row of the vectors file, by column name.
pub struct Row(HashMap<String, String>);

impl Row {
    pub fn get(&self, column: &str) -> &str {
        self.0
            .get(column)
            .unwrap_or_else(|| panic!("no column {column}"))
    }

    pub fn felt(&self, column: &str) -> Felt {
        Felt::from_hex(self.get(column)).unwrap()
    }

    pub fn u64(&self, column: &str) -> u64 {
        self.get(column).parse().unwrap()
    }
}

pub struct Vectors {
    pub header: HashMap<String, String>,
    pub rows: Vec<Row>,
}

impl Vectors {
    pub fn read() -> Vectors {
        let text = fs::read_to_string(vectors_path()).expect("the Java vectors file");
        let mut header = HashMap::new();
        let mut columns: Option<Vec<String>> = None;
        let mut rows = Vec::new();
        for line in text.lines() {
            if let Some(comment) = line.strip_prefix("# ") {
                for pair in comment.split(' ') {
                    if let Some((k, v)) = pair.split_once('=') {
                        header.insert(k.to_owned(), v.to_owned());
                    }
                }
                continue;
            }
            let cells: Vec<&str> = line.split('\t').collect();
            match &columns {
                None => columns = Some(cells.iter().map(|c| (*c).to_owned()).collect()),
                Some(names) => {
                    assert_eq!(cells.len(), names.len(), "row width: {line}");
                    rows.push(Row(names
                        .iter()
                        .cloned()
                        .zip(cells.iter().map(|c| (*c).to_owned()))
                        .collect()));
                }
            }
        }
        Vectors { header, rows }
    }

    pub fn header_felt(&self, key: &str) -> Felt {
        Felt::from_hex(&self.header[key]).unwrap()
    }

    pub fn account(&self) -> Felt {
        self.header_felt("account")
    }

    pub fn chain_id(&self) -> Felt {
        self.header_felt("chain_id")
    }

    pub fn signer(&self) -> ParadexSigner {
        let key = StarkKey::from_hex(&self.header["key"]).unwrap();
        ParadexSigner::new(self.account(), self.chain_id(), key)
    }

    pub fn row(&self, name: &str) -> &Row {
        self.rows
            .iter()
            .find(|r| r.get("name") == name)
            .unwrap_or_else(|| panic!("no vector {name}"))
    }
}
