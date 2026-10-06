//! Identifiers: a UUIDv7 rendered as a type prefix and 26 characters of Crockford base32, so
//! `rec_06G9N084KXVH9BQD3DW01R4TA4` says what it names, sorts by creation time, and survives a
//! copy from a terminal (no `I`, `L`, `O` or `U` to misread).

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// The type an identifier names, and the prefix it is written with.
pub trait Kind: Copy + Eq {
    const PREFIX: &'static str;
}

macro_rules! kinds {
    ($($name:ident => $prefix:literal),* $(,)?) => {$(
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name;
        impl Kind for $name {
            const PREFIX: &'static str = $prefix;
        }
    )*};
}

kinds! {
    Org => "org",
    Con => "con",
    Rep => "rep",
    Rec => "rec",
    Hrf => "hrf",
    Syn => "syn",
}

pub type OrgId = Id<Org>;
pub type ConnectionId = Id<Con>;
pub type RepositoryId = Id<Rep>;
pub type ReconcileId = Id<Rec>;
pub type HeadRefreshId = Id<Hrf>;
pub type SyncId = Id<Syn>;

#[derive(PartialEq, Eq, Hash, Clone, Copy, PartialOrd, Ord)]
pub struct Id<K: Kind> {
    uuid: Uuid,
    kind: PhantomData<K>,
}

impl<K: Kind> Id<K> {
    /// A new identifier, ordered after every one made before it.
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Id {
            uuid,
            kind: PhantomData,
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl<K: Kind> Default for Id<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("{0:?} is not a {1} identifier: want {1}_ followed by 26 characters")]
    Shape(String, &'static str),
    #[error("{0:?} is not a valid identifier: {1}")]
    Encoding(String, &'static str),
}

/// 128 bits as 26 base32 characters; the first carries only the top 3 bits.
pub fn encode(uuid: Uuid) -> String {
    let n = uuid.as_u128();
    let mut out = [0u8; 26];
    for (i, c) in out.iter_mut().enumerate() {
        let shift = 5 * (25 - i);
        *c = ALPHABET[((n >> shift) & 0x1f) as usize];
    }
    String::from_utf8(out.to_vec()).expect("ascii")
}

pub fn decode(s: &str) -> Result<Uuid, &'static str> {
    if s.len() != 26 {
        return Err("want 26 characters");
    }
    let mut n: u128 = 0;
    for (i, b) in s.bytes().enumerate() {
        let v = match b.to_ascii_uppercase() {
            b'O' => 0,
            b'I' | b'L' => 1,
            c => ALPHABET
                .iter()
                .position(|&a| a == c)
                .ok_or("a character outside Crockford base32")? as u128,
        };
        if i == 0 && v > 7 {
            return Err("overflows 128 bits");
        }
        n = (n << 5) | v;
    }
    Ok(Uuid::from_u128(n))
}

impl<K: Kind> fmt::Display for Id<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", K::PREFIX, encode(self.uuid))
    }
}

impl<K: Kind> fmt::Debug for Id<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<K: Kind> FromStr for Id<K> {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, IdError> {
        let body = s
            .strip_prefix(K::PREFIX)
            .and_then(|r| r.strip_prefix('_'))
            .ok_or_else(|| IdError::Shape(s.to_string(), K::PREFIX))?;
        let uuid = decode(body).map_err(|e| IdError::Encoding(s.to_string(), e))?;
        Ok(Self::from_uuid(uuid))
    }
}

/// True when `s` has the shape of an identifier of this kind. Lets a command take a name or
/// an id in the same argument.
pub fn looks_like<K: Kind>(s: &str) -> bool {
    s.parse::<Id<K>>().is_ok()
}

impl<K: Kind> Serialize for Id<K> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de, K: Kind> Deserialize<'de> for Id<K> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        for _ in 0..1000 {
            let id = ReconcileId::new();
            let s = id.to_string();
            assert_eq!(s.len(), 4 + 26);
            assert!(s.starts_with("rec_"));
            assert_eq!(s.parse::<ReconcileId>().unwrap(), id);
        }
    }

    #[test]
    fn reads_the_example_from_the_spec() {
        let id: ReconcileId = "rec_06G9N084KXVH9BQD3DW01R4TA4".parse().unwrap();
        assert_eq!(id.to_string(), "rec_06G9N084KXVH9BQD3DW01R4TA4");
    }

    #[test]
    fn extremes() {
        assert_eq!(encode(Uuid::nil()), "0".repeat(26));
        assert_eq!(encode(Uuid::max()), format!("7{}", "Z".repeat(25)));
        assert_eq!(
            decode(&format!("7{}", "Z".repeat(25))).unwrap(),
            Uuid::max()
        );
        assert!(decode(&format!("8{}", "0".repeat(25))).is_err());
    }

    #[test]
    fn rejects_the_wrong_kind_and_shape() {
        let rec = ReconcileId::new().to_string();
        assert!(rec.parse::<ConnectionId>().is_err());
        assert!("con_short".parse::<ConnectionId>().is_err());
        assert!(
            "con_0000000000000000000000000U"
                .parse::<ConnectionId>()
                .is_err()
        );
        assert!(!looks_like::<Con>("lab"));
    }

    #[test]
    fn sorts_by_creation() {
        let a = ReconcileId::new().to_string();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = ReconcileId::new().to_string();
        assert!(a < b);
    }
}
