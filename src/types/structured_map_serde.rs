use std::hash::Hash;
use std::collections::HashMap;
use serde::{Serialize, Deserialize, Serializer, Deserializer};

// This module is heavily inspired by this GitHub issue: https://github.com/serde-rs/json/issues/402#issue-286714266

pub fn serialize<S, K, V> (map: &HashMap<K,V>, s: S) -> Result<S::Ok, S::Error> where
    S: Serializer,
    K: Serialize,
    V: Serialize
{
    map.iter().map(|(a,b)| (a.clone(),b.clone())).collect::<Vec<(_,_)>>().serialize(s)
}

pub fn deserialize<'de, D, K, V>(d: D) -> Result<HashMap<K,V>, D::Error> where 
    D: Deserializer<'de>,
    K: Eq + Hash + Deserialize<'de>,
    V: Deserialize<'de>
{
    let vec = <Vec<(K, V)>>::deserialize(d)?;
    let mut map = HashMap::<K,V>::new();
    for (k, v) in vec {
        map.insert(k, v);
    }
    Ok(map)
}
