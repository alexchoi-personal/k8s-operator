use std::collections::BTreeMap;

#[derive(Clone, Debug, Default)]
pub struct Labels(pub BTreeMap<String, String>);

impl Labels {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn inner(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<String, String> {
        self.0
    }
}

impl<K, V> FromIterator<(K, V)> for Labels
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Annotations(pub BTreeMap<String, String>);

impl Annotations {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    pub fn inner(&self) -> &BTreeMap<String, String> {
        &self.0
    }

    pub fn into_inner(self) -> BTreeMap<String, String> {
        self.0
    }
}

impl<K, V> FromIterator<(K, V)> for Annotations
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}

#[derive(Clone, Debug, Default)]
pub struct Selector(pub BTreeMap<String, String>);

impl Selector {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn match_labels(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.insert(key.into(), value.into());
        self
    }

    pub fn into_inner(self) -> BTreeMap<String, String> {
        self.0
    }
}

impl<K, V> FromIterator<(K, V)> for Selector
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(iter.into_iter().map(|(k, v)| (k.into(), v.into())).collect())
    }
}
