//! [`TypeFilter`]: which event types a subscription receives.

use std::collections::BTreeSet;

/// Which event types a read or a subscription covers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TypeFilter {
    /// Every type.
    #[default]
    All,
    /// Only these types.
    Only(BTreeSet<String>),
}

impl TypeFilter {
    /// A filter for `types`. An empty list means [`TypeFilter::All`].
    pub fn only<I, S>(types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: BTreeSet<String> = types.into_iter().map(Into::into).collect();
        if set.is_empty() {
            TypeFilter::All
        } else {
            TypeFilter::Only(set)
        }
    }

    /// Whether `event_type` passes the filter.
    pub fn matches(&self, event_type: &str) -> bool {
        match self {
            TypeFilter::All => true,
            TypeFilter::Only(set) => set.contains(event_type),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_means_all() {
        assert_eq!(TypeFilter::only(Vec::<String>::new()), TypeFilter::All);
        assert!(TypeFilter::All.matches("X"));
        let f = TypeFilter::only(["A", "B"]);
        assert!(f.matches("A"));
        assert!(!f.matches("C"));
    }
}
