mod artifacts;
mod identity;
mod jobs;
mod lifecycle;
mod packages;

pub use artifacts::*;
pub use identity::*;
pub use jobs::*;
pub use lifecycle::*;
pub use packages::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_id_name_and_selector_grammars() {
        assert!("oll.anki".parse::<PluginId>().is_ok());
        assert!("oll".parse::<PluginId>().is_err());
        assert!("Oll.anki".parse::<PluginId>().is_err());
        assert!("oll-anki".parse::<PluginName>().is_ok());
        assert!("oll.anki".parse::<PluginName>().is_err());
        assert!(matches!(
            "oll.anki".parse::<PluginSelector>().unwrap(),
            PluginSelector::Id(_)
        ));
        assert!(matches!(
            "oll-anki".parse::<PluginSelector>().unwrap(),
            PluginSelector::Name(_)
        ));
    }

    #[test]
    fn normalized_payload_preserves_argument_and_deadline_semantics() {
        let id = "oll.test".parse::<PluginId>().unwrap();
        let first = NormalizedJobPayload::new(
            id.clone(),
            "run".to_owned(),
            vec!["".to_owned(), "-x".to_owned(), "-x".to_owned()],
            None,
        )
        .unwrap();
        let second = NormalizedJobPayload::new(
            id,
            "run".to_owned(),
            vec!["".to_owned(), "-x".to_owned(), "-x".to_owned()],
            None,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    }
}
