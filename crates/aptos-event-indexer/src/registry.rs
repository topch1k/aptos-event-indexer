use std::collections::HashMap;
use std::sync::Arc;

use crate::traits::event_processor::{ArcEventProcessor, EventProcessor};

/// Stable index into the registry. Cheap to copy, used as a routing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProcessorId(pub u16);

/// Registry of all event processors handled by a single indexer.
///
/// Lookups by `type_str` are O(1). Iteration order follows registration
/// order so migrations are applied deterministically.
#[derive(Clone, Default)]
pub struct EventRegistry {
    by_type_str: HashMap<String, ProcessorId>,
    processors: Vec<ArcEventProcessor>,
}

impl EventRegistry {
    pub fn builder() -> EventRegistryBuilder {
        EventRegistryBuilder::default()
    }

    pub fn lookup(&self, type_str: &str) -> Option<ProcessorId> {
        self.by_type_str.get(type_str).copied()
    }

    pub fn processor(&self, id: ProcessorId) -> &ArcEventProcessor {
        &self.processors[id.0 as usize]
    }

    pub fn processors(&self) -> &[ArcEventProcessor] {
        &self.processors
    }

    pub fn len(&self) -> usize {
        self.processors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }
}

#[derive(Default)]
pub struct EventRegistryBuilder {
    registry: EventRegistry,
}

impl EventRegistryBuilder {
    /// Register a processor. Panics on duplicate `type_str` so the error
    /// surfaces at startup instead of silently shadowing.
    pub fn register<P: EventProcessor>(mut self, processor: P) -> Self {
        let type_str = processor.type_str().to_owned();
        assert!(
            !self.registry.by_type_str.contains_key(&type_str),
            "duplicate EventProcessor registered for type_str = {type_str}"
        );
        let id = ProcessorId(self.registry.processors.len() as u16);
        self.registry.by_type_str.insert(type_str, id);
        self.registry.processors.push(Arc::new(processor));
        self
    }

    pub fn build(self) -> EventRegistry {
        self.registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use aptos_indexer_processor_sdk::aptos_protos::transaction::v1::Event;
    use aptos_indexer_processor_sdk::postgres::utils::database::ArcDbPool;
    use async_trait::async_trait;
    use rstest::rstest;
    use std::any::Any;

    use crate::context::EventContext;
    use crate::traits::event_processor::{EventProcessor, ParsedItem};

    struct StubProcessor {
        type_str: &'static str,
        name: &'static str,
    }

    #[async_trait]
    impl EventProcessor for StubProcessor {
        fn type_str(&self) -> &str {
            self.type_str
        }
        fn name(&self) -> &'static str {
            self.name
        }
        fn parse(&self, _event: &Event, _ctx: &EventContext) -> Result<Box<dyn Any + Send + Sync>> {
            Ok(Box::new(()))
        }
        async fn store(&self, _pool: &ArcDbPool, _items: &[ParsedItem]) -> Result<()> {
            Ok(())
        }
    }

    fn stub(type_str: &'static str, name: &'static str) -> StubProcessor {
        StubProcessor { type_str, name }
    }

    #[test]
    fn default_registry_is_empty() {
        let reg = EventRegistry::default();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);
        assert!(reg.lookup("0x1::foo::Bar").is_none());
    }

    #[test]
    fn register_assigns_sequential_ids_and_preserves_order() {
        let reg = EventRegistry::builder()
            .register(stub("0x1::a::A", "a"))
            .register(stub("0x1::b::B", "b"))
            .register(stub("0x1::c::C", "c"))
            .build();

        assert_eq!(reg.len(), 3);
        assert_eq!(reg.lookup("0x1::a::A"), Some(ProcessorId(0)));
        assert_eq!(reg.lookup("0x1::b::B"), Some(ProcessorId(1)));
        assert_eq!(reg.lookup("0x1::c::C"), Some(ProcessorId(2)));
        let names: Vec<_> = reg.processors().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[rstest]
    #[case("0x1::a::A", Some(0))]
    #[case("0x1::b::B", Some(1))]
    #[case("0x1::missing::X", None)]
    #[case("", None)]
    fn lookup_returns_expected_id(#[case] type_str: &str, #[case] expected: Option<u16>) {
        let reg = EventRegistry::builder()
            .register(stub("0x1::a::A", "a"))
            .register(stub("0x1::b::B", "b"))
            .build();
        assert_eq!(reg.lookup(type_str).map(|p| p.0), expected);
    }

    #[test]
    #[should_panic(expected = "duplicate EventProcessor registered")]
    fn duplicate_type_str_panics() {
        EventRegistry::builder()
            .register(stub("0x1::dup::D", "first"))
            .register(stub("0x1::dup::D", "second"));
    }
}
