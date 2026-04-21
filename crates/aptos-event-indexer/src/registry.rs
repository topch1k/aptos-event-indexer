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
