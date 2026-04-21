module counter::counter {
    use std::signer;
    use aptos_std::event;

    // -----------------------------------------------------------------------
    // Resources
    // -----------------------------------------------------------------------

    /// Holds a counter value stored under the owner's account.
    struct Counter has key {
        value: u64,
    }

    // -----------------------------------------------------------------------
    // Events  (Aptos v2 – emitted via aptos_std::event::emit)
    // -----------------------------------------------------------------------

    #[event]
    struct CounterIncrementedEvent has drop, store {
        account: address,
        old_value: u64,
        new_value: u64,
        increment_by: u64,
    }

    #[event]
    struct CounterDecrementedEvent has drop, store {
        account: address,
        old_value: u64,
        new_value: u64,
        decrement_by: u64,
    }

    // -----------------------------------------------------------------------
    // Errors
    // -----------------------------------------------------------------------

    const E_COUNTER_ALREADY_EXISTS: u64 = 1;
    const E_COUNTER_NOT_INITIALIZED: u64 = 2;
    const E_ZERO_DELTA: u64 = 3;
    const E_UNDERFLOW: u64 = 4;

    // -----------------------------------------------------------------------
    // Public entry functions
    // -----------------------------------------------------------------------

    /// Create and publish a Counter resource for the caller.
    public entry fun initialize(account: &signer) {
        let addr = signer::address_of(account);
        assert!(!exists<Counter>(addr), E_COUNTER_ALREADY_EXISTS);

        move_to(account, Counter { value: 0 });
    }

    /// Increment the caller's counter by `by`.
    public entry fun increment(account: &signer, by: u64) acquires Counter {
        assert!(by > 0, E_ZERO_DELTA);

        let addr = signer::address_of(account);
        assert!(exists<Counter>(addr), E_COUNTER_NOT_INITIALIZED);

        let counter = borrow_global_mut<Counter>(addr);
        let old_value = counter.value;
        counter.value = old_value + by;

        event::emit(CounterIncrementedEvent {
            account: addr,
            old_value,
            new_value: counter.value,
            increment_by: by,
        });
    }

    /// Decrement the caller's counter by `by`.
    public entry fun decrement(account: &signer, by: u64) acquires Counter {
        assert!(by > 0, E_ZERO_DELTA);

        let addr = signer::address_of(account);
        assert!(exists<Counter>(addr), E_COUNTER_NOT_INITIALIZED);

        let counter = borrow_global_mut<Counter>(addr);
        let old_value = counter.value;
        assert!(old_value >= by, E_UNDERFLOW);
        counter.value = old_value - by;

        event::emit(CounterDecrementedEvent {
            account: addr,
            old_value,
            new_value: counter.value,
            decrement_by: by,
        });
    }

    // -----------------------------------------------------------------------
    // Read-only helpers
    // -----------------------------------------------------------------------

    #[view]
    public fun get_value(addr: address): u64 acquires Counter {
        assert!(exists<Counter>(addr), E_COUNTER_NOT_INITIALIZED);
        borrow_global<Counter>(addr).value
    }
}
