module greeter::greeter {
    use std::signer;
    use std::string::String;
    use aptos_std::event;

    // -----------------------------------------------------------------------
    // Events
    // -----------------------------------------------------------------------

    #[event]
    struct GreetedEvent has drop, store {
        who: address,
        message: String,
    }

    // -----------------------------------------------------------------------
    // Public entry functions
    // -----------------------------------------------------------------------

    /// Emit a `GreetedEvent` carrying `message` from the caller.
    /// Stateless on purpose — demonstrates a second Move module at a
    /// distinct on-chain address, nothing more.
    public entry fun greet(account: &signer, message: String) {
        event::emit(GreetedEvent {
            who: signer::address_of(account),
            message,
        });
    }
}
