diesel::table! {
    counter_incremented_events (transaction_version, event_index) {
        transaction_version -> Int8,
        event_index -> Int8,
        transaction_timestamp -> Nullable<Timestamp>,
        account -> Text,
        old_value -> Int8,
        new_value -> Int8,
        increment_by -> Int8,
        inserted_at -> Timestamp,
    }
}

diesel::table! {
    counter_decremented_events (transaction_version, event_index) {
        transaction_version -> Int8,
        event_index -> Int8,
        transaction_timestamp -> Nullable<Timestamp>,
        account -> Text,
        old_value -> Int8,
        new_value -> Int8,
        decrement_by -> Int8,
        inserted_at -> Timestamp,
    }
}
