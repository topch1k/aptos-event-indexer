CREATE TABLE counter_incremented_events (
    transaction_version    BIGINT      NOT NULL,
    event_index            BIGINT      NOT NULL,
    transaction_timestamp  TIMESTAMP   NULL,
    account                TEXT        NOT NULL,
    old_value              BIGINT      NOT NULL,
    new_value              BIGINT      NOT NULL,
    increment_by           BIGINT      NOT NULL,
    inserted_at            TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX idx_counter_incremented_account ON counter_incremented_events(account);

CREATE TABLE counter_decremented_events (
    transaction_version    BIGINT      NOT NULL,
    event_index            BIGINT      NOT NULL,
    transaction_timestamp  TIMESTAMP   NULL,
    account                TEXT        NOT NULL,
    old_value              BIGINT      NOT NULL,
    new_value              BIGINT      NOT NULL,
    decrement_by           BIGINT      NOT NULL,
    inserted_at            TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX idx_counter_decremented_account ON counter_decremented_events(account);
