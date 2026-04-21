CREATE TABLE greeted_events (
    transaction_version    BIGINT      NOT NULL,
    event_index            BIGINT      NOT NULL,
    transaction_timestamp  TIMESTAMP   NULL,
    who                    TEXT        NOT NULL,
    message                TEXT        NOT NULL,
    inserted_at            TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_version, event_index)
);
CREATE INDEX idx_greeted_who ON greeted_events(who);
