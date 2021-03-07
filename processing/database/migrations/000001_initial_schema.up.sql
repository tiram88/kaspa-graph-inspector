CREATE TABLE blocks
(
    id         BIGSERIAL,
    block_hash CHAR(64) UNIQUE                               NOT NULL,
    timestamp  BIGINT                                        NOT NULL,
    parent_ids JSONB                                         NOT NULL,
    height     BIGINT CHECK (height >= 0)                    NOT NULL,
    color      TEXT CHECK (color IN ('gray', 'red', 'blue')) NOT NULL,
    PRIMARY KEY (id)
);
