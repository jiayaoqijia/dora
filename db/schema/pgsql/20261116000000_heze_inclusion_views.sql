-- +goose Up
CREATE TABLE heze_inclusion_views (
    block_root BYTEA NOT NULL PRIMARY KEY,
    view_data BYTEA NOT NULL
);

-- +goose Down
DROP TABLE heze_inclusion_views;
