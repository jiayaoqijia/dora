-- +goose Up
CREATE TABLE heze_inclusion_views (
    block_root BLOB NOT NULL PRIMARY KEY,
    view_data BLOB NOT NULL
);

-- +goose Down
DROP TABLE heze_inclusion_views;
