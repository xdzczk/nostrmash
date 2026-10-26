CREATE TABLE IF NOT EXISTS primal_media_metadata (
    url TEXT PRIMARY KEY,
    mimetype TEXT NOT NULL DEFAULT '',
    width INT NOT NULL DEFAULT 0,
    height INT NOT NULL DEFAULT 0,
    duration INT NOT NULL DEFAULT 0,
    blurhash TEXT NOT NULL DEFAULT '',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS primal_link_metadata (
    url TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL DEFAULT '',
    image TEXT NOT NULL DEFAULT '',
    icon_url TEXT NOT NULL DEFAULT '',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
