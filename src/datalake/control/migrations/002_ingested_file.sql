-- The file ledger: what the ingest process did with each file it found.
-- A file is identified by its source and path; its content hash tells a corrected
-- re-upload from the same file seen again.

create table control.ingested_file (
    process_id    bigint not null references control.process (process_id) on delete cascade,
    source        text not null,
    path          text not null,
    content_hash  text not null,
    status        text not null check (status in ('loaded', 'rejected', 'superseded')),
    rows_loaded   bigint,
    error         text,
    run_id        uuid references control.run (run_id) on delete set null,
    recorded_at   timestamptz not null default now(),
    primary key (process_id, source, path)
);
