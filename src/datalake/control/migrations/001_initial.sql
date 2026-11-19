-- Control schema: registered processes, their configuration, and every run.
-- One database per environment, so there is no environment column.

create schema if not exists control;

create table control.process (
    process_id   bigint generated always as identity primary key,
    name         text not null unique check (name ~ '^[a-z][a-z0-9-]*$'),
    description  text not null default '',
    is_enabled   boolean not null default true,
    created_at   timestamptz not null default now(),
    updated_at   timestamptz not null default now()
);

-- A null process_id means the value is shared by every process.
create table control.config (
    config_id   bigint generated always as identity primary key,
    process_id  bigint references control.process (process_id) on delete cascade,
    key         text not null check (key ~ '^[a-z][a-z0-9_]*$'),
    value       text not null,
    updated_by  text not null,
    updated_at  timestamptz not null default now()
);

create unique index config_scope_key on control.config (coalesce(process_id, 0), key);

create table control.config_history (
    history_id  bigint generated always as identity primary key,
    process_id  bigint,
    key         text not null,
    operation   text not null check (operation in ('insert', 'update', 'delete')),
    old_value   text,
    new_value   text,
    changed_by  text not null,
    changed_at  timestamptz not null default now()
);

create function control.record_config_change() returns trigger language plpgsql as $$
begin
    if tg_op = 'DELETE' then
        insert into control.config_history (process_id, key, operation, old_value, new_value, changed_by)
        values (old.process_id, old.key, 'delete', old.value, null,
                coalesce(current_setting('control.actor', true), old.updated_by));
        return old;
    elsif tg_op = 'UPDATE' then
        if new.value is distinct from old.value then
            insert into control.config_history (process_id, key, operation, old_value, new_value, changed_by)
            values (new.process_id, new.key, 'update', old.value, new.value, new.updated_by);
        end if;
        return new;
    else
        insert into control.config_history (process_id, key, operation, old_value, new_value, changed_by)
        values (new.process_id, new.key, 'insert', null, new.value, new.updated_by);
        return new;
    end if;
end;
$$;

create trigger config_history
    after insert or update or delete on control.config
    for each row execute function control.record_config_change();

create table control.run (
    run_id           uuid primary key,
    process_id       bigint not null references control.process (process_id),
    status           text not null check (status in ('running', 'succeeded', 'failed')),
    started_at       timestamptz not null,
    ended_at         timestamptz,
    host             text not null default '',
    git_commit       text not null default '',
    config_snapshot  jsonb not null default '{}'::jsonb,
    error            text,
    airflow_dag_id   text,
    airflow_run_id   text,
    airflow_task_id  text
);

create index run_process_started on control.run (process_id, started_at desc);

create table control.step_run (
    step_run_id   bigint generated always as identity primary key,
    run_id        uuid not null references control.run (run_id) on delete cascade,
    name          text not null,
    status        text not null check (status in ('running', 'succeeded', 'failed')),
    started_at    timestamptz not null,
    ended_at      timestamptz,
    rows_read     bigint,
    rows_written  bigint,
    error         text
);

create index step_run_run on control.step_run (run_id);

create table control.watermark (
    process_id   bigint not null references control.process (process_id) on delete cascade,
    object_name  text not null,
    value        text not null,
    updated_at   timestamptz not null default now(),
    primary key (process_id, object_name)
);
