# Docs

## Dependency recognition mechanism

So when you write:

    f"uuid not null references {user}({user.c.id})"

It actually produces:

    "uuid not null references "user"$$ref:tbl:a1b2c3$$("user"."id"$$ref:col:d4e5f6$$)"

Then later the schema:

- Scans all SQL strings for $$ref:...:...$$ patterns
- Builds the dependency graph automatically
- Strips the markers before generating final SQL
