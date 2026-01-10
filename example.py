from typing import OrderedDict
from rawmigrate.core import DB
from rawmigrate.core import SqlFormatOption

from rawmigrate.entity_manager import EntityManager

db = DB()
root = EntityManager.create_root(db)
public = root.Schema("public")
root = root.with_schema(public)

user = root.Table(
    "user",
    id="uuid primary key default uuid_generate_v4()",
    name="varchar(255) not null",
    email="varchar(255) not null",
    password="varchar(255) not null",
    created_at="timestamp not null default now()",
    updated_at="timestamp not null default now()",
    subscribers_count="integer not null default 0",
)

root.after(user).Index(
    "idx_user_email", on=user, using="btree", expressions=[user.c.email]
)

subscription = (
    root.after(user)
    .Table(
        "subscription",
        subscriber_id=f"uuid not null references {user}({user.c.id})",
        subscribed_to_id=f"uuid not null references {user}({user.c.id})",
    )
    .additional(
        "PRIMARY KEY (subscriber_id, subscribed_to_id)",
    )
)

# Can omit .then or .after here for example, because of the dependency recognition
handle_new_subscription = root.Function(
    "handle_new_subscription",
    returns="trigger",
    language="plpgsql",  # can be omitted, default value here
    args=OrderedDict(
        new_subscription_id="uuid not null",
    ),
    # Like this:
    body=f"""
    begin
        update {user} set {user.c.subscribers_count} = {user.c.subscribers_count} + 1 where {user.c.id} = new.{subscription.c.subscribed_to_id};
    end;
    """,
    # Or just begin=... to omit begin+end, for convenience.
)

test = handle_new_subscription.then.Trigger(
    "handle_new_subscription_trigger",
    before="insert or update",  # as well as after=, instead_of=
    on=subscription,  # it will accept any string-like
    function=f"{handle_new_subscription}()",  # or procedure=
)


print(test.ref, "->", test.dependency_refs)
print(handle_new_subscription.ref, "->", handle_new_subscription.dependency_refs)
print(subscription.ref, "->", subscription.dependency_refs)
print(user.ref, "->", user.dependency_refs)

print("======================")
print(format(handle_new_subscription.body, SqlFormatOption.SQL_META))
print("======================")

print(db.entity_storage)
