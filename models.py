import aiosqlite

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS messages (
    message_id TEXT PRIMARY KEY,
    from_msisdn TEXT NOT NULL,
    to_msisdn TEXT NOT NULL,
    ts TEXT NOT NULL,
    text TEXT,
    created_at TEXT NOT NULL
);
"""

async def init_db(db):
    await db.execute(CREATE_TABLE_SQL)
    await db.commit()
