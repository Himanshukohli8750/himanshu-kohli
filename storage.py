import aiosqlite
from datetime import datetime

async def insert_message(db, msg):
    try:
        await db.execute(
            """
            INSERT INTO messages
            (message_id, from_msisdn, to_msisdn, ts, text, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                msg["message_id"],
                msg["from"],
                msg["to"],
                msg["ts"],
                msg.get("text"),
                datetime.utcnow().isoformat() + "Z",
            ),
        )
        await db.commit()
        return "created"
    except aiosqlite.IntegrityError:
        return "duplicate"


async def list_messages(db, limit, offset, from_, since, q):
    where = []
    params = []

    if from_:
        where.append("from_msisdn = ?")
        params.append(from_)
    if since:
        where.append("ts >= ?")
        params.append(since)
    if q:
        where.append("LOWER(text) LIKE ?")
        params.append(f"%{q.lower()}%")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    total_q = f"SELECT COUNT(*) FROM messages {where_sql}"
    data_q = f"""
        SELECT message_id, from_msisdn, to_msisdn, ts, text
        FROM messages
        {where_sql}
        ORDER BY ts ASC, message_id ASC
        LIMIT ? OFFSET ?
    """

    async with db.execute(total_q, params) as cur:
        total = (await cur.fetchone())[0]

    async with db.execute(data_q, params + [limit, offset]) as cur:
        rows = await cur.fetchall()

    data = [
        {
            "message_id": r[0],
            "from": r[1],
            "to": r[2],
            "ts": r[3],
            "text": r[4],
        }
        for r in rows
    ]

    return data, total


async def stats(db):
    result = {}

    async with db.execute("SELECT COUNT(*) FROM messages") as c:
        result["total_messages"] = (await c.fetchone())[0]

    async with db.execute("SELECT COUNT(DISTINCT from_msisdn) FROM messages") as c:
        result["senders_count"] = (await c.fetchone())[0]

    async with db.execute("""
        SELECT from_msisdn, COUNT(*) c
        FROM messages
        GROUP BY from_msisdn
        ORDER BY c DESC
        LIMIT 10
    """) as c:
        result["messages_per_sender"] = [
            {"from": r[0], "count": r[1]} for r in await c.fetchall()
        ]

    async with db.execute("SELECT MIN(ts), MAX(ts) FROM messages") as c:
        r = await c.fetchone()
        result["first_message_ts"] = r[0]
        result["last_message_ts"] = r[1]

    return result
