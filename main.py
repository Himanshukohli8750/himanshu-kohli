import hmac, hashlib, re, time
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from pydantic import BaseModel, Field
from datetime import datetime
import aiosqlite

from config import DATABASE_URL, WEBHOOK_SECRET
from models import init_db
from storage import insert_message, list_messages, stats
from logging_utils import log

app = FastAPI()
E164 = re.compile(r"^\+\d+$")

class WebhookMessage(BaseModel):
    message_id: str = Field(..., min_length=1)
    from_: str = Field(..., alias="from")
    to: str
    ts: str
    text: str | None = Field(None, max_length=4096)

    def validate_all(self):
        if not E164.match(self.from_) or not E164.match(self.to):
            raise ValueError("invalid msisdn")
        datetime.strptime(self.ts, "%Y-%m-%dT%H:%M:%SZ")


@app.on_event("startup")
async def startup():
    if not WEBHOOK_SECRET:
        raise RuntimeError("WEBHOOK_SECRET not set")
    app.state.db = await aiosqlite.connect(DATABASE_URL.replace("sqlite:///", ""))
    await init_db(app.state.db)


@app.middleware("http")
async def timing(request: Request, call_next):
    request.state.start = time.time()
    response = await call_next(request)
    return response


@app.post("/webhook")
async def webhook(
    request: Request,
    x_signature: str | None = Header(None),
):
    body = await request.body()

    if not x_signature:
        await log(request, 401, {"result": "invalid_signature"})
        raise HTTPException(401, "invalid signature")

    sig = hmac.new(
        WEBHOOK_SECRET.encode(), body, hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(sig, x_signature):
        await log(request, 401, {"result": "invalid_signature"})
        raise HTTPException(401, "invalid signature")

    payload = WebhookMessage.model_validate_json(body)
    try:
        payload.validate_all()
    except Exception:
        await log(request, 422, {"result": "validation_error"})
        raise HTTPException(422, "validation error")

    result = await insert_message(app.state.db, payload.model_dump(by_alias=True))
    await log(
        request,
        200,
        {
            "message_id": payload.message_id,
            "dup": result == "duplicate",
            "result": result,
        },
    )
    return {"status": "ok"}


@app.get("/messages")
async def messages(
    limit: int = 50,
    offset: int = 0,
    from_: str | None = None,
    since: str | None = None,
    q: str | None = None,
):
    data, total = await list_messages(
        app.state.db, limit, offset, from_, since, q
    )
    return {"data": data, "total": total, "limit": limit, "offset": offset}


@app.get("/stats")
async def stats_endpoint():
    return await stats(app.state.db)


@app.get("/health/live")
async def live():
    return {"status": "ok"}


@app.get("/health/ready")
async def ready():
    try:
        await app.state.db.execute("SELECT 1")
        if not WEBHOOK_SECRET:
            raise Exception()
        return {"status": "ok"}
    except Exception:
        raise HTTPException(503, "not ready")
