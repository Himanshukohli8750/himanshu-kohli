import json, time, uuid
from datetime import datetime

async def log(request, status, extra=None):
    payload = {
        "ts": datetime.utcnow().isoformat() + "Z",
        "level": "INFO",
        "request_id": str(uuid.uuid4()),
        "method": request.method,
        "path": request.url.path,
        "status": status,
        "latency_ms": int((time.time() - request.state.start) * 1000),
    }
    if extra:
        payload.update(extra)
    print(json.dumps(payload))
