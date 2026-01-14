import time
from collections import defaultdict

http_requests_total = defaultdict(int)
webhook_requests_total = defaultdict(int)
latency_buckets = defaultdict(int)
LAT_BUCKETS = [100, 500, 1000]

def observe_latency(ms):
    for b in LAT_BUCKETS:
        if ms <= b:
            latency_buckets[b] += 1
            return
    latency_buckets["+Inf"] += 1

def render_metrics():
    lines = []
    for (path, status), v in http_requests_total.items():
        lines.append(
            f'http_requests_total{{path="{path}",status="{status}"}} {v}'
        )
    for result, v in webhook_requests_total.items():
        lines.append(
            f'webhook_requests_total{{result="{result}"}} {v}'
        )
    for k, v in latency_buckets.items():
        lines.append(f'request_latency_ms_bucket{{le="{k}"}} {v}')
    lines.append(f"request_latency_ms_count {sum(latency_buckets.values())}")
    return "\n".join(lines)
