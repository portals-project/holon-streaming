#!/usr/bin/env python3
"""HTTP start gate.

/ready returns 503 until the gate is flipped, then 200.

The gate flips when EITHER:
  - POST /start is called (manual override), OR
  - len(announced) >= EXPECTED_PRODUCERS, where producers register themselves
    via POST /announce?id=<producer_index>.

Endpoints:
  GET  /ready              - 200 once flipped, else 503.
  GET|POST /start          - manual flip; always 200.
  POST /announce?id=<int>  - register a producer; auto-flips when count >= EXPECTED_PRODUCERS.
  GET  /announced          - {"count", "ids", "expected", "started"} for visibility.
"""
import os
import threading

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

EXPECTED_PRODUCERS = int(os.environ.get("EXPECTED_PRODUCERS", "0"))

_lock = threading.Lock()
_announced: set[int] = set()
_started = False


def _maybe_flip_locked() -> None:
    global _started
    if not _started and EXPECTED_PRODUCERS > 0 and len(_announced) >= EXPECTED_PRODUCERS:
        _started = True


@app.route("/ready")
def ready():
    return Response(status=200 if _started else 503)


@app.route("/start", methods=["GET", "POST"])
def start():
    global _started
    with _lock:
        _started = True
    return Response(status=200)


@app.route("/announce", methods=["POST"])
def announce():
    raw_id = request.args.get("id")
    if raw_id is None and request.is_json:
        raw_id = (request.get_json(silent=True) or {}).get("id")
    if raw_id is None:
        return Response("missing 'id'", status=400)
    try:
        pid = int(raw_id)
    except (TypeError, ValueError):
        return Response("'id' must be an integer", status=400)

    with _lock:
        _announced.add(pid)
        _maybe_flip_locked()
        ids_snapshot = sorted(_announced)
        started_snapshot = _started

    return jsonify(
        {
            "count": len(ids_snapshot),
            "ids": ids_snapshot,
            "expected": EXPECTED_PRODUCERS,
            "started": started_snapshot,
        }
    )


@app.route("/announced")
def announced():
    with _lock:
        ids_snapshot = sorted(_announced)
        started_snapshot = _started
    return jsonify(
        {
            "count": len(ids_snapshot),
            "ids": ids_snapshot,
            "expected": EXPECTED_PRODUCERS,
            "started": started_snapshot,
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8090)
