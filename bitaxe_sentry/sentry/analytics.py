from fastapi.responses import StreamingResponse
import csv
import io
import json
import datetime
from typing import Optional
from sqlmodel import Session, select
from .db import engine, Miner, Reading, MinerTag, MinerEvent


def _parse_dt(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        raise ValueError("Invalid datetime; use ISO-8601 format")


def _rows(miner_id=None, start=None, end=None):
    with Session(engine) as session:
        q = select(Reading, Miner).join(Miner, Reading.miner_id == Miner.id)
        if miner_id:
            q = q.where(Reading.miner_id == miner_id)
        if start:
            q = q.where(Reading.timestamp >= start)
        if end:
            q = q.where(Reading.timestamp <= end)
        q = q.order_by(Reading.timestamp)
        return session.exec(q).all()


def export_csv(miner_id=None, start=None, end=None):
    rows = _rows(miner_id, start, end)
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow([
        "timestamp","miner_id","miner","endpoint","hash_rate","temperature",
        "vr_temp","voltage","error_percentage","response_time","fan_rpm",
        "fan_pct","best_diff","best_session_diff","pool_url","using_fallback"
    ])
    for r, m in rows:
        writer.writerow([
            r.timestamp.isoformat()+"Z", m.id, m.name, m.endpoint, r.hash_rate,
            r.temperature, r.vr_temp, r.voltage, r.error_percentage,
            r.response_time, r.fan_rpm, r.fan_pct, r.best_diff,
            r.best_session_diff, r.pool_url, r.using_fallback
        ])
    return out.getvalue()


def export_json(miner_id=None, start=None, end=None):
    rows = _rows(miner_id, start, end)
    return json.dumps([
        {
            "timestamp": r.timestamp.isoformat()+"Z",
            "miner_id": m.id, "miner": m.name, "endpoint": m.endpoint,
            "hash_rate": r.hash_rate, "temperature": r.temperature,
            "vr_temp": r.vr_temp, "voltage": r.voltage,
            "error_percentage": r.error_percentage,
            "response_time": r.response_time, "fan_rpm": r.fan_rpm,
            "fan_pct": r.fan_pct, "best_diff": r.best_diff,
            "best_session_diff": r.best_session_diff,
            "pool_url": r.pool_url, "using_fallback": r.using_fallback,
        } for r, m in rows
    ], indent=2)


def add_event(miner_id, event_type, message, details=None):
    with Session(engine) as session:
        event = MinerEvent(
            miner_id=miner_id,
            event_type=event_type,
            message=message,
            details_json=json.dumps(details) if details is not None else None,
        )
        session.add(event)
        session.commit()


def summary(miner_id=None, hours=24):
    start = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
    rows = _rows(miner_id, start, None)
    if not rows:
        return {"hours": hours, "samples": 0}
    readings = [r for r, _ in rows]
    temps = [r.temperature for r in readings]
    volts = [r.voltage for r in readings]
    rates = [r.hash_rate for r in readings]
    latencies = [r.response_time for r in readings if r.response_time is not None]
    return {
        "hours": hours, "samples": len(readings),
        "hash_rate_avg": sum(rates)/len(rates),
        "hash_rate_min": min(rates), "hash_rate_max": max(rates),
        "temperature_avg": sum(temps)/len(temps),
        "temperature_min": min(temps), "temperature_max": max(temps),
        "voltage_avg": sum(volts)/len(volts),
        "voltage_min": min(volts), "voltage_max": max(volts),
        "latency_avg": sum(latencies)/len(latencies) if latencies else None,
    }
