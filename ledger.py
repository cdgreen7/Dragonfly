"""
    This file is the ledger service for the repair system.
    It consumes events from the RabbitMQ broker and records them in a SQLite database. It also answers queries over RPC.
    The ledger is responsible for maintaining the state of appointments, including their status, assigned technicians, and notes.
    It ensures that events are applied in a single transaction and that duplicate events are ignored.
    The ledger can be run as a standalone service, and it will create the necessary database schema if it does not exist.
"""
import argparse
import datetime as dt
import json
import os
import sqlite3

import pika

from common import LEDGER_EVENTS_QUEUE, LEDGER_RPC_QUEUE, connect, declare_topology

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
DB_PATH = os.path.join(DATA_DIR, "ledger.db")

# Schema was created with assistance from Claude.
SCHEMA = """
CREATE TABLE IF NOT EXISTS appointments (
    id             TEXT PRIMARY KEY,
    customer       TEXT,
    device         TEXT,
    category       TEXT,              -- 'mobile' | 'mac'
    issue          TEXT,
    priority       INTEGER,
    checked_in_by  TEXT,
    checked_in_at  TEXT,
    status         TEXT NOT NULL DEFAULT 'waiting',  -- waiting | in_progress | completed
    tech           TEXT,              -- current / finishing technician
    taken_at       TEXT,
    ended_at       TEXT,
    reassigned     INTEGER NOT NULL DEFAULT 0
);
-- Every time a technician takes (or releases) an appointment.
CREATE TABLE IF NOT EXISTS assignments (
    appt_id      TEXT NOT NULL REFERENCES appointments(id),
    tech         TEXT NOT NULL,
    action       TEXT NOT NULL,       -- 'taken' | 'released'
    at           TEXT NOT NULL,
    redelivered  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS notes (
    note_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    appt_id  TEXT NOT NULL REFERENCES appointments(id),
    tech     TEXT NOT NULL,
    at       TEXT NOT NULL,
    text     TEXT NOT NULL
);
-- Event IDs already applied: makes duplicate deliveries harmless.
CREATE TABLE IF NOT EXISTS seen_events (
    event_id TEXT PRIMARY KEY
);
CREATE INDEX IF NOT EXISTS idx_appt_checked_in ON appointments(checked_in_at);
CREATE INDEX IF NOT EXISTS idx_assign_tech     ON assignments(tech);
CREATE INDEX IF NOT EXISTS idx_notes_appt      ON notes(appt_id);
"""

APPOINTMENT_FIELDS = ("customer", "device", "category", "issue", "priority",
                 "checked_in_by", "checked_in_at")


class Ledger:
    '''Ledger is a service that records events in a SQLite database and answers queries over RPC.
        It consumes events from the RabbitMQ broker and applies them to the database in a single transaction.
        It is also responsible for answering queries on the `ledger.rpc` queue, such as getting all appointments, appointments for a specific technician, or details of a specific appointment.'''
    
    def __init__(self, path=DB_PATH):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")   # lets you read the DB while the ledger runs
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript(SCHEMA)
        n = self.db.execute("SELECT COUNT(*) FROM appointments").fetchone()[0]
        print(f"[ledger] database {path} ({n} appointments)")


    def _ensure(self, appt_id):
        '''Ensure that an appointment exists in the database. If it does not exist, insert a new row with the given appointment ID.'''
        self.db.execute("INSERT OR IGNORE INTO appointments (id) VALUES (?)", (appt_id,))

    def apply(self, ev):
        """Apply one event in a single transaction. Returns False if already applied."""
        kind = ev["kind"]
        with self.db:  # BEGIN ... COMMIT (or ROLLBACK on exception)
            try:
                self.db.execute("INSERT INTO seen_events (event_id) VALUES (?)", (ev["event_id"],))
            except sqlite3.IntegrityError:
                return False

            if kind == "checked_in":
                t = ev["appointment"]
                cols = ", ".join(APPOINTMENT_FIELDS)
                marks = ", ".join("?" for _ in APPOINTMENT_FIELDS)
                updates = ", ".join(f"{c} = excluded.{c}" for c in APPOINTMENT_FIELDS)
                # Upsert appointment details without touching status/tech (may already be in progress).
                self.db.execute(
                    f"INSERT INTO appointments (id, {cols}) VALUES (?, {marks}) "
                    f"ON CONFLICT(id) DO UPDATE SET {updates}",
                    (t["id"], *(t.get(c) for c in APPOINTMENT_FIELDS)))
                appt_id = t["id"]

            elif kind == "taken":
                appt_id = ev["id"]
                self._ensure(appt_id)
                self.db.execute(
                    "INSERT INTO assignments (appt_id, tech, action, at, redelivered) "
                    "VALUES (?, ?, 'taken', ?, ?)",
                    (appt_id, ev["tech"], ev["at"], int(bool(ev.get("redelivered")))))
                self.db.execute(
                    "UPDATE appointments SET status = 'in_progress', tech = ?, taken_at = ?, "
                    "reassigned = MAX(reassigned, ?) WHERE id = ? AND status != 'completed'",
                    (ev["tech"], ev["at"], int(bool(ev.get("redelivered"))), appt_id))

            elif kind == "note":
                appt_id = ev["id"]
                self._ensure(appt_id)
                self.db.execute("INSERT INTO notes (appt_id, tech, at, text) VALUES (?, ?, ?, ?)",
                                (appt_id, ev["tech"], ev["at"], ev["text"]))

            elif kind == "ended":
                appt_id = ev["id"]
                self._ensure(appt_id)
                self.db.execute("UPDATE appointments SET status = 'completed', ended_at = ?, tech = ? "
                                "WHERE id = ?", (ev["at"], ev["tech"], appt_id))

            elif kind == "released":
                appt_id = ev["id"]
                self._ensure(appt_id)
                self.db.execute("INSERT INTO assignments (appt_id, tech, action, at) "
                                "VALUES (?, ?, 'released', ?)", (appt_id, ev["tech"], ev["at"]))
                self.db.execute("UPDATE appointments SET status = 'waiting', tech = NULL "
                                "WHERE id = ? AND status != 'completed'", (appt_id,))
            else:
                print(f"[ledger] unknown event kind {kind!r}")
                return False

        print(f"[ledger] {kind:10} {appt_id}" + (f" by {ev['tech']}" if ev.get("tech") else ""))
        return True

    def _to_dict(self, row):
        '''Convert a SQLite row to a dictionary, including notes and previous technicians.'''
        r = dict(row)
        r["reassigned"] = bool(r["reassigned"])
        r["notes"] = [dict(n) for n in self.db.execute(
            "SELECT tech, at, text FROM notes WHERE appt_id = ? ORDER BY note_id", (r["id"],))]
        techs = [t for (t,) in self.db.execute(
            "SELECT tech FROM assignments WHERE appt_id = ? AND action = 'taken' "
            "GROUP BY tech ORDER BY MIN(rowid)", (r["id"],))]
        r["previous_techs"] = [t for t in techs if t != r["tech"]]
        return r

    _TODAY_SQL = """
        SELECT * FROM appointments
        WHERE substr(COALESCE(checked_in_at, taken_at), 1, 10) = :today {extra}
        ORDER BY status = 'completed', priority DESC, checked_in_at
    """

    def _today(self, extra="", **params):
        '''Return a list of today's appointments, optionally filtered by extra SQL conditions and parameters.'''
        today = dt.date.today().isoformat()
        rows = self.db.execute(self._TODAY_SQL.format(extra=extra), {"today": today, **params})
        return [self._to_dict(r) for r in rows]

    def query(self, req):
        '''Answer a query from the RPC queue.'''
        cmd = req.get("cmd")
        if cmd == "allappts":
            return {"ok": True, "records": self._today()}
        if cmd == "myappts":
            return {"ok": True, "records": self._today(
                "AND id IN (SELECT appt_id FROM assignments WHERE tech = :tech AND action = 'taken')",
                tech=req.get("tech"))}
        if cmd == "show":
            row = self.db.execute("SELECT * FROM appointments WHERE id = ?",
                                  (req.get("id"),)).fetchone()
            return {"ok": True, "record": self._to_dict(row) if row else None}
        return {"ok": False, "error": f"unknown cmd {cmd!r}"}


def main():
    args = argparse.ArgumentParser(description="Dragonfly ledger (SQLite)")
    args.add_argument("--host", help="RabbitMQ host (default: $RABBIT_HOST or localhost)")
    args.add_argument("--db", default=DB_PATH, help=f"SQLite file (default: {DB_PATH})")
    a = args.parse_args()

    ledger = Ledger(a.db)
    conn = connect(a.host)
    ch = conn.channel()
    declare_topology(ch)
    ch.basic_qos(prefetch_count=50)

    def on_event(ch, method, props, body):
        ledger.apply(json.loads(body))
        ch.basic_ack(method.delivery_tag)  # ack only after the transaction commits

    def on_rpc(ch, method, props, body):
        try:
            resp = ledger.query(json.loads(body))
        except Exception as e:  # never let a bad request kill the ledger
            resp = {"ok": False, "error": str(e)}
        if props.reply_to:
            ch.basic_publish("", props.reply_to, json.dumps(resp),
                             pika.BasicProperties(correlation_id=props.correlation_id))
        ch.basic_ack(method.delivery_tag)

    ch.basic_consume(LEDGER_EVENTS_QUEUE, on_event)
    ch.basic_consume(LEDGER_RPC_QUEUE, on_rpc)
    print("[ledger] running. Ctrl+C to stop.")
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        print("\n[ledger] stopping.")
        ch.stop_consuming()
        conn.close()
        ledger.db.close()


if __name__ == "__main__":
    main()
