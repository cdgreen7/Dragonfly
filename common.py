"""
    This file holds all of the common code used by the nodes for the system.

    Specifically, it holds broker connection settings, the RabbitMQ topology (exchanges + queues),
    event publishing, appointment check-in, the RPC client used to query the ledger,
    and text formatting helpers for the terminal.
"""
import datetime as dt
import json
import os
import shlex
import time
import uuid

import pika

# RabbitMQ connection settings. 
# Override w/ environment vars (or --host on the command line) to point a node at a broker running on another computer on your network.
RABBIT_HOST = os.environ.get("RABBIT_HOST", "localhost")
RABBIT_PORT = int(os.environ.get("RABBIT_PORT", "5672"))
RABBIT_USER = os.environ.get("RABBIT_USER", "dragonfly")
RABBIT_PASS = os.environ.get("RABBIT_PASS", "dragonfly")

# rabbitMQ topology: exchanges, queues, routing keys. 
APPOINTMENT_EXCHANGE = "repairs" 
QUEUE_FOR = {"mobile": "repairs.mobile", "mac": "repairs.mac"} # If an appt is mac -> mac queue, if mobile -> mobile queue.
EVENT_EXCHANGE = "appt.events" # All nodes subscribe to this exchange to receive events about appointments (checked_in, taken, note, ended, released).
LEDGER_EVENTS_QUEUE = "ledger.events" # ledger is subscribed to all events 
LEDGER_RPC_QUEUE = "ledger.rpc"

# priority: 0 = standard, 1 = express. RabbitMQ supports 0-255, but we only need two levels.
MAX_PRIORITY = 1
EXPRESS, STANDARD = 1, 0

MAC_KEYWORDS = ("mac",)  # all mac devices have the word "mac" in their device name, otherwise device = mobile.


def prio_label(p):
    '''helper for displaying the priority'''
    return "Express" if p == EXPRESS else "Standard"

def now_iso():
    '''helper to get the current time in ISO format'''
    return dt.datetime.now().isoformat(timespec="seconds")


def category_for(device):
    '''helper to determine the category of a device based on its name. '''
    d = device.lower()
    return "mac" if any(k in d for k in MAC_KEYWORDS) else "mobile"


def new_appointment_id(node_name):
    '''helper function that generates a unique appointment ID based on the node name and a random hex string.'''
    prefix = "".join(c for c in node_name.upper() if c.isalnum())[:6] or "NODE"
    return f"{prefix}-{uuid.uuid4().hex[:5].upper()}"


def connect(host=None):
    '''connect() returns a new pika.BlockingConnection to the broker, using the default host/port/user/pass'''
    params = pika.ConnectionParameters(
        host=host or RABBIT_HOST, 
        port=RABBIT_PORT, 
        credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASS), 
        heartbeat=0, # disable heartbeats to prevent connection drops due to inactivity (i.e tech is in an appt. and inactive during it).
        connection_attempts=10, 
        retry_delay=3, # essentially, this gives the broker 30 seconds to come up before the node gives up trying to connect.
    )
    return pika.BlockingConnection(params)


def declare_topology(ch):
    ''' declare_topology() creates the exchanges and queues in RabbitMQ if they don't already exist.
        We have to declare the topology in code because RabbitMQ doesn't have a built-in way to persist topology across restarts.
        We then bind the queues to the exchanges with the appropriate routing keys because we want to route appointments to the correct queue based on their category (mac or mobile). '''
    ch.exchange_declare(APPOINTMENT_EXCHANGE, exchange_type="direct", durable=True) # this is a direct exchange because we want to route appointments to the correct queue based on their category (mac or mobile).
    for key, queue in QUEUE_FOR.items(): 
        ch.queue_declare(queue, durable=True, arguments={"x-max-priority": MAX_PRIORITY}) # declare the queues for each category (mac and mobile) with max priority support.
        ch.queue_bind(queue, APPOINTMENT_EXCHANGE, routing_key=key) 

    ch.exchange_declare(EVENT_EXCHANGE, exchange_type="fanout", durable=True) # fanout exchange because we want to broadcast events to all nodes, regardless of category.
    ch.queue_declare(LEDGER_EVENTS_QUEUE, durable=True) 
    ch.queue_bind(LEDGER_EVENTS_QUEUE, EVENT_EXCHANGE)

    ch.queue_declare(LEDGER_RPC_QUEUE, durable=True)


def publish_event(ch, kind, **fields):
    ''' publish_event() publishes an event to the EVENT_EXCHANGE with the given kind and fields.
        it takes in 3 parameters: ch (the channel to publish on), kind (the kind of event), and fields (any additional fields to include in the event).
        The event is a dictionary with an event_id, kind, at (timestamp), and any additional fields passed in as keyword arguments. 
        The event is published as a persistent JSON string. '''
    event = {"event_id": uuid.uuid4().hex, "kind": kind, "at": now_iso(), **fields}
    ch.basic_publish(
        exchange=EVENT_EXCHANGE,
        routing_key="",
        body=json.dumps(event),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    return event


def submit_appointment(ch, node_name, customer, device, issue, priority):
    ''' submit_appointment() creates a new repair appointment and publishes it to the exchange with the appropriate routing key based on the device category (mac or mobile).
        It takes in 6 parameters: 
        ch (the channel to publish on), node_name (the name of the node submitting the appointment), customer (the name of the customer), device (the name of the device), 
        issue (the issue with the device), and priority (the priority of the appointment). '''
    category = category_for(device)
    appointment = {
        "id": new_appointment_id(node_name),
        "customer": customer,
        "device": device,
        "category": category,
        "issue": issue,
        "priority": int(priority),
        "checked_in_by": node_name,
        "checked_in_at": now_iso(),
    }
    # publish the appointment to the APPOINTMENT_EXCHANGE with the appropriate routing key based on the device category (mac or mobile).
    ch.basic_publish(
        exchange=APPOINTMENT_EXCHANGE,
        routing_key=category,
        body=json.dumps(appointment),
        properties=pika.BasicProperties(
            priority=appointment["priority"], delivery_mode=2, content_type="application/json"
        ),
        mandatory=True,  # with confirm_delivery(): raises if no queue got it
    )
    publish_event(ch, "checked_in", appointment=appointment)
    return appointment


class RpcClient:
    """
        Request/reply to the ledger over RabbitMQ (reply_to + correlation_id).
    """

    def __init__(self, conn, ch):
        self.conn, self.ch = conn, ch
        self.reply_queue = ch.queue_declare("", exclusive=True).method.queue
        self.responses = {}
        ch.basic_consume(self.reply_queue, self._on_reply, auto_ack=True)

    def _on_reply(self, ch, method, props, body):
        ''' callback for when a reply is received from the ledger. 
        It takes in 4 parameters: ch (the channel), method (the method frame), props (the properties), and body (the body of the message).
        It stores the reply in the responses dictionary using the correlation_id as the key. This allows the call() method to retrieve the reply later. '''
        self.responses[props.correlation_id] = json.loads(body)

    def call(self, request, timeout=5.0):
        ''' call() sends a request to the ledger and waits for a reply. It takes in 2 parameters: request (the request to send) and timeout (the maximum time to wait for a reply).
            It returns the reply from the ledger, or None if the timeout is reached.'''
        cid = uuid.uuid4().hex
        self.responses.clear()  # drop late replies to earlier, timed-out calls
        self.ch.basic_publish(  # send the request to the ledger 
            exchange="",
            routing_key=LEDGER_RPC_QUEUE,
            body=json.dumps(request),
            properties=pika.BasicProperties(
                reply_to=self.reply_queue,
                correlation_id=cid,
                expiration=str(int(timeout * 1000)),  # drop stale requests
            ),
        )
        deadline = time.time() + timeout
        while cid not in self.responses and time.time() < deadline:
            self.conn.process_data_events(time_limit=0.2)
        return self.responses.pop(cid, None)


# functions for formatting output in the terminal. These are used by the nodes to display appointments and events in a human-readable format. 
# These functions were written by Claude.

# helper to clip a string to n characters, adding an ellipsis if it was clipped. Returns "" if s is None.
def _clip(s, n):
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: n - 1] + "…"

# helper to extract the HH:MM portion of an ISO timestamp. Returns "" if iso is None.
def _hhmm(iso):
    return iso[11:16] if iso else ""

# helper to print a table of records with a title. Each record is a dictionary with keys: id, customer, device, issue, priority, checked_in_at, status, tech, notes.
def print_table(records, title):
    print(f"\n== {title} ({len(records)}) ==")
    if not records:
        print("  (none)")
        return
    cols = [("ID", 12), ("Customer", 14), ("Device", 16), ("Issue", 20),
            ("Type", 8), ("In", 5), ("Status", 11), ("Tech", 10), ("Notes", 5)]
    print("  " + " ".join(h.ljust(w) for h, w in cols))
    print("  " + " ".join("-" * w for _, w in cols))
    for r in records:
        status = r.get("status", "")
        if r.get("reassigned") and status == "in_progress":
            status = "in_prog(R)"
        row = [r.get("id"), r.get("customer"), r.get("device"), r.get("issue"),
               prio_label(r.get("priority")), _hhmm(r.get("checked_in_at")), status,
               r.get("tech") or "-", len(r.get("notes", []))]
        print("  " + " ".join(_clip(v, w).ljust(w) for v, (_, w) in zip(row, cols)))
    print()

# helper to print a single record in a human-readable format. Each record is a dictionary with keys: id, customer, device, issue, priority, checked_in_at, status, tech, notes.
def print_record(r):
    print(f"\n#{r['id']}  {r.get('device')} ({r.get('category')})  {prio_label(r.get('priority'))}")
    print(f"  Customer:      {r.get('customer')}")
    print(f"  Issue:         {r.get('issue')}")
    print(f"  Checked in:    {r.get('checked_in_at')} by {r.get('checked_in_by')}")
    print(f"  Status:        {r.get('status')}" + ("  (reassigned)" if r.get("reassigned") else ""))
    print(f"  Technician:    {r.get('tech') or '-'}"
          + (f"  (previously: {', '.join(r['previous_techs'])})" if r.get("previous_techs") else ""))
    if r.get("taken_at"):
        print(f"  Started:       {r['taken_at']}")
    if r.get("ended_at"):
        print(f"  Ended:         {r['ended_at']}")
    for n in r.get("notes", []):
        print(f"  - [{_hhmm(n['at'])} {n['tech']}] {n['text']}")
    print()

# helper to parse a text argument that may be quoted or unquoted. Returns the text without quotes and stripped of whitespace.
def parse_text_arg(arg):
    """Accept: writenotes "some text"  or  writenotes some text"""
    try:
        return " ".join(shlex.split(arg)).strip()
    except ValueError:  # unbalanced quotes
        return arg.strip().strip('"').strip("'")
