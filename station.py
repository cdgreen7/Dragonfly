"""
    This file implements the interface for technicians / front desk staff to check in customers, take appointments, write notes, and finish appointments. 
    It connects to the RabbitMQ broker and communicates with the ledger over RPC.
"""
import argparse
import cmd
import json

from common import (EXPRESS, QUEUE_FOR, STANDARD, RpcClient, connect, declare_topology,
                    parse_text_arg, prio_label, print_record, print_table,
                    publish_event, submit_ticket)


class StationError(Exception):
    """A problem to show the user (no active appointment, ledger offline...)."""


class Station(cmd.Cmd):
    """
    Station is a command-line interface for technicians and front desk staff to manage appointments. It connects to RabbitMQ and communicates with the ledger over RPC.
    Commands:
        checkin - check in a customer (prompts for details)
        takenext - take the next appointment for your role (express first)
        writenotes "text" - add a note to your current appointment
        endappt - finish your current appointment
        release - put your current appointment back in the queue
        current - details and notes for your current appointment
        myappts - appointments you've taken today
        allappts - every appointment today
        show (id) - full details and notes for one appointment
        queues - how many tickets are waiting in each queue
        quit - exit (an unfinished appointment goes back to the queue)
    """

    def __init__(self, name, role, host=None):
        super().__init__()  # let cmd.Cmd set itself up first
        self.name, self.role = name, role
        self.conn = connect(host)
        self.ch = self.conn.channel()
        self.ch.confirm_delivery()  # basic_publish raises if the broker didn't take it
        declare_topology(self.ch)
        self.rpc = RpcClient(self.conn, self.ch)

        self.pull_order = ([QUEUE_FOR["mac"], QUEUE_FOR["mobile"]] if role == "mac"
                           else [QUEUE_FOR["mobile"]])
        self.current = None  # (delivery_tag, ticket) while holding an appointment

        self.prompt = f"{name}({role})> "
        self.intro = (f"Station {name} connected. takenext pulls from: "
                      f"{' -> '.join(self.pull_order)}\nType 'help' for commands.")

    def onecmd(self, line):
        '''Print friendly errors instead of a traceback.'''
        try:
            return super().onecmd(line)
        except StationError as e:
            print(e)

    def emptyline(self):
        '''Do nothing on empty input (don't repeat the last command).'''
        pass

    def default(self, line):
        '''Handle unknown commands.'''
        print(f"Unknown command: {line.split()[0]}  (type 'help')")

    def ledger(self, request):
        """"""
        resp = self.rpc.call(request)
        if resp is None:
            raise StationError("Ledger is not responding (is ledger.py running?)")
        if not resp.get("ok"):
            raise StationError(resp.get("error", "ledger error"))
        return resp

    def require_current(self):
        '''Return the current appointment, or raise an error if there isn't one.'''
        if not self.current:
            raise StationError("No active appointment. Use 'takenext' to start one.")
        return self.current

    # commands for the user
    
    def do_checkin(self, arg):
        """checkin - check in a customer (prompts for details)"""
        try:
            customer = input("  Customer name: ").strip()
            device = input("  Device (e.g. iPhone 15, MacBook Pro): ").strip()
            issue = input("  Issue: ").strip()
            express = input("  Express? [y/N]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\n  Check-in cancelled.")
            return
        if not (customer and device and issue):
            raise StationError("  Customer, device and issue are required.")
        if express not in ("", "y", "yes", "n", "no"):
            raise StationError("  Answer y or n.")
        priority = EXPRESS if express in ("y", "yes") else STANDARD
        t = submit_ticket(self.ch, self.name, customer, device, issue, priority)
        print(f"  Checked in #{t['id']} -> {QUEUE_FOR[t['category']]} queue "
              f"({prio_label(priority)})")

    def do_takenext(self, arg):
        """takenext - take the next appointment for your role (express first)"""
        if self.current:
            raise StationError(f"You're still on #{self.current[1]['id']}. "
                               "Use 'endappt' (or 'release') first.")
        for q in self.pull_order:
            method, props, body = self.ch.basic_get(q, auto_ack=False)
            if method is None:
                continue  # nothing waiting here, try the next queue
            t = json.loads(body)
            self.current = (method.delivery_tag, t)
            publish_event(self.ch, "taken", id=t["id"], tech=self.name,
                          redelivered=method.redelivered)
            overflow = self.role == "mac" and q == QUEUE_FOR["mobile"]
            print(f"Now serving #{t['id']}: {t['device']} - {t['issue']} "
                  f"({prio_label(t['priority'])}, customer {t['customer']})"
                  + ("  [overflow from mobile queue]" if overflow else "")
                  + ("  [REASSIGNED - a previous station released or dropped it]"
                     if method.redelivered else ""))
            return
        print("No appointments waiting.")

    def do_writenotes(self, arg):
        """writenotes "text" - add a note to your current appointment"""
        _, t = self.require_current()
        text = parse_text_arg(arg)
        if not text:
            raise StationError('Usage: writenotes "your note"')
        publish_event(self.ch, "note", id=t["id"], tech=self.name, text=text)
        print(f"Note added to #{t['id']}.")

    def do_endappt(self, arg):
        """endappt - finish your current appointment"""
        tag, t = self.require_current()
        publish_event(self.ch, "ended", id=t["id"], tech=self.name)
        self.ch.basic_ack(delivery_tag=tag)  # now it's gone from the queue for good
        self.current = None
        print(f"Appointment #{t['id']} ended.")

    def do_release(self, arg):
        """release - put your current appointment back in the queue"""
        tag, t = self.require_current()
        publish_event(self.ch, "released", id=t["id"], tech=self.name)
        self.ch.basic_nack(delivery_tag=tag, requeue=True)
        self.current = None
        print(f"#{t['id']} returned to the queue.")

    def do_current(self, arg):
        """current - details and notes for your current appointment"""
        _, t = self.require_current()
        print_record(self.ledger({"cmd": "show", "id": t["id"]})["record"])

    def do_myappts(self, arg):
        """myappts - appointments you've taken today"""
        print_table(self.ledger({"cmd": "myappts", "tech": self.name})["records"],
                    f"{self.name}'s appointments today")

    def do_allappts(self, arg):
        """allappts - every appointment today"""
        print_table(self.ledger({"cmd": "allappts"})["records"], "All appointments today")

    def do_show(self, arg):
        """show (id) - full details and notes for one appointment"""
        if not arg.strip():
            raise StationError("Usage: show (id)")
        record = self.ledger({"cmd": "show", "id": arg.strip().upper()})["record"]
        if not record:
            raise StationError(f"No appointment {arg.strip()}")
        print_record(record)

    def do_queues(self, arg):
        """queues - how many tickets are waiting in each queue"""
        for q in QUEUE_FOR.values():
            n = self.ch.queue_declare(q, passive=True).method.message_count
            print(f"  {q:16} {n} waiting")

    def do_quit(self, arg):
        """quit - exit (an unfinished appointment goes back to the queue)"""
        if self.current:
            print(f"Note: #{self.current[1]['id']} was not ended; it will return to the queue.")
        try:
            self.conn.close()
        except Exception:
            pass
        return True

    do_exit = do_quit

    def do_EOF(self, arg):  # Ctrl+D
        print()
        return self.do_quit(arg)


def main():
    ap = argparse.ArgumentParser(description="Dragonfly station (technician or front desk)")
    ap.add_argument("--name", required=True)
    ap.add_argument("--role", choices=["mobile", "mac"], required=True,
                    help="which queue(s) takenext pulls from")
    ap.add_argument("--host", help="RabbitMQ host (default: $RABBIT_HOST or localhost)")
    a = ap.parse_args()
    station = Station(a.name, a.role, a.host)
    try:
        station.cmdloop()
    except KeyboardInterrupt:
        print("\nExiting.")


if __name__ == "__main__":
    main()
