"""
    This file simulates online bookings for the repair queue. 
    It randomly generates customer names, devices, and issues, and submits them to a RabbitMQ queue with either standard or express priority. 
    The script can run indefinitely or for a specified number of bookings, with configurable time intervals between bookings.
    When going to run, you can specify the number of bookings to make, the minimum and maximum time between bookings, and the RabbitMQ host to connect to.
    --count N: Stop after N bookings (default: 0, which means run indefinitely)
    --min SECONDS: Minimum seconds between bookings (default: 5)
    --max SECONDS: Maximum seconds between bookings (default: 15)
    --host HOST: RabbitMQ host (default: $RABBIT_HOST or localhost)
"""
import argparse
import random
import time

from common import EXPRESS, STANDARD, connect, declare_topology, prio_label, submit_appointment

# random customer names and devices/issues for the simulation
CUSTOMERS = ["Avery Park", "Jordan Lee", "Riley Chen", "Morgan Diaz", "Casey Nguyen",
             "Taylor Brooks", "Jamie Patel", "Quinn Rivera", "Drew Kim", "Sky Johnson"]
JOBS = [
    ("iPhone 15", "Cracked screen"), ("iPhone 13", "Battery drains fast"),
    ("iPhone 14 Pro", "Camera won't focus"), ("iPad Air", "Won't charge"),
    ("Apple Watch Series 9", "Display flicker"), ("AirPods Pro", "Left bud no sound"),
    ("Apple TV 4K", "No HDMI signal"), ("iPod touch", "Stuck on logo"),
    ("MacBook Pro 14", "Battery service"), ("MacBook Air M2", "Keyboard keys sticking"),
    ("iMac 24", "Won't boot"), ("Mac mini", "Wi-Fi drops"), ("Mac Studio", "Fan noise"),
]
EXPRESS_CHANCE = 0.25  # modify this to change the odds of express appointments. By default 25% of appointments are express.


def main():
    ap = argparse.ArgumentParser(description="Simulated online bookings")
    ap.add_argument("--count", type=int, default=0, help="stop after N (0 = forever)") 
    ap.add_argument("--min", type=float, default=5, help="min seconds between bookings")
    ap.add_argument("--max", type=float, default=15, help="max seconds between bookings")
    ap.add_argument("--host", help="RabbitMQ host (default: $RABBIT_HOST or localhost)")
    
    a = ap.parse_args()

    # connect to RabbitMQ and declare the necessary queues and exchanges
    conn = connect(a.host)
    ch = conn.channel()
    ch.confirm_delivery()
    declare_topology(ch)

    n = 0
    try:
        while not a.count or n < a.count:
            device, issue = random.choice(JOBS)
            prio = EXPRESS if random.random() < EXPRESS_CHANCE else STANDARD
            t = submit_appointment(ch, "Online", random.choice(CUSTOMERS), device, issue, prio)
            n += 1
            print(f"[online] booked #{t['id']}  {device:22} {prio_label(prio):8}  -> {t['category']}")
            time.sleep(random.uniform(a.min, a.max))
    except KeyboardInterrupt:
        print("\n[online] stopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
