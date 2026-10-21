"""Load test of the card support tools, in the mix a support conversation calls them (report: --html <file>).

REST for the four tools plus the same card lookup over MCP (JSON-RPC tools/call), so the MCP layer's overhead
shows up next to the plain REST call. Card ids come from CARDS_FILE (one per line, real cards so Redis lookups hit)
or are random.
Usage: locust -f services/card_support_mcp/locustfile.py --host http://127.0.0.1:8000 --headless -u 10 -r 5 -t 2m
"""
import os
import random

from locust import HttpUser, between, task

CARDS = open(os.environ["CARDS_FILE"]).read().split() if os.environ.get("CARDS_FILE") else []
QUESTIONS = ["Someone used my card abroad, am I liable?", "What is the foreign transaction fee?",
             "How do I dispute a payment?", "Why was my online payment asked for an OTP?", "How do I lock my card?"]
CHUNKS = ["card_terms%234.2", "faq%233", "fee_schedule%232", "dispute_policy%231.1", "authentication_rules%232.1"]
MCP_HEADERS = {"Accept": "application/json, text/event-stream", "Content-Type": "application/json"}


def card():
    """A card id from the sample list, or a random one when none is given."""
    return random.choice(CARDS) if CARDS else f"card_{random.randrange(130_000):06d}"


class SupportAgent(HttpUser):
    """One agent working a chat: reads the card, searches the policy, scores the payment asked about."""

    wait_time = between(0.05, 0.3)

    @task(4)
    def card_data(self):
        """GET a card's data; an unknown random card (404) counts as a valid answer."""
        with self.client.get(f"/v1/cards/{card()}", name="/v1/cards/{id}", catch_response=True) as r:
            if r.status_code == 404:   # random ids: an unknown card is a valid answer
                r.success()

    @task(3)
    def search(self):
        """Policy search for one of the sample questions."""
        self.client.post("/v1/search", json={"question": random.choice(QUESTIONS), "k": 4})

    @task(1)
    def chunk(self):
        """GET one known policy chunk by id."""
        self.client.get(f"/v1/chunks/{random.choice(CHUNKS)}", name="/v1/chunks/{id}")

    @task(2)
    def score(self):
        """Score a random payment on a card."""
        self.client.post("/v1/score", json={"card_id": card(), "customer_id": "cus_load",
                                            "amount_vnd": random.choice([250_000, 1_200_000, 6_000_000]),
                                            "channel": random.choice(["ecom", "pos", "atm"])})

    @task(2)
    def mcp_card_data(self):
        """Call the MCP tool get_card_data over JSON-RPC; a tool error counts as a failure."""
        body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "get_card_data", "arguments": {"card_id": card()}}}
        with self.client.post("/mcp", json=body, headers=MCP_HEADERS, name="mcp get_card_data",
                              catch_response=True) as r:
            if r.ok and r.json().get("result", {}).get("isError"):
                r.failure("tool error")
