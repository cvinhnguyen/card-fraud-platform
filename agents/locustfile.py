"""Load test of the coordinator over A2A through agentgateway (scaling demo: KEDA adds replicas as the rate grows).

Each user is a customer chat: one question per A2A message/send, waiting for the full answer. The key is the
support-app key (A2A_KEY_SUPPORT_APP), which the gateway's /a2a/coordinator route requires.
Usage: A2A_KEY=<key> locust -f agents/locustfile.py --host http://agentgateway-proxy.agentgateway-system \
         --headless -u 8 -r 1 -t 5m --html report.html
"""
import os
import random
import uuid

from locust import HttpUser, between, task

CARDS = ["card_00006087", "card_00013ea5", "card_00019614", "card_00021497"]
QUESTIONS = [
    "How many payments did card {card} make in the last 30 days?",
    "Will a {amount} VND online payment on card {card} go through?",
    "My {amount} VND payment at a shop with card {card} was declined, why?",
    "What is the foreign transaction fee?",
    "Thẻ {card} của tôi bị dùng ở nước ngoài mà tôi không làm, tôi có phải chịu trách nhiệm không?",
]


class Customer(HttpUser):
    """One chat with the bank's card support assistant."""

    wait_time = between(1, 5)

    @task
    def ask(self):
        """Send one support question to the coordinator over A2A; a reply not 'completed' counts as a failure."""
        text = random.choice(QUESTIONS).format(card=random.choice(CARDS),
                                               amount=f"{random.choice([450_000, 4_500_000, 45_000_000]):,}")
        body = {"jsonrpc": "2.0", "id": 1, "method": "message/send",
                "params": {"message": {"role": "user", "messageId": uuid.uuid4().hex,
                                       "parts": [{"kind": "text", "text": text}]}}}
        with self.client.post("/a2a/coordinator", json=body, name="A2A message/send", timeout=300,
                              headers={"Authorization": f"Bearer {os.environ['A2A_KEY']}"},
                              catch_response=True) as r:
            state = (r.json().get("result") or {}).get("status", {}).get("state") if r.ok else None
            if r.ok and state != "completed":
                r.failure(f"task {state}")
