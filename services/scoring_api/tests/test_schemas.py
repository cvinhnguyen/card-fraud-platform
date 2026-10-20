"""Request validation through the HTTP API (422 before any lookup). Equivalence partitions + boundary values:

  amount_vnd  invalid <= 0 | valid 1 .. 10,000,000,000 | invalid above
  currency    valid 3 upper-case letters | invalid lower case, 2 or 4 letters
  channel     valid ecom / pos / atm | invalid anything else
  ids         valid 1 .. 64 chars | invalid empty, 65 chars
  device_id   optional: missing / null valid, 64 chars valid, 65 invalid
"""
import pytest

from tests.conftest import request_body


@pytest.mark.parametrize("amount, status", [
    (-1, 422), (0, 422),                          # invalid partition, boundary 0
    (1, 200), (250_000, 200),                     # valid lower boundary, inside
    (10_000_000_000, 200), (10_000_000_001, 422),  # upper boundary, just above
])
def test_amount_boundaries(client, amount, status):
    assert client.post("/v1/score", json=request_body(amount_vnd=amount)).status_code == status


@pytest.mark.parametrize("currency, status", [("VND", 200), ("USD", 200), ("vnd", 422), ("VN", 422), ("VNDX", 422)])
def test_currency_partitions(client, currency, status):
    assert client.post("/v1/score", json=request_body(currency=currency)).status_code == status


@pytest.mark.parametrize("channel, status", [("ecom", 200), ("pos", 200), ("atm", 200), ("moto", 422), ("", 422)])
def test_channel_partitions(client, channel, status):
    assert client.post("/v1/score", json=request_body(channel=channel)).status_code == status


@pytest.mark.parametrize("field", ["auth_id", "card_id", "customer_id", "merchant_id"])
@pytest.mark.parametrize("length, status", [(0, 422), (1, 200), (64, 200), (65, 422)])
def test_id_length_boundaries(client, field, length, status):
    assert client.post("/v1/score", json=request_body(**{field: "x" * length})).status_code == status


@pytest.mark.parametrize("device, status", [(None, 200), ("d" * 64, 200), ("d" * 65, 422)])
def test_device_is_optional(client, device, status):
    assert client.post("/v1/score", json=request_body(device_id=device)).status_code == status


def test_missing_required_field(client):
    body = request_body()
    del body["card_id"]
    r = client.post("/v1/score", json=body)
    assert r.status_code == 422 and r.json()["detail"][0]["loc"][-1] == "card_id"
