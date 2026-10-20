"""Equivalence partitions and boundary values of the tool inputs (what pydantic lets through to the stores)."""
import pytest
from pydantic import ValidationError

from app.schemas import CHUNK_ID, ScorePaymentRequest, SearchRequest
import re

BASE = {"card_id": "card_00001", "customer_id": "cus_00001", "amount_vnd": 250_000, "channel": "ecom"}


@pytest.mark.parametrize("amount, ok", [(-1, False), (0, False), (1, True), (10_000_000_000, True),
                                        (10_000_000_001, False)])
def test_amount_boundaries(amount, ok):
    if ok:
        assert ScorePaymentRequest(**BASE | {"amount_vnd": amount}).amount_vnd == amount
    else:
        with pytest.raises(ValidationError):
            ScorePaymentRequest(**BASE | {"amount_vnd": amount})


@pytest.mark.parametrize("field, value, ok", [
    ("currency", "USD", True), ("currency", "usd", False), ("currency", "US", False), ("currency", "USDT", False),
    ("channel", "pos", True), ("channel", "atm", True), ("channel", "wire", False),
    ("card_id", "", False), ("card_id", "c" * 64, True), ("card_id", "c" * 65, False),
    ("card_id", "card 1; drop", False), ("device_id", None, True), ("device_id", "dev_01", True),
])
def test_score_request_partitions(field, value, ok):
    body = BASE | {field: value}
    if ok:
        ScorePaymentRequest(**body)
    else:
        with pytest.raises(ValidationError):
            ScorePaymentRequest(**body)


def test_score_request_defaults_to_vnd():
    assert ScorePaymentRequest(**BASE).currency == "VND"


@pytest.mark.parametrize("length, ok", [(2, False), (3, True), (500, True), (501, False)])
def test_question_length_boundaries(length, ok):
    if ok:
        SearchRequest(question="q" * length)
    else:
        with pytest.raises(ValidationError):
            SearchRequest(question="q" * length)


@pytest.mark.parametrize("k, ok", [(0, False), (1, True), (10, True), (11, False)])
def test_search_k_boundaries(k, ok):
    if ok:
        assert SearchRequest(question="fees?", k=k).k == k
    else:
        with pytest.raises(ValidationError):
            SearchRequest(question="fees?", k=k)


@pytest.mark.parametrize("chunk_id, ok", [("card_terms#4.2", True), ("faq#3", True), ("fee_schedule#10", True),
                                          ("card_terms", False), ("CARD#1", False), ("faq#", False),
                                          ("faq#1.2.3", False), ("x" * 41 + "#1", False)])
def test_chunk_id_pattern(chunk_id, ok):
    assert bool(re.fullmatch(CHUNK_ID, chunk_id)) is ok
