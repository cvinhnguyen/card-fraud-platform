"""Checks of the eval scoring (no gateway needed): python agents/eval/test_run_eval.py, or pytest."""
from run_eval import DOWN_QUESTIONS, correct, down_ok

DECLINED, FEE = (q["question"] for q in DOWN_QUESTIONS)


def test_correct_needs_the_whole_fact():
    assert correct("Fee: **3%** of the amount.", [["3%"]])
    assert correct("It costs 100,000 VND.", [["100,000"]])
    assert correct("No, you are not liable; the fee is 3%", [["3%"]])
    assert not correct("The fee is 1.3% of the amount", [["3%"]])
    assert not correct("The fee is 13%", [["3%"]])
    assert not correct("It costs 11,000,000 VND", [["1,000,000"]])
    assert not correct("within 160 days", [["60 days"]])
    assert not correct("It is not free.", [["free"]])
    assert not correct("The card does not need to be replaced", [["replaced"]])


def test_down_ok():
    # the answers of the fixed prompt (Qwen3-8B, 2026-11-12)
    assert down_ok("The card has not been used in the last hour, with 3 payments in the last 30 days, averaging about "
                   "383,800 VND. It is a credit card on the Napas network, issued in April 2016. The risk agent could "
                   "not check the payment risk or dispute period due to an error. You can ask again later for this "
                   "information.", DECLINED)
    assert down_ok("The card with ID card_d10f799f made 3 payments in the last 30 days, averaging about 383,800 VND. "
                   "The foreign transaction fee could not be checked right now. Please try again later.", FEE)
    assert down_ok("I could not check how risky the 2,500,000 VND payment is. Please ask again later.", DECLINED)
    # the answer of the old prompt, and invented facts next to an honest sentence
    assert not down_ok("For the payment of 2,500,000 VND, the risk score may be high due to the unusual amount. You "
                       "can dispute it within 7 days. I could not check the risk.", DECLINED)
    assert not down_ok("The foreign fee is 50,000 VND. I could not check the risk right now.", FEE)
    assert not down_ok("The payment looks suspicious, likely declined as fraud (high-risk). Please try again later.",
                       DECLINED)
    assert not down_ok("The card made 3 payments in the last 30 days.", FEE)   # says nothing about the failed part


if __name__ == "__main__":
    test_correct_needs_the_whole_fact()
    test_down_ok()
    print("ok")
