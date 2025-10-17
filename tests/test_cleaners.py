import pytest

from pipeline.cleaners import mask_pii, is_english

def test_email():
    text = "Contact me at email@example.com"
    cleaned = mask_pii(text)
    assert cleaned == "Contact me at |||EMAIL|||"

def test_ip_address():
    text = "My IP is 192.168.1.1"
    cleaned = mask_pii(text)
    assert cleaned == "My IP is |||IP|||"

def test_is_english():
    assert is_english("I like to code a lot.")
    assert not is_english("Eu gosto muito de programar.")