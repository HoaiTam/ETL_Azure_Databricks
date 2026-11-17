from __future__ import annotations

import pytest

from common.text import norm


@pytest.mark.parametrize(
    ("ic_name", "ic_normalized", "off_name", "off_normalized"),
    [
        ("Banana", "banana", "Fresh Banana", "fresh banana"),
        (
            "Robust Golden Unsweetened Oolong Tea",
            "robust golden unsweetened oolong tea",
            "Oolong Tea (Pokka)",
            "oolong tea pokka",
        ),
        ("Organic Strawberries", "strawberries", "Yaourt fraise biologique", "yaourt fraise biologique"),
        ("Bag of Organic Bananas", "bananas", "Plastic Bag", "plastic bag"),
    ],
)
def test_dictionary_examples(ic_name, ic_normalized, off_name, off_normalized):
    assert norm(ic_name) == ic_normalized
    assert norm(off_name) == off_normalized
    # Chuẩn hóa không được tự biến các cặp khác sản phẩm thành cùng một tên.
    assert norm(ic_name) != norm(off_name)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        (None, ""),
        ("", ""),
        (" \t\n...!!! ", ""),
        ("  ORGANIC   Strawberries  ", "strawberries"),
        ("Bag-of Organic Bananas", "bananas"),
        ('Tea, Green "Classic"', "tea green classic"),
        ("Milk 12.5 oz", "milk"),
        ("Tea 16 fl oz", "tea"),
        ("Coffee 1.25lb Pack", "coffee"),
        ("Juice 250ml", "juice"),
        ("Rice 2 kg", "rice"),
        ("Eggs 12 ct", "eggs"),
        ("Vitamin B12 500 mg", "vitamin b12 500 mg"),
        ("7 Grain Bread", "7 grain bread"),
        ("organicity backpack", "organicity backpack"),
    ],
)
def test_noise_measurements_and_word_boundaries(name, expected):
    assert norm(name) == expected
    assert norm(norm(name)) == expected
