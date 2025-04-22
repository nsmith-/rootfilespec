from dataclasses import dataclass


@dataclass
class Base1:
    a: int
    b: int


@dataclass
class Base2:
    c: int
    d: int


@dataclass
class Derived(Base1, Base2):
    e: int


def test_dataclass_order():
    assert list(Derived.__dataclass_fields__.keys()) == ["c", "d", "a", "b", "e"]
