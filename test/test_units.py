from .utils import process

START = "2021-08-01"
END = "2021-08-10"


def test_auto():
    data = {}
    process(data)


def test_manual():
    data = {
        "start": START,
        "end": END,
    }
    process(data)
