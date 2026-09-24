import os
import random

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--seed",
        type=int,
        default=None,
        help="deterministic seed for key/value generation",
    )


@pytest.fixture(scope="session")
def seed(request):
    value = request.config.getoption("--seed")
    if value is None:
        value = int(os.environ.get("SV_SEED", "0"))
        if value == 0:
            value = random.randrange(1 << 32)
    return value
