"""Open-ended amounts in the Senate matcher.

eFD reports two amounts with no upper bound: "Over $50,000,000" and "Spouse/DC Over $1,000,000".
The matcher used to read them as the fixed amount $50,000,000 / $1,000,000, which put them in
the band BELOW the figure (25,000,001-50,000,000 / 500,001-1,000,000) with a made-up exactAmount.

Run offline, no AWS credentials needed:
    python -m pytest backend_app/src/POLITRADES/politician_trades_senate_matcher/tests
"""
import importlib.util
import os
from pathlib import Path

import pytest

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")  # the module builds boto3 clients at import
_PATH = Path(__file__).resolve().parents[1] / "app" / "lambda_function.py"
_spec = importlib.util.spec_from_file_location("senate_matcher_lambda_function", _PATH)
senate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(senate)

# An eFD transaction table: # | date | owner | ticker | asset | asset type | type | amount | comment
_EFD_TABLE = """<html><body><h2 class="filedReport">The Honorable Test Senator (Senator, Test)</h2>
<table class="table table-striped"><thead><tr><th>#</th><th>Transaction Date</th><th>Owner</th>
<th>Ticker</th><th>Asset Name</th><th>Asset Type</th><th>Type</th><th>Amount</th><th>Comment</th></tr></thead>
<tbody>
<tr><td>1</td><td>08/01/2023</td><td>Self</td><td>AAPL</td><td>Apple Inc.</td><td>Stock</td>
<td>Purchase</td><td>$1,001 - $15,000</td><td>--</td></tr>
<tr><td>2</td><td>08/02/2023</td><td>Joint</td><td>MSFT</td><td>Microsoft Corp</td><td>Stock</td>
<td>Sale (Partial)</td><td>Over $50,000,000</td><td>--</td></tr>
<tr><td>3</td><td>08/03/2023</td><td>Spouse</td><td>--</td><td>US Treasury Bill</td><td>Other Securities</td>
<td>Sale (Full)</td><td>Spouse/DC Over $1,000,000</td><td>--</td></tr>
</tbody></table></body></html>"""


@pytest.fixture(scope="module")
def rows():
    return senate.parse_senate_ptr_html(_EFD_TABLE)


def test_over_50_million_is_above_the_top_band(rows):
    msft = rows[1]
    assert msft['amountRange'] == [50000001, 999999999]
    assert (msft['amountMin'], msft['amountMax'], msft['exactAmount']) == (50000001, None, None)
    assert msft['transactionType'] == 'Sale (Partial)'


def test_spouse_dc_over_1_million_has_no_upper_bound(rows):
    bill = rows[2]
    assert bill['amountRange'] == [1000001, 999999999]
    assert (bill['amountMin'], bill['amountMax'], bill['exactAmount']) == (1000001, None, None)


def test_ordinary_ranges_are_unchanged(rows):
    aapl = rows[0]
    assert (aapl['amountRange'], aapl['amountMin'], aapl['amountMax'], aapl['exactAmount']) == (
        [1001, 15000], 1001, 15000, None)


@pytest.mark.parametrize("text,expected", [
    ("Over $50,000,000", [50000001, 999999999]),
    ("Spouse/DC Over $1,000,000", [1000001, 999999999]),
    ("$1,001 - $15,000", [1001, 15000]),       # unchanged
    ("$15,000", [1001, 15000]),                # unchanged: a fixed amount maps to its band
])
def test_parse_amount_range(text, expected):
    assert senate.parse_amount_range(text) == expected
