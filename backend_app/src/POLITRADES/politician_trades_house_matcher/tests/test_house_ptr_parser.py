"""Tests for the House PTR text parser (app/house_ptr_parser.py) and its wiring in the Lambda.

Every fixture is real pypdf output (see house_ptr_fixtures.py). Each edge-case test below fails
on the parser this module replaced (commit 88c60f6), which:
  1. required the [XX] asset tag, so 2015-2018 filings yielded no rows at all;
  2. read tickers with \\(([A-Z]{1,5})\\): no "(aaPl)", no "BRK.B";
  3. took the FIRST parenthesis, so "(SP)" beat "(VLo)";
  4. matched the type letter upper-case only and merged "S (partial)" into "Sale";
  5. could not read "Over $50,000,000" / "Spouse/DC Over $1,000,000";
  6. never flagged options except through the [OP] tag's asset type;
  7. never read the filing status, amended or not.

Run offline, no AWS credentials needed:
    python -m pytest backend_app/src/POLITRADES/politician_trades_house_matcher/tests
"""
import io
import os
import sys
from pathlib import Path

import pytest

APP_DIR = Path(__file__).resolve().parents[1] / "app"
sys.path.insert(0, str(APP_DIR))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")  # the Lambda module builds boto3 clients at import

import house_ptr_parser as hp  # noqa: E402
from house_ptr_fixtures import (  # noqa: E402
    PTR_2016_AMENDED, PTR_2016_COLGATE_CL, PTR_2016_DEERE_DE, PTR_2016_GLUED_OWNER_AND_ACCOUNT,
    PTR_2016_NO_TAGS, PTR_2016_PAGE_SPLIT_ROW, PTR_2016_SPOUSE_DC_OVER_1M, PTR_2018_LOWERCASE_TYPE_AND_TAG,
    PTR_2019_EXCHANGE, PTR_2019_GLUED_TYPE_LETTERS, PTR_2019_PAREN_IN_GLUED_DESCRIPTION, PTR_2019_RUSHA,
    PTR_2020_DOTTED_AND_LOWERCASE_TICKERS, PTR_2020_OPTIONS, PTR_2020_OVER_50M, PTR_2022_AMENDED,
    PTR_2022_OVER_50M_WRAPPED, PTR_2025_MODERN, PTR_MODELLED_ADR_THEN_TICKER, PTR_MODELLED_OPTION_CARE,
)

# The asset-code mapping the Lambda loads from S3 (static-files/mappings/house_ptr_asset_codes.csv).
ASSET_CODES = {
    'CS': 'Corporate Securities (Bonds and Notes)', 'OP': 'Options', 'OT': 'Other',
    'PS': 'Stock (Not Publicly Traded)', 'ST': 'Stocks (including ADRs)',
}

# The keys the matcher always consumed, plus the two this change adds.
ORIGINAL_KEYS = {
    'filerName', 'filingDate', 'transactionDate', 'securityName', 'securitySymbol', 'assetType',
    'transactionType', 'amount', 'amountMin', 'amountMax', 'amountRange', 'exactAmount', 'owner',
    'formType', 'source', 'metadata',
}


def parse(text):
    return hp.parse_house_ptr_text(text, ASSET_CODES)


def col(rows, key):
    return [r[key] for r in rows]


# ── 1. Filings without the [XX] asset tag (2015-2018) ──────────────────────────

class TestUntaggedFilings:
    def test_2016_rows_are_parsed_without_asset_tags(self):
        rows = parse(PTR_2016_NO_TAGS)
        assert col(rows, 'securitySymbol') == ['BCS', 'LYG', 'LYG', 'LYG', 'MAXD', 'MAXD']
        assert col(rows, 'transactionDate') == ['2016-06-27', '2016-06-28', '2016-06-30', '2016-07-06',
                                                '2016-07-14', '2016-05-05']
        assert col(rows, 'assetType') == [None] * 6
        assert {r['filerName'] for r in rows} == {'Adam Kinzinger'}

    def test_owner_codes_and_names_glued_to_account_names(self):
        rows = parse(PTR_2016_GLUED_OWNER_AND_ACCOUNT)
        assert col(rows, 'securitySymbol') == ['BRK.A', 'CVX', 'GOGO', 'PG', 'XPOI']
        assert col(rows, 'owner') == ['SP', 'SP', 'SP', 'SP', None]
        # "...Brokerage accountXponential, Inc. (XPoI)": the account, then the next asset
        assert rows[-1]['securityName'] == 'Xponential, Inc'
        assert 'Subholding Of: Edward Jones Brokerage account' in rows[-2]['metadata']

    def test_a_row_split_by_a_page_break_keeps_its_ticker(self):
        rows = parse(PTR_2016_PAGE_SPLIT_ROW)
        assert [(r['securitySymbol'], r['transactionType']) for r in rows] == [('KMB', 'Sale'), ('MRK', 'Purchase')]
        assert rows[1]['securityName'] == 'Merck & Company, Inc. Common Stock'


# ── 2. Mixed-case and dotted tickers ────────────────────────────────────────────

class TestTickers:
    def test_mixed_case_tickers_are_read_and_upper_cased(self):
        rows = parse(PTR_2019_GLUED_TYPE_LETTERS)
        assert col(rows, 'securitySymbol') == ['ADDYY', 'AJRD', 'AJRD', 'AMSF', 'AMN']

    def test_dotted_tickers(self):
        assert col(parse(PTR_2020_DOTTED_AND_LOWERCASE_TICKERS), 'securitySymbol') == ['ACN', 'GOOG', 'AAPL', 'BRK.B']
        assert parse(PTR_2016_GLUED_OWNER_AND_ACCOUNT)[0]['securitySymbol'] == 'BRK.A'

    def test_ticker_on_a_later_line_than_the_name(self):
        [row] = parse(PTR_2019_RUSHA)
        assert (row['securitySymbol'], row['assetType'], row['owner']) == ('RUSHA', 'Stocks (including ADRs)', 'JT')
        assert row['securityName'] == 'Rush Enterprises, Inc. - Cl ass A'


# ── 3. The ticker is the LAST parenthesised token before the tag; no stop-list ──

class TestLastParenthesis:
    def test_a_parenthesis_glued_on_from_the_previous_description_loses(self):
        rows = parse(PTR_2019_PAREN_IN_GLUED_DESCRIPTION)
        # "...Smith Barney LLC IRA (SP).Valero Energy Corporation (VLo)" is Valero, not "SP"
        assert col(rows, 'securitySymbol') == ['AYR', 'ALC', 'DUK', 'VLO', 'DIS', 'WPX']
        assert rows[3]['securityName'] == 'Valero Energy Corporation'
        assert col(rows, 'transactionType') == ['Sale', 'Purchase', 'Sale', 'Sale', 'Purchase', 'Sale']

    def test_adr_marker_before_the_ticker(self):
        rows = parse(PTR_MODELLED_ADR_THEN_TICKER)
        [baba] = [r for r in rows if 'Alibaba' in r['securityName']]
        assert baba['securitySymbol'] == 'BABA'
        assert baba['securityName'] == 'Alibaba Group Holding Ltd Sponsored (ADR)'

    @pytest.mark.parametrize('text,ticker,name', [
        (PTR_2016_COLGATE_CL, 'CL', 'Colgate-Palmolive Company'),
        (PTR_2016_DEERE_DE, 'DE', 'Deere & Company'),
    ], ids=['colgate-CL', 'deere-DE'])
    def test_real_two_letter_tickers_are_kept(self, text, ticker, name):
        [row] = [r for r in parse(text) if r['securityName'] == name]
        assert row['securitySymbol'] == ticker


# ── 4. Transaction type letters ─────────────────────────────────────────────────

class TestTransactionType:
    def test_lowercase_type_letters(self):
        assert parse(PTR_2016_NO_TAGS)[4]['transactionType'] == 'Sale'          # "s 07/14/2016"
        rows = parse(PTR_2018_LOWERCASE_TYPE_AND_TAG)
        assert col(rows, 'transactionType') == ['Sale'] * 7                      # "[sT] s 08/28/2018"
        assert col(rows, 'assetType')[3] == 'Other'                              # "[oT]"
        assert rows[5]['exactAmount'] == 115 and rows[5]['amountRange'] == [0, 1000]  # "$115.32"

    def test_type_letter_glued_to_the_date(self):
        rows = parse(PTR_2019_GLUED_TYPE_LETTERS)
        assert col(rows, 'transactionType') == ['Purchase', 'Sale (Partial)', 'Sale (Partial)', 'Sale (Partial)', 'Purchase']

    def test_partial_sale_is_not_a_full_sale(self):
        rows = parse(PTR_2025_MODERN)
        by_symbol = {r['securitySymbol']: r['transactionType'] for r in rows}
        assert by_symbol['DVN'] == 'Sale (Partial)'
        assert by_symbol['AGCO'] == 'Sale'

    def test_e_is_an_exchange(self):
        [row] = parse(PTR_2019_EXCHANGE)
        assert (row['securitySymbol'], row['transactionType']) == ('ESRX', 'Exchange')


# ── 5. Open-ended amounts ───────────────────────────────────────────────────────

class TestOpenEndedAmounts:
    @pytest.mark.parametrize('text', [PTR_2020_OVER_50M, PTR_2022_OVER_50M_WRAPPED], ids=['2020', '2022-wrapped'])
    def test_over_50_million(self, text):
        row = parse(text)[-1]
        assert row['amountRange'] == [50000001, 999999999]
        assert (row['amountMin'], row['amountMax'], row['exactAmount'], row['amount']) == (50000001, None, None, None)

    def test_spouse_dc_over_1_million(self):
        [row] = parse(PTR_2016_SPOUSE_DC_OVER_1M)
        assert row['amountRange'] == [1000001, 999999999]
        assert (row['amountMin'], row['amountMax'], row['exactAmount']) == (1000001, None, None)
        assert (row['owner'], row['securityName']) == ('SP', 'u.S. Treasury Bills')

    def test_a_range_wrapped_onto_the_next_line_is_still_a_range(self):
        agco = parse(PTR_2025_MODERN)[0]                                # "$50,001 -\n$100,000"
        assert (agco['amountRange'], agco['exactAmount'], agco['amount']) == ([50001, 100000], None, 75000.5)


# ── 6. Options, judged on the row's own text ───────────────────────────────────

class TestOptions:
    def test_option_tag_and_own_description(self):
        rows = parse(PTR_2020_OPTIONS)
        assert col(rows, 'securitySymbol') == ['AMZN', 'AXP', 'AAPL', 'NFLX', 'PYPL']
        # AMZN/AXP/AAPL are tagged [OP]; NFLX is tagged [ST] but its own description is an option
        # exercise; PYPL follows it and its own description is a plain share purchase.
        assert col(rows, 'isOption') == [True, True, True, True, False]

    def test_a_company_named_option_is_a_stock(self):
        rows = parse(PTR_MODELLED_OPTION_CARE)
        [opch] = [r for r in rows if r['securitySymbol'] == 'OPCH']
        assert opch['isOption'] is False
        assert opch['securityName'] == 'Option Care Health, Inc'

    def test_plain_stock_rows_are_not_options(self):
        assert col(parse(PTR_2025_MODERN), 'isOption') == [False] * 9


# ── 7. Filing status ────────────────────────────────────────────────────────────

class TestFilingStatus:
    def test_amended_in_the_2022_layout(self):
        [row] = parse(PTR_2022_AMENDED)                                 # "F\x00...S\x00...: Amended"
        assert row['filingStatus'] == 'Amended'
        assert '\x00' not in row['metadata']

    def test_amended_in_the_older_layouts(self):
        assert parse(PTR_2016_AMENDED)[0]['filingStatus'] == 'Amended'
        assert col(parse(PTR_2020_OPTIONS), 'filingStatus') == ['Amended', 'New', 'New', 'New', 'New']

    def test_new_in_the_2022_layout(self):
        rows = parse(PTR_2025_MODERN)
        assert col(rows, 'filingStatus') == ['New'] * 9
        assert rows[0]['metadata'] == ('Filing Status: New\n'
                                       'Subholding Of: Hern Family Revocable Trust > Brokerage Investment Account\n'
                                       'Description: sell to close')


# ── Regression: a normal 2019+ filing parses as it always did ──────────────────

class TestModernRegression:
    def test_output_keys(self):
        for row in parse(PTR_2025_MODERN) + parse(PTR_2016_NO_TAGS):
            assert set(row) == ORIGINAL_KEYS | {'filingStatus', 'isOption'}

    def test_stock_rows_match_the_original_parser(self):
        rows = {r['securitySymbol']: r for r in parse(PTR_2025_MODERN)}
        # Values the original parser returned for these rows of DocID 20026597.
        expected = {
            'AGCO': dict(transactionDate='2024-12-31', filingDate='2025-01-02', transactionType='Sale',
                         securityName='AGCO Corporation Common Stock', amountRange=[50001, 100000]),
            'XOM': dict(transactionDate='2024-12-10', filingDate='2024-12-11', transactionType='Purchase',
                        securityName='Exxon Mobil Corporation Common Stock', amountRange=[1001, 15000]),
            'HD': dict(transactionDate='2024-12-12', filingDate='2024-12-13', transactionType='Purchase',
                       securityName='Home Depot, Inc', amountRange=[1001, 15000]),
            'EL': dict(transactionDate='2024-12-31', filingDate='2025-01-02', transactionType='Sale',
                       securityName='Estee Lauder Companies, Inc', amountRange=[15001, 50000]),
        }
        for symbol, fields in expected.items():
            row = rows[symbol]
            for key, value in fields.items():
                assert row[key] == value, (symbol, key)
            assert row['amountMin'] == fields['amountRange'][0] and row['amountMax'] == fields['amountRange'][1]
            assert row['amount'] == sum(fields['amountRange']) / 2 and row['exactAmount'] is None
            assert (row['owner'], row['assetType'], row['filerName']) == ('JT', 'Stocks (including ADRs)', 'Kevin Hern')
            assert (row['formType'], row['source']) == ('house_ptr', 'house')
        assert list(rows) == ['AGCO', None, 'DVN', 'DXCM', 'EL', 'XOM', 'HD', 'INTC', 'JNJ']

    def test_a_date_inside_a_bond_name_is_not_the_trade_date(self):
        # "JT Citigroup MTN 9/1/2026  [CS] S 12/03/2024 12/04/2024": the original parser took the
        # bond's maturity as the trade date and the trade date as the filing date.
        bond = parse(PTR_2025_MODERN)[1]
        assert (bond['transactionDate'], bond['filingDate']) == ('2024-12-03', '2024-12-04')
        assert (bond['securitySymbol'], bond['assetType']) == (None, 'Corporate Securities (Bonds and Notes)')
        assert bond['securityName'] == 'Citigroup MTN 9/1/2026'


# ── Wiring: the Lambda hands pypdf's text to the pure parser ───────────────────

class _FakeS3:
    def get_object(self, Bucket, Key):
        return {'Body': io.BytesIO(b'%PDF-1.4' + b' ' * 200)}


class _FakePage:
    def __init__(self, text):
        self._text = text

    def extract_text(self):
        return self._text


@pytest.fixture
def lambda_module(monkeypatch):
    import lambda_function
    monkeypatch.setattr(lambda_function, 's3_client', _FakeS3())
    monkeypatch.setattr(lambda_function, '_ASSET_CODES_CACHE', dict(ASSET_CODES))
    return lambda_function


def _serve_pages(monkeypatch, module, pages):
    reader = type('Reader', (), {'pages': [_FakePage(p) for p in pages]})
    monkeypatch.setattr(module, 'PdfReader', lambda _stream: reader)


def test_lambda_parses_every_page_through_the_pure_parser(lambda_module, monkeypatch):
    split = PTR_2025_MODERN.index('JT Exxon Mobil')
    _serve_pages(monkeypatch, lambda_module, [PTR_2025_MODERN[:split], PTR_2025_MODERN[split:]])
    trades = lambda_module.parse_house_ptr_with_textract('trades/house/2025/20026597.pdf')
    assert trades == hp.parse_house_ptr_text(PTR_2025_MODERN[:split] + '\n' + PTR_2025_MODERN[split:] + '\n', ASSET_CODES)
    assert len(trades) == 9


def test_matched_trades_carry_status_and_option_flag(lambda_module, monkeypatch):
    _serve_pages(monkeypatch, lambda_module, [PTR_2020_OPTIONS])
    # The excerpt has no FILER INFORMATION block; name the filer so every row reaches the matcher.
    monkeypatch.setattr(hp, 'extract_filer_name', lambda text: 'Test Member')
    member = {'name': 'Member', 'party': 'D', 'position': 'House', 'websiteUrl': None, 'matchScore': 1.0}
    monkeypatch.setattr(lambda_module, 'find_matching_politician', lambda *a, **k: member)
    monkeypatch.setattr(lambda_module, 'format_state_district', lambda p: 'CA11')
    out = lambda_module.match_house_ptr_trades('trades/house/2020/20016961.pdf', [member], skip_duplicate_check=True)
    matched = out['matchedTrades']
    assert [m['securitySymbol'] for m in matched] == ['AMZN', 'AXP', 'AAPL', 'NFLX', 'PYPL']
    assert [m['filingStatus'] for m in matched] == ['Amended', 'New', 'New', 'New', 'New']
    assert [m['isOption'] for m in matched] == [True, True, True, True, False]
    assert matched[0]['tradeId'] == 'trade_2020-01-16_house_20016961_0'
    assert matched[0]['owner'] == 'Spouse' and matched[0]['transactionDate'] == 20200116
