"""
House PTR text parser (pure: no AWS, no I/O).

Turns the text that pypdf's ``extract_text()`` produces for an electronic House Periodic
Transaction Report into trade dicts. ``lambda_function.parse_house_ptr_with_textract`` fetches
the PDF from S3, extracts the text and calls ``parse_house_ptr_text``.

How a row is found
------------------
pypdf does not keep the table's columns. It emits each row as a stream::

    [owner] asset name (TICKER) [TAG] <type> <tx date> <notification date> <amount>
    [cap-gains checkbox debris] FILING STATUS: New [SUBHOLDING OF: ...] [DESCRIPTION: ...]

with line breaks in arbitrary places and neighbouring text runs glued together
("...Brokerage accountSP" is an account name followed by the next row's owner code).
The one part every layout keeps together is the transaction anchor: the type letter, the two
dates and the amount. So rows are found by that anchor, and everything between two anchors is
split into the previous row's metadata and the next row's asset name.

Layouts this handles (all seen in real filings):
- 2015-2018: no ``[XX]`` asset tag; the font renders some capitals in lowercase, so tickers
  read "(aaPl)" and type letters read "s" / "p"; labels read "FIlINg STaTuS:".
- 2019-2021: ``[ST]`` tags, sometimes lowercase ("[sT]"); the type letter is often glued to
  the date ("P02/01/2019", "S (partial)01/23/2019").
- 2022+: the label font drops lowercase letters, and pypdf returns NUL characters for them,
  so "Filing Status: Amended" arrives as "F\\x00\\x00\\x00\\x00\\x00 S\\x00...: Amended".
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Standard House PTR ranges (same as Senate PTR ranges)
# Note: Minimum reporting threshold is $1,000, but we handle sub-$1k amounts
HOUSE_PTR_RANGES = [
    (0, 1000),  # $0 - $1,000 (handles sub-$1k amounts)
    (1001, 15000),
    (15001, 50000),
    (50001, 100000),
    (100001, 250000),
    (250001, 500000),
    (500001, 1000000),
    (1000001, 5000000),
    (5000001, 25000000),
    (25000001, 50000000),
    (50000001, None)  # Over $50,000,000 - max is None/unbounded
]

# Stand-in for an unbounded top of range in amountRange (kept from the original parser).
UNBOUNDED_MAX = 999999999


def find_standard_range(amount_value: float) -> tuple:
    """Find the standard House PTR range that contains the given amount"""
    # Handle zero or negative amounts (use first range)
    if amount_value <= 0:
        return HOUSE_PTR_RANGES[0]

    for range_min, range_max in HOUSE_PTR_RANGES:
        if range_max is None:
            if amount_value >= range_min:
                return (range_min, None)
        else:
            if range_min <= amount_value <= range_max:
                return (range_min, range_max)
    # Fallback: if amount is less than minimum, use first range
    return HOUSE_PTR_RANGES[0]


# ── Patterns ─────────────────────────────────────────────────────────────────

# The transaction anchor. The type letter is matched case-blind (the 2015-18 fonts render it
# "s"/"p") and may be glued to the date. "S (partial)" is its own type. The amount may be a
# range (possibly wrapped onto the next line), a fixed amount, or "Over $X" /
# "Spouse/DC Over $X", which has no range at all.
_AMOUNT_NUM = r"\$\s*(?:\d[\d,]*(?:\.\d+)?|\.\d+)"
_ANCHOR = re.compile(
    r"(?P<type>S\s*\(\s*partial\s*\)|[PSE])\s*"
    r"(?P<tx>\d{1,2}/\d{1,2}/\d{4})\s*"
    r"(?P<nd>\d{1,2}/\d{1,2}/\d{4})\s*"
    r"(?P<amt>(?:spouse\s*/\s*dc\s*)?over\s*" + _AMOUNT_NUM +
    r"|" + _AMOUNT_NUM + r"(?:[ \t]*[-–—](?:\s*" + _AMOUNT_NUM + r")?)?)",
    re.IGNORECASE)

# Ticker: the LAST parenthesised ticker-like token before the asset tag. Mixed case ("(aaPl)")
# and dotted ("BRK.B") tickers are real. There is deliberately no stop-list: "(DE)" is Deere and
# "(CL)" is Colgate. "(ADR)" loses to a later "(BABA)" because the last token wins.
_TICKER = re.compile(r"\(\s*([A-Za-z][A-Za-z0-9.\-]{0,6}|[A-Za-z][A-Za-z0-9.\-]{0,3} [A-Za-z0-9.\-]{1,4})\s*\)")
# pypdf sometimes puts a space inside a ticker: "(MC D)", "(PCL N)", "(A DDYY)". A split token
# only counts when it is ticker-sized once joined, so "(Class A)" is not a ticker.
_SPLIT_TICKER_MAX = 5
# Asset-type tag, e.g. [ST], [OP], [GS]. Some fonts render it lowercase ("[sT]"); codes such as
# 4K / 5C contain digits.
_TAG = re.compile(r"\[\s*([A-Za-z0-9]{2})\s*\]")

# Option wording, judged on the row's OWN text only (its name and its own metadata, never the
# next row's). A bare "option" is not enough: "Option Care Health (OPCH)" is a stock.
_OPTION = re.compile(
    r"\b(?:call|put)s?\s+options?\b|\b(?:calls|puts)\b|\boptions?\s+contracts?\b|"
    r"\bstrike\b|\bexpir(?:y|es|ation)\b", re.IGNORECASE)

_OWNER_CODES = ('JT', 'SP', 'DC')
_OWNER_ONLY = re.compile(r"^\s*(JT|SP|DC)\s*$", re.IGNORECASE)
# An owner code leading the asset name: "JT AGCO Corporation", or glued as in "DCMedtronic plc"
# (but not "SPDR ...", where the code letters are part of the name).
_OWNER_PREFIX = re.compile(r"^\s*(JT|SP|DC)(?:\s+(?=\S)|(?=[A-Z][a-z]))")
# An owner code glued to the end of the metadata line just above the next asset name
# ("...Brokerage accountSP", "...Traditional IRASP", "...PLCJT").
_OWNER_SUFFIX = re.compile(r"(?<=\S)\s*(JT|SP|DC|sP)\s*$")

# Metadata labels. Older fonts render the label as "FIlINg sTaTus:"; the 2022+ font keeps only
# the capitals ("F  S :"). pypdf often glues a label's first letter to the end of the previous
# line; _reglue_initials() puts it back before these run.
_LABELS: List[Tuple[str, 're.Pattern']] = [
    ('Filing Status', re.compile(r"^\s*(?:F\s*)?I\s*L\s*I\s*N\s*G\s*S\s*T\s*A\s*T\s*U\s*S\s*:\s*(?P<v>.*)$", re.I)),
    ('Subholding Of', re.compile(r"^\s*(?:S\s*)?U\s*B\s*H\s*O\s*L\s*D\s*I\s*N\s*G\s*O\s*F\s*:\s*(?P<v>.*)$", re.I)),
    ('Description', re.compile(r"^\s*(?:D\s*)?E\s*S\s*C\s*R\s*I\s*P\s*T\s*I\s*O\s*N\s*:\s*(?P<v>.*)$", re.I)),
    ('Location', re.compile(r"^\s*(?:L\s*)?O\s*C\s*A\s*T\s*I\s*O\s*N\s*:\s*(?P<v>.*)$", re.I)),
    ('Comments', re.compile(r"^\s*(?:C\s*)?O\s*M\s*M\s*E\s*N\s*T\s*S?\s*:\s*(?P<v>.*)$", re.I)),
    ('Filing Status', re.compile(r"^\s*F\s+S\s*:\s*(?P<v>.*)$")),
    ('Subholding Of', re.compile(r"^\s*S\s+O\s*:\s*(?P<v>.*)$")),
    ('Description', re.compile(r"^\s*D\s+:\s*(?P<v>.*)$")),
    ('Location', re.compile(r"^\s*L\s+:\s*(?P<v>.*)$")),
    ('Comments', re.compile(r"^\s*C\s+:\s*(?P<v>.*)$")),
]
# A label or section heading whose first letter pypdf moved to the end of the previous line:
# (initial, what the next line starts with). "NewS" + "UBHOLDING O F:" is "New" + "SUBHOLDING OF:".
_SPLIT_INITIALS: List[Tuple[str, 're.Pattern']] = [
    ('F', re.compile(r"^\s*I\s*L\s*I\s*N\s*G\s*S\s*T\s*A\s*T\s*U\s*S\s*:", re.I)),
    ('S', re.compile(r"^\s*U\s*B\s*H\s*O\s*L\s*D\s*I\s*N\s*G\s*O\s*F\s*:", re.I)),
    ('D', re.compile(r"^\s*E\s*S\s*C\s*R\s*I\s*P\s*T\s*I\s*O\s*N\s*:", re.I)),
    ('L', re.compile(r"^\s*O\s*C\s*A\s*T\s*I\s*O\s*N\s*:", re.I)),
    ('C', re.compile(r"^\s*O\s*M\s*M\s*E\s*N\s*T\s*S?\s*:", re.I)),
    ('A', re.compile(r"^\s*S\s*S\s*E\s*T\s*C\s*L\s*A\s*S\s*S", re.I)),
    ('I', re.compile(r"^\s*N\s*I\s*T\s*I\s*A\s*L\s*P\s*U\s*B\s*L\s*I\s*C", re.I)),
    ('C', re.compile(r"^\s*E\s*R\s*T\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N", re.I)),
]
_STATUS_VALUE = re.compile(r"^\s*(new|amended|deleted)(?P<rest>.*)$", re.IGNORECASE)

# Page furniture that separates rows: the table header (repeated on every page) and the
# "Filing ID #..." footer. Both are replaced by a boundary marker line.
_BOUNDARY = '\x01'
_HEADER = re.compile(
    r"(?:I\s*D\s*)?Owner\s*Asset\s*Transaction\s*Type\s*Date\s*Notification\s*Date\s*Amount"
    r"(?:\s*Cap\s*\.?\s*Gains\s*>\s*\$\s*200\s*\?)?", re.IGNORECASE)
_FILING_ID = re.compile(r"Filing\s*I\s*D\s*#\s*\d+", re.IGNORECASE)
_ASSET_CODE_NOTE = re.compile(
    r"\*\s*For\s+the\s+complete\s+list\s+of\s+asset\s+type\s+abbreviations.*?asset-type-codes\.aspx\s*\.?",
    re.IGNORECASE | re.DOTALL)
# Checkbox glyphs (cap-gains box "gfedc"/"gfedcb", radio buttons "nmlkj"/"nmlkji").
_CHECKBOX = re.compile(r"gfedcb?|nmlkji?")

# Where the transaction table ends (only the last row's metadata can run into these).
_TABLE_END = re.compile(
    r"A\s*S\s*S\s*E\s*T\s*C\s*L\s*A\s*S\s*S\s*D\s*E\s*T\s*A\s*I\s*L\s*S"
    r"|I\s*N\s*I\s*T\s*I\s*A\s*L\s*P\s*U\s*B\s*L\s*I\s*C\s*O\s*F\s*F\s*E\s*R\s*I\s*N\s*G"
    r"|C\s*E\s*R\s*T\s*I\s*F\s*I\s*C\s*A\s*T\s*I\s*O\s*N"
    r"|Digitally\s+Signed"
    r"|^[ \t]*I[ \t]+V[ \t]+D[ \t]*$"
    r"|^[ \t]*I[ \t]+P[ \t]+O[ \t]*$",
    re.IGNORECASE | re.MULTILINE)

# In the 2022+ layout a long description/comment wraps onto following lines, which then sit
# directly above the next row's asset name. A line at least this long filled the metadata column
# and wrapped (full lines run 90-125 characters; asset names wrap at about 40).
_WRAPPED_LINE_MIN = 90

_TRANSACTION_TYPES = {
    'P': 'Purchase',
    'S': 'Sale',
    'S (PARTIAL)': 'Sale (Partial)',
    'E': 'Exchange',
}


# ── Filer name and signature date (unchanged from the original parser) ───────

def extract_filer_name(full_text: str) -> Optional[str]:
    """Filer name from the FILER INFORMATION block, e.g. "Name: Hon. Virginia Foxx Status: Member"."""
    filer_name = None
    name_patterns = [
        r'Name:\s*(?:Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+?)(?:\s+Status:)',  # Stop before "Status:"
        r'Name:\s*(?:Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',  # Fallback without Status
    ]
    for pattern in name_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
        if match:
            filer_name = match.group(1).strip()
            # Remove "Hon." prefix if present
            filer_name = re.sub(r'^Hon\.?\s+', '', filer_name, flags=re.IGNORECASE).strip()
            # Remove any trailing "Status" that might have been captured
            filer_name = re.sub(r'\s+Status\s*$', '', filer_name, flags=re.IGNORECASE).strip()
            break

    if not filer_name:
        house_patterns = [
            r'Hon\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
            r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
        ]
        for pattern in house_patterns:
            match = re.search(pattern, full_text)
            if match:
                filer_name = match.group(1).strip()
                filer_name = re.sub(r'\b(Honorable|Hon\.?|Representative|Rep\.?)\b', '', filer_name, flags=re.IGNORECASE).strip()
                break
    return filer_name


def extract_signature_date(full_text: str) -> Optional[str]:
    """Filing date (YYYY-MM-DD) from the e-signature at the bottom; fallback when a row has no
    notification date."""
    signature_patterns = [
        r'Digitally Signed:\s+[^,]+,\s*(\d{1,2}/\d{1,2}/\d{4})',  # "Digitally Signed: Name, MM/DD/YYYY"
        r'Digitally Signed:\s+[^\d]+(\d{1,2}/\d{1,2}/\d{4})',    # "Digitally Signed: Name MM/DD/YYYY"
        r'(?:signed|signature)[:\s]+[^,]+,\s*(\d{1,2}/\d{1,2}/\d{4})',
        r'(?:signed|signature)[:\s]+[^\d]+(\d{1,2}/\d{1,2}/\d{4})',
    ]
    for pattern in signature_patterns:
        matches = list(re.finditer(pattern, full_text, re.IGNORECASE))
        if matches:
            try:
                return datetime.strptime(matches[-1].group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
            except ValueError:
                continue

    # Fallback: the last MM/DD/YYYY date in the document (likely the signature date)
    date_matches = list(re.finditer(r'(\d{1,2}/\d{1,2}/\d{4})', full_text))
    if date_matches:
        try:
            return datetime.strptime(date_matches[-1].group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None


# ── Row pieces ───────────────────────────────────────────────────────────────

def _reglue_initials(lines: List[str]) -> List[str]:
    """Move a label's first letter back from the end of the previous line to its own line."""
    lines = list(lines)
    for i in range(len(lines) - 1):
        prev = lines[i].rstrip()
        if not prev:
            continue
        for initial, rest in _SPLIT_INITIALS:
            if prev[-1].upper() == initial and rest.match(lines[i + 1]):
                lines[i] = prev[:-1]
                lines[i + 1] = prev[-1] + lines[i + 1].lstrip()
                break
    return lines


def normalize_ptr_text(text: str) -> str:
    """NULs to spaces (2022+ label font), page furniture to boundary lines, checkbox glyphs out,
    and label initials glued to the previous line put back."""
    text = text.replace('\x00', ' ').replace('\r\n', '\n').replace('\r', '\n')
    boundary = '\n' + _BOUNDARY + '\n'
    text = _HEADER.sub(boundary, text)
    text = _FILING_ID.sub(boundary, text)
    text = _ASSET_CODE_NOTE.sub(boundary, text)
    text = _CHECKBOX.sub('\n', text)
    return '\n'.join(_reglue_initials(text.split('\n')))


def parse_amount(raw: str) -> Dict[str, Any]:
    """Map a PTR amount to the standard-range fields the table stores.

    Returns amountMin, amountMax, amountRange, exactAmount and amount (midpoint), following the
    original parser's conventions: ranges map to the standard range around their midpoint, a
    fixed amount keeps exactAmount and maps to the range that contains it, and an unbounded top
    is amountMax=None with amountRange [min, 999999999].
    """
    out = {'amountMin': None, 'amountMax': None, 'amountRange': None, 'exactAmount': None, 'amount': None}
    if not raw:
        return out
    nums = [float(n.replace(',', '')) for n in re.findall(r'\$\s*(\d[\d,]*(?:\.\d+)?|\.\d+)', raw)]
    if not nums:
        return out

    def _std(value):
        lo, hi = find_standard_range(value)
        return [lo, hi if hi is not None else UNBOUNDED_MAX]

    if re.search(r'over', raw, re.IGNORECASE):
        # "Over $50,000,000" / "Spouse/DC Over $1,000,000": strictly above the figure, no top.
        low = int(nums[0]) + 1
        amount_range = [low, UNBOUNDED_MAX]
    elif len(nums) >= 2 and nums[0] < nums[1]:
        # A range: map to the standard range around its midpoint
        amount_range = _std((nums[0] + nums[1]) / 2)
    elif len(nums) >= 2 or re.search(r'[-–—]', raw) is None:
        # A fixed amount (or a degenerate "range" with min >= max): keep the exact figure
        out['exactAmount'] = int(nums[0])
        amount_range = _std(nums[0])
    else:
        # "$15,001 -" whose upper bound was lost to a page break: it is still a range, and its
        # lower bound names the standard range.
        amount_range = _std(nums[0])

    out['amountRange'] = amount_range
    out['amountMin'] = amount_range[0]
    out['amountMax'] = amount_range[1] if amount_range[1] != UNBOUNDED_MAX else None
    if out['amountMin'] is not None and out['amountMax'] is not None:
        out['amount'] = (out['amountMin'] + out['amountMax']) / 2
    return out


def _match_label(line: str) -> Optional[Tuple[str, str]]:
    for name, pattern in _LABELS:
        m = pattern.match(line)
        if m:
            return name, m.group('v')
    return None


def _is_junk(line: str) -> bool:
    """Empty lines and stray single letters."""
    return re.fullmatch(r'\s*[A-Za-z]?\s*', line) is not None


def _clean(value: str) -> str:
    return re.sub(r'\s+', ' ', value).strip()


def _split_label_value(label: str, value: str, ends_metadata: bool = True) -> Tuple[str, Optional[str], str]:
    """Split a label's value from text glued after it that belongs to the NEXT row.

    Returns (value, owner, glued_name): "NewSP" is status New + owner SP; "NewMax sound Corp
    (MAXD)" is status New + the next row's asset name; "...Brokerage accountSP" is the account
    name + owner SP.
    """
    if label == 'Filing Status':
        m = _STATUS_VALUE.match(value)
        if m:
            rest = m.group('rest').strip()
            owner_only = _OWNER_ONLY.match(rest)
            if owner_only:
                return m.group(1).capitalize(), owner_only.group(1).upper(), ''
            return m.group(1).capitalize(), None, (rest if len(rest) > 1 else '')
    m = _OWNER_SUFFIX.search(value) if ends_metadata else None
    if m:
        return _clean(value[:m.start(1)]), m.group(1).upper(), ''
    return _clean(value), None, ''


def _find_tickers(text: str) -> List['re.Match']:
    return [m for m in _TICKER.finditer(text)
            if ' ' not in m.group(1) or len(m.group(1).replace(' ', '')) <= _SPLIT_TICKER_MAX]


def _has_name_text(lines: List[str]) -> bool:
    """True when the lines hold more than a ticker and an asset tag."""
    text = ' '.join(lines)
    for m in reversed(_find_tickers(text)):
        text = text[:m.start()] + ' ' + text[m.end():]
    return re.search(r'[A-Za-z]{2}', _TAG.sub(' ', text)) is not None


def _starts_mid_name(lines: List[str]) -> bool:
    """True when the name block starts part-way through a name whose start was glued onto the
    metadata line above it: "The Hershey Company (" + "HsY)", "Sarepta Therapeutics, I" + "nc."."""
    text = ' '.join(lines).strip()
    if not text:
        return False
    close, open_ = text.find(')'), text.find('(')
    return (close != -1 and (open_ == -1 or close < open_)) or text[0].islower() or text[0] in '-,&'


class _Region:
    """The text between two anchors: the previous row's metadata, then this row's name block.

    meta holds [label, value] pairs of the PREVIOUS row. last_label is the label line that ends
    that metadata when it directly precedes the name block (its value may have the name glued
    on). name_lines/owner belong to THIS row.
    """

    def __init__(self, text: str, modern: bool, is_last: bool = False):
        self.meta: List[List[str]] = []
        self.last_label: Optional[List[str]] = None
        self.name_lines: List[str] = []
        self.owner: Optional[str] = None
        # Text between the previous row's anchor and its first label: the rest of that row's
        # name when a page break split it ("... Common Stock" / page header / "(MRK)").
        self.carry_over: List[str] = []
        self.name_unresolved = False
        self._parse(text.split('\n'), modern, is_last)

    def _parse(self, lines: List[str], modern: bool, is_last: bool) -> None:
        # The last line that closes the previous row: a label or a page boundary.
        last_stop = -1
        for idx, line in enumerate(lines):
            if line.strip() == _BOUNDARY or _match_label(line):
                last_stop = idx

        current: Optional[List[str]] = None
        glued_name, owner = '', None
        for idx, line in enumerate(lines[:last_stop + 1]):
            if line.strip() == _BOUNDARY:
                current = None
                continue
            lab = _match_label(line)
            if lab:
                value, line_owner, glued = _split_label_value(lab[0], lab[1], idx == last_stop)
                current = [lab[0], value]
                self.meta.append(current)
                if idx == last_stop:
                    owner, glued_name = line_owner, glued
                    self.last_label = current
                continue
            if _is_junk(line):
                continue
            if current is not None:
                # A value that wrapped onto the next line
                current[1] = _clean(current[1] + ' ' + line)
            elif not self.meta:
                self.carry_over.append(line.strip())

        tail = lines[last_stop + 1:]
        if is_last:
            # After the last row only its own metadata can follow.
            for line in tail:
                if current is not None and not _is_junk(line):
                    current[1] = _clean(current[1] + ' ' + line)
            return

        # 2022+: a long description/comment wraps onto the lines right after its label, directly
        # above the next asset name. A line follows as paragraph text only while the line before
        # it filled the column. The region's last line is the start of the anchor line, so it is
        # always the name; so is a line that starts with an owner code.
        if modern and current is not None and last_stop >= 0:
            prev = lines[last_stop].rstrip()
            while (tail and not _is_junk(tail[0]) and any(not _is_junk(ln) for ln in tail[1:])
                   and len(prev) >= _WRAPPED_LINE_MIN and not prev.endswith('.')
                   and not _OWNER_PREFIX.match(tail[0]) and not _OWNER_ONLY.match(tail[0])):
                current[1] = _clean(current[1] + ' ' + tail[0])
                prev = tail[0].rstrip()
                tail = tail[1:]

        name_lines = ([glued_name] if glued_name else []) + [
            ln.strip() for ln in tail if not _is_junk(ln) or _OWNER_ONLY.match(ln)]
        # An owner code on its own line, or leading a line, marks where the asset name starts.
        for idx in range(len(name_lines) - 1, -1, -1):
            only = _OWNER_ONLY.match(name_lines[idx])
            prefix = _OWNER_PREFIX.match(name_lines[idx])
            if only or prefix:
                owner = (only or prefix).group(1).upper()
                rest = [] if only else [name_lines[idx][prefix.end():]]
                if idx > 0 and current is not None:
                    # Text above the owner code is the previous row's wrapped metadata
                    current[1] = _clean(current[1] + ' ' + ' '.join(name_lines[:idx]))
                    self.last_label = None
                name_lines = rest + name_lines[idx + 1:]
                break
        self.owner = owner
        self.name_lines = [ln for ln in name_lines if ln]


def _glue_point(value: str, forced: bool = False) -> Optional[int]:
    """Where a name glued onto a metadata value starts, judged from the text alone.

    Strong signs, of which the last one wins: a word starting right after a digit or after
    punctuation that ends a word of 3+ characters, with no space ("Exp. 11/16/18Tesla, Inc.",
    "$302.00.Stryker", "Eli lilly and Company.gilead Sciences" - but not "U.S." or "L.P.").
    Weak sign, used only when there is no strong one, and the first one wins: a capital that
    starts a new word right after a lowercase word ("U.S. Trust HoldingsSarepta Therapeutics").
    With ``forced`` (the name must be in this value), any capital right after a lowercase letter
    is accepted as a last resort ("Preferred securityHD Supply Holdings").
    Never inside parentheses, so a ticker is never cut.
    """
    strong, weak, weakest = None, None, None
    depth = 0
    for i in range(1, len(value)):
        prev, ch = value[i - 1], value[i]
        depth += (prev == '(') - (prev == ')')
        if depth > 0 or not ch.isalpha():
            continue
        nxt = value[i + 1] if i + 1 < len(value) else ''
        word_like = ch.isupper() or nxt.islower()
        if prev.isdigit() and word_like:
            strong = i
        elif prev in '\\)' and word_like:
            strong = i
        elif prev in '.;:/' and word_like:
            token = re.search(r'([A-Za-z0-9$.]+)$', value[:i - 1])
            if (token and len(token.group(1)) >= 3) or value[i - 2:i - 1] == ")":
                strong = i
        elif prev.islower() and ch.isupper():
            word = re.search(r'[A-Za-z]+$', value[:i])
            if weak is None and nxt.islower() and word and len(word.group(0)) >= 4:
                weak = i
            if weakest is None:
                weakest = i
    if strong is not None:
        return strong
    return weak if weak is not None or not forced else weakest


def _resolve_glued_names(regions: List['_Region'], modern: bool) -> None:
    """Recover asset names that pypdf glued onto the previous row's last metadata value.

    "SUBHOLDING OF: Fidelity Investments Brokerage accountautoNation, Inc. (aN)" is the account
    "Fidelity Investments Brokerage account" followed by the next row's "autoNation, Inc. (aN)".
    The split point is found, in order, from: a value of the same label that appears on its own
    elsewhere in the filing (accounts repeat), an asset name that appears on its own elsewhere,
    and the glue point in the text. When all fail and the row has no name of its own, the whole
    value becomes the name block so the row and its ticker survive; the row's option flag then
    ignores that text, because it may be the previous row's description.

    In the older layouts a known account prefix is trusted even when the row already has name
    lines ("...accountThermo Fisher Scientific" + "Inc(TMo)"), provided nothing separates the
    account from the glued text: "Morgan Stanley INH IRA" is a longer account, not "Morgan
    Stanley" + a name, and "Trust > Brokerage" is a sub-account.
    """
    known: Dict[str, set] = {}
    for region in regions:
        for label, value in region.meta:
            known.setdefault(label, set()).add(value)
    known_names = {_clean(' '.join(r.name_lines)) for r in regions
                   if _has_name_text(r.name_lines) and not _starts_mid_name(r.name_lines)}
    known_names = {n for n in known_names if len(n) >= 6}

    for region in regions[:-1]:
        lab = region.last_label
        if lab is None or lab[0] == 'Filing Status' or not lab[1]:
            continue
        nameless = not _has_name_text(region.name_lines)
        value = lab[1]
        prefixes = [v for v in known.get(lab[0], ()) if v and len(v) < len(value) and value.startswith(v)
                    and not value[len(v) - 1].isspace() and not value[len(v)].isspace()
                    and value[len(v)] != '>']
        if not nameless and not _starts_mid_name(region.name_lines) and (modern or not prefixes):
            continue
        split = None
        # A ticker cut in two: the value ends "... Inc. (H" and the name block starts "CSG)".
        open_paren = value.rfind('(')
        cut_ticker = open_paren > value.rfind(')') and _starts_mid_name(region.name_lines)
        if prefixes:
            split = len(max(prefixes, key=len))
        else:
            suffixes = [n for n in known_names if len(n) < len(value) and value.endswith(n)]
            if suffixes:
                split = len(value) - len(max(suffixes, key=len))
            else:
                split = _glue_point(value, forced=nameless or cut_ticker)
                if split is None and cut_ticker:
                    split = open_paren
        if split is not None and value[split:].strip() and not value[split:].lstrip().startswith('>'):
            lab[1] = _clean(value[:split])
            region.name_lines = [value[split:].strip()] + region.name_lines
        elif nameless:
            region.name_lines = [value] + region.name_lines
            region.name_unresolved = True


def _name_ticker_tag(name_text: str) -> Tuple[str, Optional[str], Optional[str]]:
    """(security name, ticker, asset tag) from a row's name block."""
    text = _clean(name_text)
    tag = None
    before_tag = text
    tag_matches = list(_TAG.finditer(text))
    if tag_matches:
        tag = tag_matches[-1].group(1).upper()
        before_tag = text[:tag_matches[-1].start()]
    tickers = _find_tickers(before_tag)
    ticker = None
    name = before_tag
    if tickers:
        ticker = tickers[-1].group(1).replace(' ', '').upper()
        name = before_tag[:tickers[-1].start()]
    name = _clean(_TAG.sub(' ', name))
    name = re.sub(r'[,.]\s*$', '', name).strip()
    return name, ticker, tag


def _to_iso(date_str: str) -> Optional[str]:
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def _format_metadata(meta: List[Tuple[str, str]]) -> Optional[str]:
    lines = [f"{label}: {value}".rstrip() for label, value in meta]
    return '\n'.join(lines) if lines else None


# ── Public entry point ───────────────────────────────────────────────────────

def parse_house_ptr_text(full_text: str, asset_codes: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """Parse the pypdf text of an electronic House PTR into trade dicts.

    Each dict has the keys the matcher has always consumed (filerName, filingDate,
    transactionDate, securityName, securitySymbol, assetType, transactionType, amount,
    amountMin, amountMax, amountRange, exactAmount, owner, formType, source, metadata) plus:
      - filingStatus: "New" / "Amended" (None when the row carries no status line)
      - isOption: True when the tag is [OP] or the row's own text describes an option
    """
    asset_codes = asset_codes or {}
    if not full_text:
        return []

    filer_name = extract_filer_name(full_text)
    signature_date = extract_signature_date(full_text)

    text = normalize_ptr_text(full_text)
    anchors = list(_ANCHOR.finditer(text))
    if not anchors:
        return []
    modern = bool(re.search(r'(?m)^\s*F\s+S\s*:', text))

    # Region k lies before anchor k; the region after the last anchor ends at the table's end.
    regions: List[_Region] = []
    prev_end = 0
    for anchor in anchors:
        regions.append(_Region(text[prev_end:anchor.start()], modern))
        prev_end = anchor.end()
    tail_text = text[prev_end:]
    end = _TABLE_END.search(tail_text)
    regions.append(_Region(tail_text[:end.start()] if end else tail_text, modern, is_last=True))
    _resolve_glued_names(regions, modern)

    trades: List[Dict[str, Any]] = []
    for k, anchor in enumerate(anchors):
        region = regions[k]
        own_meta = regions[k + 1].meta
        carry_over = [re.sub(_AMOUNT_NUM, ' ', ln) for ln in regions[k + 1].carry_over]
        name_text = ' '.join(region.name_lines + carry_over)
        security_name, security_symbol, tag = _name_ticker_tag(name_text)

        type_key = re.sub(r'\s+', ' ', anchor.group('type').upper()).replace('( ', '(').replace(' )', ')')
        type_key = 'S (PARTIAL)' if type_key.startswith('S') and 'PARTIAL' in type_key else type_key
        transaction_type = _TRANSACTION_TYPES.get(type_key)
        transaction_date = _to_iso(anchor.group('tx'))
        notification_date = _to_iso(anchor.group('nd'))
        amounts = parse_amount(anchor.group('amt'))

        status = next((val for lab, val in own_meta if lab == 'Filing Status'), None)
        if status is not None:
            m = _STATUS_VALUE.match(status)
            status = m.group(1).capitalize() if m else None
        own_text = ' '.join(([] if region.name_unresolved else [name_text]) + [val for _, val in own_meta])
        is_option = tag == 'OP' or bool(_OPTION.search(own_text))

        if not (transaction_date and (security_name or security_symbol) and transaction_type
                and (amounts['amountMin'] is not None or amounts['amountMax'] is not None)):
            logger.info(f"   ⚠️ Skipped potential trade (incomplete): {name_text[:100]!r} {anchor.group(0)!r}")
            continue

        trades.append({
            'filerName': filer_name,
            # The notification date is the row's filing date; the signature date is the fallback
            'filingDate': notification_date or signature_date,
            'transactionDate': transaction_date,
            'securityName': security_name,
            'securitySymbol': security_symbol,
            'assetType': asset_codes.get(tag) if tag else None,
            'transactionType': transaction_type,
            'amount': amounts['amount'],  # Average/midpoint for backwards compatibility
            'amountMin': amounts['amountMin'],
            'amountMax': amounts['amountMax'],
            'amountRange': amounts['amountRange'],  # Standard range [min, max] for GSI queries
            'exactAmount': amounts['exactAmount'],  # Exact dollar amount if it was a fixed value
            'owner': region.owner,
            'formType': 'house_ptr',
            'source': 'house',
            'metadata': _format_metadata(own_meta),
            'filingStatus': status,
            'isOption': is_option,
        })
    return trades
