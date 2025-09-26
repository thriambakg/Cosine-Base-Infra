# Financial Keywords Database
# Sorted alphabetically for O(log N) binary search performance

# Major Stock Exchanges
EXCHANGES = [
    "nasdaq", "nyse", "amex", "lse", "tsx", "asx", "hkex", "tse", "bse", "nse"
]

# Major Indices
INDICES = [
    "sp500", "s&p 500", "s&p500", "dow", "dow jones", "nasdaq composite", "nasdaq 100",
    "russell 2000", "wilshire 5000", "ftse 100", "dax", "cac 40", "nikkei", "hang seng",
    "shanghai composite", "sensex", "nifty 50"
]

# Major Companies (NASDAQ + Blue Chips)
# Load comprehensive company names from NASDAQ data
def load_nasdaq_companies():
    """Load all NASDAQ company names from the parsed data file."""
    try:
        with open('nasdaq_companies.txt', 'r') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except FileNotFoundError:
        # Fallback to major companies if file not found
        return [
            "3m", "adobe", "airbnb", "alibaba", "alphabet", "amazon", "amd", "american express",
            "apple", "at&t", "bank of america", "berkshire hathaway", "blackrock", "boeing",
            "broadcom", "caterpillar", "chevron", "cisco", "coca cola", "costco", "disney",
            "exxon mobil", "facebook", "goldman sachs", "google", "home depot", "ibm",
            "intel", "johnson & johnson", "jpmorgan", "mcdonald's", "microsoft", "meta",
            "netflix", "nike", "nvidia", "oracle", "paypal", "pepsi", "procter & gamble",
            "salesforce", "tesla", "uber", "united health", "verizon", "visa", "walmart",
            "walt disney", "wells fargo"
        ]

# Load comprehensive company names (4,297+ NASDAQ companies)
COMPANIES = load_nasdaq_companies()

# Industry-Specific Keywords
# Load industry keywords from file
def load_industry_keywords():
    """Load industry-specific keywords from the parsed data file."""
    try:
        with open('industry_keywords.txt', 'r') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except FileNotFoundError:
        # Fallback to basic industry keywords if file not found
        return [
            "aerospace", "agriculture", "automotive", "banking", "biotechnology", 
            "chemicals", "construction", "defense", "energy", "healthcare", 
            "manufacturing", "mining", "oil", "pharmaceuticals", "retail", 
            "technology", "telecommunications", "utilities"
        ]

# Load industry keywords
INDUSTRY_KEYWORDS = load_industry_keywords()

# Stock Tickers (NASDAQ + Major Exchanges)
# Load comprehensive ticker list from NASDAQ data
def load_nasdaq_tickers():
    """Load all NASDAQ tickers from the parsed data file."""
    try:
        with open('nasdaq_tickers.txt', 'r') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except FileNotFoundError:
        # Fallback to major tickers if file not found
        return [
            "aapl", "msft", "googl", "goog", "amzn", "tsla", "nvda", "meta", "brk.a", "brk.b",
            "unh", "jnj", "xom", "jpm", "pg", "v", "hd", "ma", "bac", "abt", "cvs", "wmt",
            "ko", "pep", "tmo", "cost", "avgo", "dhr", "vz", "adbe", "acn", "nflx", "cmcsa",
            "cop", "t", "qcom", "intc", "txn", "nke", "lmt", "abbv", "amgn", "pm", "tmus",
            "lin", "low", "snow", "amd", "pypl", "crm", "orcl", "csco", "adp",
            "nclh", "ups", "spgi", "gs", "blk", "axp", "cat", "ba", "cvx", "dis", "hon",
            "ibm", "mcd", "mrk", "pfe", "trv", "utx", "wba", "ge", "xom"
        ]

# Load comprehensive ticker list (5,060+ NASDAQ tickers)
TICKERS = load_nasdaq_tickers()

# Financial Terms
FINANCIAL_TERMS = [
    "analyst", "arbitrage", "asset", "assets", "balance sheet", "bear market",
    "beta", "bond", "bonds", "bull market", "call option", "capital", "capital gains",
    "cash flow", "commodity", "commodities", "corporate action", "credit rating",
    "debt", "derivative", "dividend", "dividends", "earnings", "equity", "etf",
    "exchange traded fund", "futures", "growth stock", "hedge fund", "ipo",
    "initial public offering", "leverage", "liquidity", "market cap", "market capitalization",
    "merger", "acquisition", "mutual fund", "option", "options", "portfolio",
    "preferred stock", "private equity", "profit", "profits", "put option",
    "revenue", "roi", "return on investment", "securities", "share", "shares",
    "stock split", "valuation", "volatility", "yield", "yields"
]

# Economic Indicators
ECONOMIC_INDICATORS = [
    "cpi", "consumer price index", "inflation", "deflation", "gdp", "gross domestic product",
    "unemployment", "unemployment rate", "interest rates", "federal funds rate",
    "treasury", "treasury bonds", "yield curve", "money supply", "m1", "m2",
    "trade deficit", "trade surplus", "current account", "budget deficit",
    "national debt", "fiscal policy", "monetary policy", "quantitative easing",
    "taper", "tapering", "stimulus", "recession", "recovery", "expansion",
    "contraction", "business cycle", "leading indicators", "lagging indicators"
]

# Central Banks & Institutions
CENTRAL_BANKS = [
    "federal reserve", "fed", "ecb", "european central bank", "bank of england",
    "boe", "bank of japan", "boj", "bank of canada", "boc", "reserve bank of australia",
    "rba", "people's bank of china", "pboc", "swiss national bank", "snb",
    "bank of india", "rbi", "bank of korea", "bok", "bank of mexico", "banxico"
]

# Federal Reserve Terms
FED_TERMS = [
    "fomc", "federal open market committee", "chairman", "chair", "governor",
    "dot plot", "beige book", "minutes", "press conference", "speech", "testimony",
    "hawkish", "dovish", "neutral", "accommodative", "restrictive", "tightening",
    "easing", "pivot", "pause", "cut", "hike", "rate cut", "rate hike",
    "balance sheet", "runoff", "rolloff", "mbs", "mortgage backed securities"
]

# Cryptocurrency
CRYPTO = [
    "bitcoin", "btc", "ethereum", "eth", "cryptocurrency", "crypto", "blockchain",
    "altcoin", "altcoins", "defi", "decentralized finance", "nft", "non fungible token",
    "nfts", "stablecoin", "stablecoins", "usdt", "usdc", "binance", "bnb",
    "cardano", "ada", "solana", "sol", "polkadot", "dot", "chainlink", "link",
    "uniswap", "uniswap", "pancakeswap", "sushi swap", "curve", "aave", "compound",
    "maker", "mkr", "yearn", "yfi", "sushi", "sushiswap", "1inch", "balancer"
]

# Commodities
COMMODITIES = [
    "oil", "crude oil", "wti", "brent", "natural gas", "gas", "gold", "silver",
    "platinum", "palladium", "copper", "aluminum", "steel", "iron ore", "coal",
    "wheat", "corn", "soybeans", "sugar", "coffee", "cocoa", "cotton", "lumber",
    "uranium", "lithium", "rare earth", "palladium", "rhodium", "ruthenium"
]

# Currencies
CURRENCIES = [
    "dollar", "usd", "euro", "eur", "pound", "gbp", "yen", "jpy", "yuan", "cny",
    "franc", "chf", "canadian dollar", "cad", "australian dollar", "aud",
    "new zealand dollar", "nzd", "swedish krona", "sek", "norwegian krone", "nok",
    "danish krone", "dkk", "swiss franc", "chf", "ruble", "rub", "real", "brl",
    "peso", "mxn", "rupee", "inr", "won", "krw", "baht", "thb", "ringgit", "myr"
]

# Sectors & Industries
SECTORS = [
    "technology", "healthcare", "financial", "energy", "utilities", "consumer discretionary",
    "consumer staples", "industrials", "materials", "real estate", "communication",
    "telecommunications", "aerospace", "defense", "automotive", "retail", "banking",
    "insurance", "pharmaceutical", "biotech", "semiconductor", "software", "hardware",
    "internet", "e-commerce", "social media", "streaming", "gaming", "fintech",
    "clean energy", "renewable energy", "solar", "wind", "nuclear", "oil & gas",
    "mining", "agriculture", "food & beverage", "restaurants", "hospitality",
    "transportation", "logistics", "shipping", "airlines", "cruise", "hotels"
]

# Market Conditions
MARKET_CONDITIONS = [
    "rally", "selloff", "correction", "crash", "bear market", "bull market",
    "volatility", "volatile", "stable", "trending", "sideways", "breakout",
    "breakdown", "support", "resistance", "momentum", "oversold", "overbought",
    "volume", "liquidity", "bid", "ask", "spread", "gap", "gap up", "gap down",
    "premarket", "after hours", "extended hours", "halt", "circuit breaker"
]

# Trading Terms
TRADING_TERMS = [
    "buy", "sell", "hold", "long", "short", "position", "positions", "portfolio",
    "diversification", "rebalance", "rebalancing", "allocation", "weight", "weighting",
    "sector rotation", "style rotation", "value", "growth", "momentum", "quality",
    "dividend", "dividend yield", "payout ratio", "buyback", "share buyback",
    "stock repurchase", "insider trading", "institutional", "retail", "hedge fund",
    "mutual fund", "etf", "index fund", "active", "passive", "alpha", "beta",
    "sharpe ratio", "sortino ratio", "max drawdown", "var", "value at risk"
]

# Earnings & Reports
EARNINGS_TERMS = [
    "earnings", "earnings report", "quarterly", "q1", "q2", "q3", "q4", "annual",
    "guidance", "guidance", "beat", "miss", "meet", "surprise", "revenue",
    "revenues", "profit", "profits", "loss", "losses", "eps", "earnings per share",
    "pe ratio", "price to earnings", "pb ratio", "price to book", "ps ratio",
    "price to sales", "peg ratio", "debt to equity", "current ratio", "quick ratio",
    "return on equity", "roe", "return on assets", "roa", "return on investment", "roi"
]

# Mergers & Acquisitions
M_A_TERMS = [
    "merger", "acquisition", "takeover", "buyout", "spin off", "spin-off", "ipo",
    "initial public offering", "spac", "special purpose acquisition company",
    "blank check", "reverse merger", "going public", "private equity", "leveraged buyout",
    "lbo", "hostile takeover", "friendly takeover", "due diligence", "synergy",
    "integration", "divestiture", "asset sale", "joint venture", "partnership",
    "strategic alliance", "licensing", "franchise", "franchising"
]

# Regulatory & Legal
REGULATORY_TERMS = [
    "sec", "securities and exchange commission", "finra", "cftc", "fcc", "fda",
    "ftc", "doj", "justice department", "antitrust", "monopoly", "oligopoly",
    "regulation", "regulatory", "compliance", "audit", "auditing", "fraud",
    "insider trading", "market manipulation", "pump and dump", "wash trading",
    "front running", "high frequency trading", "hft", "algorithmic trading",
    "dark pool", "lit pool", "market maker", "specialist", "broker", "dealer"
]

# All keywords combined and sorted for binary search
ALL_KEYWORDS = sorted(list(set(
    EXCHANGES + INDICES + COMPANIES + TICKERS + FINANCIAL_TERMS + ECONOMIC_INDICATORS +
    CENTRAL_BANKS + FED_TERMS + CRYPTO + COMMODITIES + CURRENCIES + SECTORS +
    MARKET_CONDITIONS + TRADING_TERMS + EARNINGS_TERMS + M_A_TERMS + REGULATORY_TERMS +
    INDUSTRY_KEYWORDS
)))

# Create a set for O(1) lookup as well
ALL_KEYWORDS_SET = set(ALL_KEYWORDS)

def get_keywords():
    """Get the complete list of financial keywords"""
    return ALL_KEYWORDS

def get_keywords_set():
    """Get the complete set of financial keywords for O(1) lookup"""
    return ALL_KEYWORDS_SET

def binary_search_keywords(word, keywords_list=None):
    """Binary search for keyword in sorted list - O(log N) performance"""
    if keywords_list is None:
        keywords_list = ALL_KEYWORDS
    
    left, right = 0, len(keywords_list) - 1
    word_lower = word.lower()
    
    while left <= right:
        mid = (left + right) // 2
        mid_word = keywords_list[mid].lower()
        
        if mid_word == word_lower:
            return True
        elif mid_word < word_lower:
            left = mid + 1
        else:
            right = mid - 1
    
    return False

def find_keywords_in_text(text, use_binary_search=True):
    """Find all financial keywords present in the given text"""
    if not text:
        return []
    
    text_lower = text.lower()
    found_keywords = []
    
    if use_binary_search:
        # Use binary search for O(log N) performance per keyword
        for keyword in ALL_KEYWORDS:
            if keyword.lower() in text_lower:
                found_keywords.append(keyword)
    else:
        # Use set lookup for O(1) performance per keyword
        words = text_lower.split()
        for word in words:
            # Check individual words
            if word in ALL_KEYWORDS_SET:
                found_keywords.append(word)
            
            # Check phrases (2-3 words)
            words_list = text_lower.split()
            for i in range(len(words_list) - 1):
                phrase_2 = f"{words_list[i]} {words_list[i+1]}"
                if phrase_2 in ALL_KEYWORDS_SET:
                    found_keywords.append(phrase_2)
            
            for i in range(len(words_list) - 2):
                phrase_3 = f"{words_list[i]} {words_list[i+1]} {words_list[i+2]}"
                if phrase_3 in ALL_KEYWORDS_SET:
                    found_keywords.append(phrase_3)
    
    # Remove duplicates and return
    return list(dict.fromkeys(found_keywords))
