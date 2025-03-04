from technical_analysis import indicators
from technical_analysis.backtest.strategy import (
    MovingAverageCrossover,
    CenterLineCrossover,
    Divergence,
)

strategy = [
    {
        "name": "ema9/ema20",
        "columns_req": {
            "ema9": {"func": indicators.ema, "kwargs": {"period": 9}},
            "ema20": {"func": indicators.ema, "kwargs": {"period": 20}},
        },
        "entry": [MovingAverageCrossover("ema9", "ema20", "bullish")],
        "exit": [MovingAverageCrossover("ema9", "ema20", "bearish")],
    },
    {
        "name": "ema50/ema200",
        "columns_req": {
            "ema50": {"func": indicators.ema, "kwargs": {"period": 50}},
            "ema200": {"func": indicators.ema, "kwargs": {"period": 200}},
        },
        "entry": [MovingAverageCrossover("ema50", "ema200", "bullish")],
        "exit": [MovingAverageCrossover("ema50", "ema200", "bearish")],
    },
    {
        "name": "macd_Xover",
        "columns_req": {
            "macd": {"func": indicators.macd, "kwargs": {"output": ["macd"]}},
            "macd_sig": {"func": indicators.macd, "kwargs": {"output": ["signal"]}},
        },
        "entry": [MovingAverageCrossover("macd", "macd_sig", "bullish")],
        "exit": [MovingAverageCrossover("macd", "macd_sig", "bearish")],
    },
    {
        "name": "macd_0over",
        "columns_req": {},
        "entry": [CenterLineCrossover("macd", "bullish")],
        "exit": [CenterLineCrossover("macd", "bearish")],
    },
    {
        "name": "macd_div&Xover",
        "columns_req": {},
        "entry": [
            Divergence("low", "macd", "bullish", lookback_periods=5),
            MovingAverageCrossover("macd", "macd_sig", "bullish"),
        ],
        "exit": [
            Divergence("high", "macd", "bearish", lookback_periods=5),
            MovingAverageCrossover("macd", "macd_sig", "bearish"),
        ],
    },
    {
        "name": "rsi",
        "columns_req": {
            "rsi": {"func": indicators.rsi, "kwargs": {"period": 14}},
        },
        "entry": [
            "rsi < 30",
            Divergence("low", "rsi", "bullish", lookback_periods=5),
        ],
        "exit": [
            "rsi > 70",
            Divergence("high", "rsi", "bearish", lookback_periods=5),
        ],
    },
    {
        "name": "stochastic_0over",
        "columns_req": {
            "stoD": {
                "func": indicators.stochastic,
                "kwargs": {"period": 14, "output": ["perc_d"]},
            },
            "stoK": {
                "func": indicators.stochastic,
                "kwargs": {"period": 14, "output": ["perc_k"]},
            },
        },
        "entry": [
            CenterLineCrossover("stoK", "bullish", 20),
            # MovingAverageCrossover('stoK', 'stoD', 'bullish'),
            # Divergence("low", "stoK", "bullish", lookback_periods=5),
        ],
        "exit": [
            CenterLineCrossover("stoK", "bearish", 80),
            # MovingAverageCrossover('stoK', 'stoD', 'bearish'),
            # Divergence("high", "stoK", "bearish", lookback_periods=5),
        ],
    },
    {
        "name": "obv",
        "columns_req": {
            "obv": {"func": indicators.obv, "kwargs": {}},
        },
        "entry": [
            Divergence("low", "obv", "bullish", lookback_periods=10),
        ],
        "exit": [
            Divergence("high", "obv", "bearish", lookback_periods=10),
        ],
    },
    {
        "name": "ad",
        "columns_req": {
            "ad": {"func": indicators.ad, "kwargs": {}},
        },
        "entry": [
            Divergence("low", "ad", "bullish", lookback_periods=10),
        ],
        "exit": [
            Divergence("high", "ad", "bearish", lookback_periods=10),
        ],
    },
    {
        "name": "adx",
        "columns_req": {
            "adx": {"func": indicators.adx, "kwargs": {"output": ["adx"]}},
            "-DI": {"func": indicators.adx, "kwargs": {"output": ["-DI"]}},
            "+DI": {"func": indicators.adx, "kwargs": {"output": ["+DI"]}},
        },
        "entry": ["adx > 25", MovingAverageCrossover("+DI", "-DI", "bullish")],
        "exit": ["adx > 25", MovingAverageCrossover("+DI", "-DI", "bearish")],
    },
]
