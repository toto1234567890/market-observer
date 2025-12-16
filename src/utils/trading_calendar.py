#!/usr/bin/env python
# coding:utf-8

from typing import Optional, Any
import exchange_calendars as ec

# --- Mapping Yahoo suffixes → exchange codes ---
EXCHANGE_MAP = {
    # Europe
    ".L": "XLON",   # London Stock Exchange
    ".PA": "XPAR",  # Euronext Paris
    ".DE": "XFRA",  # Frankfurt (XETRA)
    ".AS": "XAMS",  # Euronext Amsterdam
    ".BR": "XBRU",  # Euronext Brussels
    ".MI": "XMIL",  # Milan
    ".MC": "XMAD",  # Madrid
    ".ST": "XSTO",  # Stockholm
    ".CO": "XCSE",  # Copenhagen
    ".HE": "XHEL",  # Helsinki
    ".VI": "XWBO",  # Vienna
    ".SW": "XSWX",  # SIX Swiss Exchange
    # North America
    ".TO": "XTSE",  # Toronto
    ".V": "XTSX",   # TSX Venture
    # Asia-Pacific
    ".T": "XTKS",   # Tokyo
    ".HK": "XHKG",  # Hong Kong
    ".AX": "XASX",  # Australia
    ".KS": "XKRX",  # Korea
    ".TW": "XTAI",  # Taiwan
    ".SS": "XSHG",  # Shanghai
    ".SZ": "XSHE",  # Shenzhen
    # Fallback examples for other codes
    ".SA": "BVMF",  # Brazil B3 (limited calendar support)
}

def get_calendar(ticker: str, logger: object, instance_name: str, return_default: bool=True) -> Optional[Any]:
    suffix = next((s for s in EXCHANGE_MAP if ticker.endswith(s)), None)
    exch = EXCHANGE_MAP.get(suffix, "XNYS")  # default → U.S. NYSE
    try:
        calendar  = ec.get_calendar(exch)
    except Exception as e:
        logger.info("{0} : no trading calendar found for ticker '{1}', using default '{2}' -> {3}".format(instance_name, ticker, exch, e))
        calendar = ec.get_calendar("XNYS")
    return calendar if return_default else None