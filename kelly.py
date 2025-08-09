import logging
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import pyupbit
import yfinance as yf

logger = logging.getLogger(__name__)


def _detect_symbol_type(symbol: str) -> str:
    """
    심볼 타입 감지 (crypto vs stock)

    Returns:
        "crypto": KRW-BTC, BTC-ETH 등
        "stock": 005930, 000660 등
        "unknown": 그 외
    """
    if not symbol:
        return "unknown"
    if "-" in symbol:
        return "crypto"
    if symbol.isdigit() and len(symbol) == 6:
        return "stock"
    return "unknown"


def _get_crypto_candles(symbol: str, interval: str = "day", count: int = 30) -> pd.DataFrame:
    """Upbit에서 과거 캔들 데이터 가져오기 (기본: 최근 30 일봉)"""
    try:
        df = pyupbit.get_ohlcv(symbol, interval=interval, count=count)
        if df is None or df.empty:
            logger.warning(f"캔들 데이터 조회 실패: {symbol}")
            return pd.DataFrame()
        logger.info(f"📊 {symbol} {interval} 캔들 {len(df)}개 조회 완료")
        return df
    except Exception as exc:
        logger.error(f"캔들 데이터 조회 오류: {exc}")
        return pd.DataFrame()


def _get_stock_history(stock_code: str, days: int = 30) -> pd.DataFrame:
    """yfinance를 사용해 한국 주식 데이터 가져오기 (기본: 최근 30일)"""
    try:
        if stock_code.isdigit() and len(stock_code) == 6:
            yf_ticker = f"{stock_code}.KS"
        else:
            yf_ticker = stock_code

        logger.info(f"📊 yfinance에서 {yf_ticker} 데이터 조회 중...")

        stock = yf.Ticker(yf_ticker)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days + 10)
        hist = stock.history(start=start_date, end=end_date, interval="1d")

        if hist.empty:
            logger.warning(f"yfinance 데이터 조회 실패: {yf_ticker}")
            return pd.DataFrame()

        hist = hist.tail(days) if len(hist) > days else hist
        logger.info(f"✅ {yf_ticker} 일봉 데이터 {len(hist)}개 조회 완료")
        return hist
    except Exception as exc:
        logger.error(f"yfinance 데이터 조회 오류 {stock_code}: {exc}")
        return pd.DataFrame()


def calculate_dynamic_kelly_fraction(
    symbol: str,
    available_krw: float,
    volatility_symbol: Optional[str] = None,
) -> Tuple[float, Dict[str, Any]]:
    """
    동적 Kelly Fraction 계산 (변동성 적응형)

    - 크립토: Upbit 일봉 30개 기준 변동성
    - 주식: yfinance 일봉 30개 기준 변동성
    - 변동성 구간별 Kelly: 40%/30%/20%/15% (최종 10%~50%로 클램프)
    - 반환값: (투자금액KRW, 상세지표)
    """
    try:
        logger.info("🚀 동적 Kelly Fraction 계산 시작")

        vol_symbol = volatility_symbol if volatility_symbol else symbol
        symbol_type = _detect_symbol_type(vol_symbol)

        if symbol_type == "crypto":
            df = _get_crypto_candles(vol_symbol, interval="day", count=30)
            price_column = "close"
        elif symbol_type == "stock":
            df = _get_stock_history(vol_symbol, days=30)
            price_column = "Close"
        else:
            df = pd.DataFrame()
            price_column = "close"

        if not df.empty and price_column in df.columns:
            daily_returns = df[price_column].pct_change().dropna()
            volatility = float(daily_returns.std())
        else:
            volatility = 0.02  # 기본값 2%
            logger.warning(f"변동성 계산 실패: {vol_symbol}, 기본값 2% 사용")

        # 변동성 구간별 Kelly
        if volatility <= 0.01:
            tier_kelly = 0.40
            tier_name = "저변동성(공격적)"
        elif volatility <= 0.02:
            tier_kelly = 0.30
            tier_name = "보통변동성(균형)"
        elif volatility <= 0.03:
            tier_kelly = 0.20
            tier_name = "중변동성(보수적)"
        else:
            tier_kelly = 0.15
            tier_name = "고변동성(안전)"

        # 부가 지표
        base_kelly = 0.25
        volatility_adjusted_kelly = base_kelly * (0.02 / max(volatility, 0.005))
        volatility_kelly = max(0.10, min(volatility_adjusted_kelly, 0.50))
        fixed_kelly = 0.25
        aggressive_kelly = 0.50

        kelly_fraction = max(0.10, min(tier_kelly, 0.50))
        kelly_amount = available_krw * kelly_fraction
        final_amount = max(kelly_amount, 5000)
        final_amount = min(final_amount, available_krw)

        stats: Dict[str, Any] = {
            "method": f"구간별_Kelly_{tier_name}",
            "volatility": volatility,
            "volatility_kelly": volatility_kelly,
            "fixed_kelly": fixed_kelly,
            "aggressive_kelly": aggressive_kelly,
            "tier_kelly": tier_kelly,
            "tier_name": tier_name,
            "kelly_fraction": kelly_fraction,
            "kelly_amount": kelly_amount,
            "final_amount": final_amount,
            "available_krw": available_krw,
            "min_threshold": 0.10,
            "max_threshold": 0.50,
        }

        logger.info(
            "✅ 동적 Kelly 계산: 변동성 %.2f%%, 선택 %s, 최종 %.1f%% → 금액 %,.0f원",
            volatility * 100,
            tier_name,
            kelly_fraction * 100,
            final_amount,
        )

        return final_amount, stats
    except Exception as exc:
        logger.error(f"동적 Kelly 계산 오류: {exc}")
        safe_amount = max(available_krw * 0.25, 5000)
        return safe_amount, {"method": "error", "kelly_fraction": 0.25}



