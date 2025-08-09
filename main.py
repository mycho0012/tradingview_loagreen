from fastapi import FastAPI, Request, HTTPException
import pyupbit
import uvicorn
import os
import logging
import json
import requests
import pytz
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
from kelly import calculate_dynamic_kelly_fraction
from notion_client import Client as NotionClient

# 로깅 설정 (먼저 설정)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# .env 파일 자동 로딩
def load_env_file(env_file: str = ".env"):
    """환경변수 파일을 로드"""
    env_path = Path(env_file)
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    os.environ[key] = value
        logger.info(f"✅ {env_file} 파일 로딩 완료")
    else:
        logger.warning(f"⚠️  {env_file} 파일을 찾을 수 없습니다.")

# .env 파일 로딩 (main.py와 같은 디렉토리에서)
load_env_file(".env")

# --- 설정 (Configuration) ---
# Required: pip install yfinance (for individual stock volatility)
# Upbit 설정 (환경 변수에서 API 키 로드)
UPBIT_ACCESS_KEY = os.getenv('UPBIT_ACCESS_KEY')
UPBIT_SECRET_KEY = os.getenv('UPBIT_SECRET_KEY')

# KIS 설정 (환경 변수에서 API 키 로드)
KIS_APPKEY = os.getenv('KIS_APPKEY')
KIS_APPSECRET = os.getenv('KIS_APPSECRET')
KIS_ACCOUNT_PREFIX = os.getenv('KIS_ACCOUNT_PREFIX')
KIS_ACCOUNT_SUFFIX = os.getenv('KIS_ACCOUNT_SUFFIX')
KIS_BASE_URL = os.getenv('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443')

# 보안 설정
MY_SECRET_PASSPHRASE = os.getenv('PASSPHRASE', "YourSuperSecretPassword")

# Notion 설정
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
NOTION_DATABASE_ID = os.getenv('NOTION_DATABASE_ID')
TIMEZONE_NAME = os.getenv('TIMEZONE', 'Asia/Seoul')

def _parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y"}

# 동작 정책 (환경변수 다양한 철자 수용)
_dup_candidates = [
    os.getenv('ALLOW_DUPLICATE_BUY'),
    os.getenv('ALLOWED_DUPLICATE_BUY'),
    os.getenv('Allowed_duplicatePbuy'),  # 사용자가 입력한 철자도 허용
]
ALLOW_DUPLICATE_BUY = _parse_bool(next((v for v in _dup_candidates if v is not None), None), False)

app = FastAPI(title="TradingView to Multi-Exchange Webhook", version="2.0.0")

# Upbit 클라이언트 초기화
upbit = None
if UPBIT_ACCESS_KEY and UPBIT_SECRET_KEY:
    upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
    logger.info("✅ Upbit 클라이언트 초기화 완료")
else:
    logger.warning("⚠️ Upbit API 키가 설정되지 않았습니다.")

# KIS API 토큰 관리
kis_access_token = None
kis_token_issued_at = None

# Notion 클라이언트 초기화
notion: Optional[NotionClient] = None
if NOTION_API_KEY and NOTION_DATABASE_ID:
    try:
        notion = NotionClient(auth=NOTION_API_KEY)
        logger.info("✅ Notion 클라이언트 초기화 완료")
    except Exception as e:
        logger.warning(f"⚠️ Notion 클라이언트 초기화 실패: {e}")

# Notion DB 메타 캐시
_notion_db_meta: Optional[Dict[str, Any]] = None

def _fetch_notion_db_meta() -> Dict[str, Any]:
    global _notion_db_meta
    if _notion_db_meta is not None:
        return _notion_db_meta
    meta: Dict[str, Any] = {"props": {}, "status_options": []}
    if notion and NOTION_DATABASE_ID:
        try:
            db = notion.databases.retrieve(NOTION_DATABASE_ID)
            props = db.get("properties", {})
            meta["props"] = props
            if "Status" in props and props["Status"].get("type") == "status":
                meta["status_options"] = [
                    opt.get("name") for opt in props["Status"]["status"].get("options", [])
                ]
        except Exception as e:
            logger.warning(f"⚠️ Notion DB 메타 조회 실패: {e}")
    _notion_db_meta = meta
    return meta

# --- 포트폴리오 관리 함수 ---
def get_current_balance(currency: str = "KRW") -> float:
    """현재 잔고 조회"""
    if not upbit:
        return 0.0
    
    try:
        balances = upbit.get_balances()
        for balance in balances:
            if balance['currency'] == currency:
                return float(balance['balance'])
        return 0.0
    except Exception as e:
        logger.error(f"잔고 조회 오류: {e}")
        return 0.0

def get_current_position(symbol: str) -> float:
    """현재 포지션 수량 조회 (예: BTC 보유 수량)"""
    if not upbit:
        return 0.0
    
    # KRW-BTC에서 BTC 추출
    currency = symbol.split('-')[1]
    
    try:
        balances = upbit.get_balances()
        for balance in balances:
            if balance['currency'] == currency:
                return float(balance['balance'])
        return 0.0
    except Exception as e:
        logger.error(f"포지션 조회 오류: {e}")
        return 0.0


def get_upbit_last_price(symbol: str) -> Optional[float]:
    try:
        price = pyupbit.get_current_price(symbol)
        return float(price) if price is not None else None
    except Exception as e:
        logger.warning(f"현재가 조회 실패: {symbol}, {e}")
        return None


def calculate_sell_quantity(symbol: str) -> float:
    """매도할 전체 수량 계산"""
    return get_current_position(symbol)

# --- Upbit 주문 처리 함수 ---
def place_upbit_order(symbol: str, side: str, quantity: float, order_type: str = "market") -> Dict[str, Any]:
    """
    Upbit 주문을 처리하는 함수
    
    Args:
        symbol: 마켓 심볼 (예: KRW-BTC)
        side: 매수/매도 ("buy" or "sell")
        quantity: 주문 수량/금액
        order_type: 주문 타입 (기본값: "market")
    
    Returns:
        주문 결과 딕셔너리
    """
    if not upbit:
        raise Exception("Upbit 클라이언트가 초기화되지 않았습니다.")
    
    try:
        if side.lower() == 'buy':
            # 시장가 매수: quantity는 주문 총액(KRW)
            result = upbit.buy_market_order(symbol, quantity)
            logger.info(f"Upbit 매수 주문 완료: {symbol}, 금액: {quantity} KRW")
        elif side.lower() == 'sell':
            # 시장가 매도: quantity는 코인 수량
            result = upbit.sell_market_order(symbol, quantity)
            logger.info(f"Upbit 매도 주문 완료: {symbol}, 수량: {quantity}")
        else:
            raise ValueError(f"지원하지 않는 주문 방향: {side}")
        
        return result
    except Exception as e:
        logger.error(f"Upbit 주문 오류: {e}")
        raise

def validate_upbit_symbol(symbol: str) -> bool:
    """
    Upbit 심볼 형식 검증
    """
    if not symbol or '-' not in symbol:
        return False
    
    parts = symbol.split('-')
    if len(parts) != 2:
        return False
    
    market, coin = parts
    return market in ['KRW', 'BTC', 'USDT'] and len(coin) > 0

# --- 심볼 라우팅 함수 ---
def detect_symbol_type(symbol: str) -> str:
    """
    심볼 타입 감지 (crypto vs stock)
    
    Returns:
        "crypto": KRW-BTC, BTC-ETH 등
        "stock": 005930, 000660 등 
    """
    if not symbol:
        return "unknown"
    
    # 크립토: 하이픈 포함
    if '-' in symbol:
        return "crypto"
    
    # 주식: 숫자로만 구성 (6자리 주식 코드)
    if symbol.isdigit() and len(symbol) == 6:
        return "stock"
    
    return "unknown"


# --- Notion 연동 함수 ---
def _notion_safe_select(name: str) -> Optional[Dict[str, Any]]:
    return {"name": name} if name else None


def _notion_pick_status(name: str) -> Optional[Dict[str, Any]]:
    meta = _fetch_notion_db_meta()
    options = meta.get("status_options", [])
    if not options:
        return None
    picked = name if name in options else options[0]
    return {"name": picked}


def _now_in_tz() -> datetime:
    try:
        tz = pytz.timezone(TIMEZONE_NAME)
        return datetime.now(tz)
    except Exception:
        return datetime.now()


def _create_notion_trade_page(
    title: str,
    timestamp: datetime,
    asset: str,
    status: str,
    position: str,
    strategy: str,
    interval: str,
    entry_price: Optional[float],
    exit_price: Optional[float],
    quantity: Optional[float],
    fee: Optional[float],
    order_id: str,
    webhook_json: Dict[str, Any],
) -> Optional[str]:
    """Notion 데이터베이스에 거래 기록 생성"""
    if not notion:
        return None
    try:
        props_meta = _fetch_notion_db_meta().get("props", {})
        properties: Dict[str, Any] = {}

        if "Trade ID" in props_meta:
            properties["Trade ID"] = {
                "title": [
                    {"type": "text", "text": {"content": title[:200]}}
                ]
            }
        if "Time Stamp" in props_meta:
            properties["Time Stamp"] = {"date": {"start": timestamp.isoformat()}}
        if "Asset" in props_meta:
            sel = _notion_safe_select(asset)
            if sel:
                properties["Asset"] = {"select": sel}
        if "Status" in props_meta:
            st = _notion_pick_status(status)
            if st:
                properties["Status"] = {"status": st}
        if "Position" in props_meta:
            sel = _notion_safe_select(position)
            if sel:
                properties["Position"] = {"select": sel}
        if "Strategy" in props_meta:
            sel = _notion_safe_select(strategy)
            if sel:
                properties["Strategy"] = {"select": sel}
        if "Interval" in props_meta:
            sel = _notion_safe_select(interval)
            if sel:
                properties["Interval"] = {"select": sel}
        if "Entry Price" in props_meta and entry_price is not None:
            properties["Entry Price"] = {"number": float(entry_price)}
        if "Exit Price" in props_meta and exit_price is not None:
            properties["Exit Price"] = {"number": float(exit_price)}
        if "Quantity" in props_meta and quantity is not None:
            properties["Quantity"] = {"number": float(quantity)}
        if "Fee" in props_meta and fee is not None:
            properties["Fee"] = {"number": float(fee)}
        if "Order ID" in props_meta:
            properties["Order ID"] = {"rich_text": [{"type": "text", "text": {"content": str(order_id)[:200]}}]}
        if "Webhook Data" in props_meta:
            properties["Webhook Data"] = {"rich_text": [{"type": "text", "text": {"content": json.dumps(webhook_json)[:2000]}}]}

        # Fallback: 스키마 매칭이 하나도 안 된 경우 표준 필드로 강제 생성 시도
        if not properties:
            logger.warning("⚠️ Notion properties 비어 있음. 표준 필드로 생성 시도")
            properties = {
                "Trade ID": {"title": [{"type": "text", "text": {"content": title[:200]}}]},
                "Time Stamp": {"date": {"start": timestamp.isoformat()}},
                "Status": {"status": _notion_pick_status(status) or {}},
                "Asset": {"select": _notion_safe_select(asset) or {}},
                "Position": {"select": _notion_safe_select(position) or {}},
                "Strategy": {"select": _notion_safe_select(strategy) or {}},
                "Order ID": {"rich_text": [{"type": "text", "text": {"content": str(order_id)[:200]}}]},
                "Webhook Data": {"rich_text": [{"type": "text", "text": {"content": json.dumps(webhook_json)[:2000]}}]},
            }
            if entry_price is not None:
                properties["Entry Price"] = {"number": float(entry_price)}
            if exit_price is not None:
                properties["Exit Price"] = {"number": float(exit_price)}
            if quantity is not None:
                properties["Quantity"] = {"number": float(quantity)}
            if fee is not None:
                properties["Fee"] = {"number": float(fee)}

        page = notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, properties=properties)
        page_id = page.get("id")
        logger.info(f"📝 Notion 페이지 생성 성공: {page_id}")
        return page_id
    except Exception as e:
        try:
            from httpx import HTTPStatusError
            if isinstance(e, HTTPStatusError) and getattr(e, "response", None) is not None:
                logger.error(f"❌ Notion 페이지 생성 실패: HTTP {e.response.status_code} {e.response.text}")
            else:
                logger.error(f"❌ Notion 페이지 생성 실패: {e}")
        except Exception:
            logger.error(f"❌ Notion 페이지 생성 실패: {e}")
        return None


def _update_notion_trade_page(
    page_id: str,
    *,
    status: Optional[str] = None,
    position: Optional[str] = None,
    strategy: Optional[str] = None,
    interval: Optional[str] = None,
    entry_price: Optional[float] = None,
    exit_price: Optional[float] = None,
    quantity: Optional[float] = None,
    fee: Optional[float] = None,
    order_id: Optional[str] = None,
) -> bool:
    if not notion or not page_id:
        return False
    try:
        props_meta = _fetch_notion_db_meta().get("props", {})
        properties: Dict[str, Any] = {}

        if status is not None and "Status" in props_meta:
            st = _notion_pick_status(status)
            if st:
                properties["Status"] = {"status": st}
        if position is not None and "Position" in props_meta:
            sel = _notion_safe_select(position)
            if sel:
                properties["Position"] = {"select": sel}
        if strategy is not None and "Strategy" in props_meta:
            sel = _notion_safe_select(strategy)
            if sel:
                properties["Strategy"] = {"select": sel}
        if interval is not None and "Interval" in props_meta:
            sel = _notion_safe_select(interval)
            if sel:
                properties["Interval"] = {"select": sel}
        if entry_price is not None and "Entry Price" in props_meta:
            properties["Entry Price"] = {"number": float(entry_price)}
        if exit_price is not None and "Exit Price" in props_meta:
            properties["Exit Price"] = {"number": float(exit_price)}
        if quantity is not None and "Quantity" in props_meta:
            properties["Quantity"] = {"number": float(quantity)}
        if fee is not None and "Fee" in props_meta:
            properties["Fee"] = {"number": float(fee)}
        if order_id is not None and "Order ID" in props_meta:
            properties["Order ID"] = {"rich_text": [{"type": "text", "text": {"content": str(order_id)[:200]}}]}

        if not properties:
            # Fallback: 표준 필드명으로 강제 업데이트 시도
            if status is not None:
                properties["Status"] = {"status": _notion_pick_status(status) or {}}
            if position is not None:
                properties["Position"] = {"select": _notion_safe_select(position) or {}}
            if strategy is not None:
                properties["Strategy"] = {"select": _notion_safe_select(strategy) or {}}
            if interval is not None:
                properties["Interval"] = {"select": _notion_safe_select(interval) or {}}
            if entry_price is not None:
                properties["Entry Price"] = {"number": float(entry_price)}
            if exit_price is not None:
                properties["Exit Price"] = {"number": float(exit_price)}
            if quantity is not None:
                properties["Quantity"] = {"number": float(quantity)}
            if fee is not None:
                properties["Fee"] = {"number": float(fee)}
            if order_id is not None:
                properties["Order ID"] = {"rich_text": [{"type": "text", "text": {"content": str(order_id)[:200]}}]}

        notion.pages.update(page_id=page_id, properties=properties)
        logger.info(f"📝 Notion 페이지 업데이트 성공: {page_id}")
        return True
    except Exception as e:
        try:
            from httpx import HTTPStatusError
            if isinstance(e, HTTPStatusError) and getattr(e, "response", None) is not None:
                logger.error(f"❌ Notion 페이지 업데이트 실패: HTTP {e.response.status_code} {e.response.text}")
            else:
                logger.error(f"❌ Notion 페이지 업데이트 실패: {e}")
        except Exception:
            logger.error(f"❌ Notion 페이지 업데이트 실패: {e}")
        return False

    try:
        props_meta = _fetch_notion_db_meta().get("props", {})
        properties: Dict[str, Any] = {}

        if "Trade ID" in props_meta:
            properties["Trade ID"] = {
                "title": [
                    {"type": "text", "text": {"content": title[:200]}}
                ]
            }
        if "Time Stamp" in props_meta:
            properties["Time Stamp"] = {"date": {"start": timestamp.isoformat()}}
        if "Asset" in props_meta:
            sel = _notion_safe_select(asset)
            if sel:
                properties["Asset"] = {"select": sel}
        if "Status" in props_meta:
            st = _notion_pick_status(status)
            if st:
                properties["Status"] = {"status": st}
        if "Position" in props_meta:
            sel = _notion_safe_select(position)
            if sel:
                properties["Position"] = {"select": sel}
        if "Strategy" in props_meta:
            sel = _notion_safe_select(strategy)
            if sel:
                properties["Strategy"] = {"select": sel}
        if "Interval" in props_meta:
            sel = _notion_safe_select(interval)
            if sel:
                properties["Interval"] = {"select": sel}
        if "Entry Price" in props_meta and entry_price is not None:
            properties["Entry Price"] = {"number": float(entry_price)}
        if "Exit Price" in props_meta and exit_price is not None:
            properties["Exit Price"] = {"number": float(exit_price)}
        if "Quantity" in props_meta and quantity is not None:
            properties["Quantity"] = {"number": float(quantity)}
        if "Fee" in props_meta and fee is not None:
            properties["Fee"] = {"number": float(fee)}
        if "Order ID" in props_meta:
            properties["Order ID"] = {"rich_text": [{"type": "text", "text": {"content": str(order_id)[:200]}}]}
        if "Webhook Data" in props_meta:
            properties["Webhook Data"] = {"rich_text": [{"type": "text", "text": {"content": json.dumps(webhook_json)[:2000]}}]}

        if not properties:
            logger.warning("⚠️ Notion properties 비어 있음. 스키마 확인 필요")
            return None

        page = notion.pages.create(parent={"database_id": NOTION_DATABASE_ID}, properties=properties)
        return page.get("id")
    except Exception as e:
        try:
            from httpx import HTTPStatusError
            if isinstance(e, HTTPStatusError) and getattr(e, "response", None) is not None:
                logger.error(f"❌ Notion 페이지 생성 실패: HTTP {e.response.status_code} {e.response.text}")
            else:
                logger.error(f"❌ Notion 페이지 생성 실패: {e}")
        except Exception:
            logger.error(f"❌ Notion 페이지 생성 실패: {e}")
        return None

# --- KIS Market Hours Check ---
def is_kis_market_open() -> bool:
    """
    Check if Korean stock market is open
    Trading hours: 09:00 - 15:30 KST (Korean Standard Time)
    Monday to Friday, excluding holidays
    """
    try:
        # Get current time in Korea timezone
        korea_tz = pytz.timezone('Asia/Seoul')
        korea_time = datetime.now(korea_tz)
        
        # Check if it's weekend
        if korea_time.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check trading hours (09:00 - 15:30)
        market_open = korea_time.replace(hour=9, minute=0, second=0, microsecond=0)
        market_close = korea_time.replace(hour=15, minute=30, second=0, microsecond=0)
        
        is_open = market_open <= korea_time <= market_close
        
        logger.info(f"🕐 Korean time: {korea_time.strftime('%Y-%m-%d %H:%M:%S KST')}")
        logger.info(f"📊 KIS market status: {'🟢 OPEN' if is_open else '🔴 CLOSED'}")
        
        return is_open
        
    except Exception as e:
        logger.error(f"❌ Market hours check error: {e}")
        # If timezone check fails, allow trading (safer default)
        return True

# --- KIS API 함수들 ---
def _load_kis_token_from_file() -> Optional[str]:
    """파일에서 KIS 토큰 로드"""
    global kis_access_token, kis_token_issued_at
    
    token_file = 'kis_token_prod.json'
    if os.path.exists(token_file):
        try:
            with open(token_file, 'r') as f:
                token_data = json.load(f)
                access_token = token_data.get('access_token')
                expires_at = token_data.get('expires_at')
                
                if access_token and expires_at:
                    expiry_time = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    if datetime.now() < expiry_time:
                        kis_access_token = access_token
                        kis_token_issued_at = datetime.now()
                        logger.info("✅ KIS 토큰을 파일에서 로드했습니다")
                        return access_token
                    else:
                        logger.info("⏰ KIS 토큰이 만료되었습니다")
        except Exception as e:
            logger.warning(f"⚠️ KIS 토큰 파일 로드 실패: {e}")
    
    return None

def _save_kis_token_to_file(token: str):
    """KIS 토큰을 파일에 저장"""
    token_file = 'kis_token_prod.json'
    expires_at = (datetime.now() + timedelta(hours=24)).isoformat()
    token_data = {
        'access_token': token,
        'expires_at': expires_at
    }
    try:
        with open(token_file, 'w') as f:
            json.dump(token_data, f)
        logger.info("💾 KIS 토큰을 파일에 저장했습니다")
    except Exception as e:
        logger.warning(f"⚠️ KIS 토큰 저장 실패: {e}")

def get_kis_access_token() -> Optional[str]:
    """KIS API 액세스 토큰 획득"""
    global kis_access_token, kis_token_issued_at
    
    # 기존 토큰이 유효하면 사용
    if kis_access_token and kis_token_issued_at:
        if (datetime.now() - kis_token_issued_at).total_seconds() < 86400 - 600:  # 24시간 - 10분 마진
            return kis_access_token
    
    # 파일에서 토큰 로드 시도
    if _load_kis_token_from_file():
        return kis_access_token
    
    # 새 토큰 요청
    if not all([KIS_APPKEY, KIS_APPSECRET]):
        logger.error("❌ KIS API 키가 설정되지 않았습니다")
        return None
    
    url = f"{KIS_BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET
    }
    headers = {"content-type": "application/json; charset=utf-8"}
    
    try:
        res = requests.post(url, data=json.dumps(body), headers=headers, timeout=10)
        if res.status_code == 403:
            logger.warning("⚠️ KIS API 토큰 요청 제한 (시간 외 또는 제한)")
            return kis_access_token  # 기존 토큰 반환
        
        res.raise_for_status()
        token_data = res.json()
        
        if 'access_token' in token_data:
            kis_access_token = token_data['access_token']
            kis_token_issued_at = datetime.now()
            _save_kis_token_to_file(kis_access_token)
            logger.info("✅ 새 KIS 액세스 토큰을 획득했습니다")
            return kis_access_token
        else:
            logger.error(f"❌ KIS 토큰 응답에 액세스 토큰이 없습니다: {token_data}")
            
    except Exception as e:
        logger.error(f"❌ KIS 토큰 획득 오류: {e}")
    
    return None

def _generate_kis_hashkey(data: Dict) -> Optional[str]:
    """KIS API 해시키 생성"""
    url = f"{KIS_BASE_URL}/uapi/hashkey"
    headers = {
        "content-type": "application/json",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "User-Agent": "Mozilla/5.0"
    }
    
    try:
        res = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        res.raise_for_status()
        hashkey_data = res.json()
        
        if 'HASH' in hashkey_data:
            return hashkey_data['HASH']
        else:
            logger.error(f"❌ 해시키 생성 실패: {hashkey_data}")
            return None
    except Exception as e:
        logger.error(f"❌ 해시키 생성 오류: {e}")
        return None

def get_kis_account_balance() -> Optional[Dict]:
    """KIS 계좌 잔고 조회"""
    token = get_kis_access_token()
    if not token:
        return None
    
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/inquire-balance"
    
    params = {
        "CANO": KIS_ACCOUNT_PREFIX,
        "ACNT_PRDT_CD": KIS_ACCOUNT_SUFFIX,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "00",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": ""
    }
    
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": "TTTC8434R",
        "custtype": "P"
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        
        if data.get('rt_cd') == '0':
            return data
        else:
            logger.error(f"❌ KIS 잔고 조회 실패: {data.get('msg1')}")
            return None
    except Exception as e:
        logger.error(f"❌ KIS 잔고 조회 오류: {e}")
        return None

def get_kis_available_cash() -> float:
    """KIS 사용 가능 현금 조회"""
    balance_data = get_kis_account_balance()
    if not balance_data or not balance_data.get('output2'):
        return 0.0
    
    output2 = balance_data['output2'][0]
    available_cash = float(output2.get('prvs_rcdl_excc_amt', 0))
    logger.info(f"💰 KIS 사용 가능 현금: {available_cash:,.0f}원")
    return available_cash

def get_kis_current_position(ticker: str) -> float:
    """KIS 특정 종목 보유 수량 조회"""
    balance_data = get_kis_account_balance()
    if not balance_data:
        return 0.0
    
    for item in balance_data.get('output1', []):
        if item.get('pdno') == ticker:
            quantity = float(item.get('hldg_qty', 0))
            return quantity
    
    return 0.0

def get_kis_stock_price(ticker: str) -> Optional[Dict]:
    """KIS 주식 현재가 조회"""
    token = get_kis_access_token()
    if not token:
        return None
    
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
    
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker
    }
    
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": "FHKST01010100",
        "custtype": "P"
    }
    
    try:
        res = requests.get(url, headers=headers, params=params, timeout=5)
        res.raise_for_status()
        data = res.json()
        
        if data.get('rt_cd') == '0':
            return data.get('output', {})
        else:
            logger.error(f"❌ KIS 주가 조회 실패 {ticker}: {data.get('msg1')}")
            return None
    except Exception as e:
        logger.error(f"❌ KIS 주가 조회 오류 {ticker}: {e}")
        return None

def place_kis_order(ticker: str, side: str, quantity: int) -> Optional[Dict]:
    """KIS 주식 주문"""
    token = get_kis_access_token()
    if not token:
        return None
    
    # 주문 타입 결정
    if side.lower() == 'sell':
        tr_id = "TTTC0801U"  # 매도
    else:
        tr_id = "TTTC0802U"  # 매수
    
    # 현재가 조회 (시장가 주문을 위해)
    price_info = get_kis_stock_price(ticker)
    if not price_info:
        logger.error(f"❌ 주가 정보 조회 실패: {ticker}")
        return None
    
    current_price = int(price_info.get('stck_prpr', '0'))
    if current_price == 0:
        logger.error(f"❌ 유효하지 않은 주가: {ticker}")
        return None
    
    url = f"{KIS_BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"
    
    request_body = {
        "CANO": KIS_ACCOUNT_PREFIX,
        "ACNT_PRDT_CD": KIS_ACCOUNT_SUFFIX,
        "PDNO": ticker,
        "ORD_DVSN": "01",  # 01: 시장가
        "ORD_QTY": str(quantity),
        "ORD_UNPR": "0",  # 시장가는 0
    }
    
    # 해시키 생성
    hashkey = _generate_kis_hashkey(request_body)
    if not hashkey:
        return None
    
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": KIS_APPKEY,
        "appsecret": KIS_APPSECRET,
        "tr_id": tr_id,
        "custtype": "P",
        "hashkey": hashkey
    }
    
    try:
        res = requests.post(url, headers=headers, data=json.dumps(request_body), timeout=10)
        order_result = res.json()
        
        if order_result.get('rt_cd') == '0':
            order_no = order_result.get('output', {}).get('ODNO', 'N/A')
            logger.info(f"✅ KIS {side.upper()} 주문 완료: {ticker}, 수량: {quantity}, 주문번호: {order_no}")
            return order_result
        else:
            logger.error(f"❌ KIS {side.upper()} 주문 실패: {ticker}, 오류: {order_result.get('msg1')}")
            return None
    except Exception as e:
        logger.error(f"❌ KIS 주문 처리 오류 {ticker}: {e}")
        return None

# --- 헬스체크 엔드포인트 ---
@app.get("/")
async def root():
    return {
        "message": "TradingView to Upbit Webhook Server",
        "status": "running",
        "upbit_connected": upbit is not None
    }

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    try:
        health_status = {"status": "healthy"}
        
        # Upbit 연결 상태 확인
        if upbit:
            try:
                balances = upbit.get_balances()
                health_status["upbit"] = {
                    "connected": True,
                    "balance_count": len(balances) if balances else 0
                }
            except Exception as e:
                health_status["upbit"] = {
                    "connected": False,
                    "error": str(e)
                }
                health_status["status"] = "warning"
        else:
            health_status["upbit"] = {
                "connected": False,
                "message": "API 키가 설정되지 않았습니다."
            }
        
        # KIS 연결 상태 확인
        if all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
            try:
                token = get_kis_access_token()
                if token:
                    health_status["kis"] = {
                        "connected": True,
                        "token_available": True
                    }
                else:
                    health_status["kis"] = {
                        "connected": False,
                        "token_available": False
                    }
                    health_status["status"] = "warning"
            except Exception as e:
                health_status["kis"] = {
                    "connected": False,
                    "error": str(e)
                }
                health_status["status"] = "warning"
        else:
            health_status["kis"] = {
                "connected": False,
                "message": "API 키가 설정되지 않았습니다."
            }
        
        return health_status
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

# --- 웹훅 엔드포인트 ---
@app.post("/webhook")
async def tradingview_webhook(request: Request):
    """
    TradingView 웹훅을 받아 Upbit 주문을 처리하는 엔드포인트
    
    기본 형식:
    {
        "symbol": "KRW-BTC",
        "side": "buy",
        "quantity": "10000",
        "passphrase": "YOUR_SECRET_PASSPHRASE"
    }
    
    고급 알림 형식:
    {
        "alert_name": "signal_buy" or "signal_exit",
        "symbol": "KRW-BTC",
        "passphrase": "YOUR_SECRET_PASSPHRASE"
    }
    """
    try:
        data = await request.json()
        logger.info(f"웹훅 요청 수신: {data}")
        
        # 패스프레이즈 검증
        if data.get("passphrase") != MY_SECRET_PASSPHRASE:
            client_host = getattr(request.client, 'host', 'unknown') if request.client else 'unknown'
            logger.warning(f"잘못된 패스프레이즈 시도: {client_host}")
            raise HTTPException(status_code=401, detail="Invalid passphrase")
        
        # 알림 이름 기반 고급 거래 로직
        alert_name = data.get("alert_name", "").lower()
        symbol = data.get("symbol")
        # 전략/인터벌(옵션): 다양한 키 지원
        strategy_name = (
            data.get("strategy")
            or data.get("condition")
            or data.get("Strategy")
            or "Kelly"
        )
        interval_name = (
            data.get("interval")
            or data.get("timeframe")
            or data.get("tf")
            or ""
        )
        
        if not symbol:
            raise HTTPException(status_code=400, detail="Missing symbol")
        
        # 심볼 타입 감지
        symbol_type = detect_symbol_type(symbol)
        if symbol_type == "unknown":
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported symbol format: {symbol}. Expected: KRW-BTC (crypto) or 005930 (stock)"
            )
        
        logger.info(f"🔍 심볼 타입 감지: {symbol} → {symbol_type}")
        
        # 유연한 매핑: *_buy 포함 시 매수로 간주
        if alert_name == "signal_buy" or (alert_name and "buy" in alert_name):
            # 매수 신호 로직 (모든 전략 호환)
            logger.info(f"🚀 Buy Signal 수신: {symbol} ({symbol_type})")
            # 1차 기록: Placed
            page_id = _create_notion_trade_page(
                title=f"{symbol} BUY",
                timestamp=_now_in_tz(),
                asset=symbol,
                status="Placed",
                position="Long",
                strategy=strategy_name,
                interval=interval_name,
                entry_price=None,
                exit_price=None,
                quantity=None,
                fee=None,
                order_id="",
                webhook_json=data,
            )
            
            if symbol_type == "crypto":
                # === 크립토 매수 로직 ===
                if not upbit:
                    raise HTTPException(status_code=500, detail="Upbit 클라이언트가 초기화되지 않았습니다")
                
                # 현재 포지션 확인
                current_position = get_current_position(symbol)
                if current_position > 0 and not ALLOW_DUPLICATE_BUY:
                    logger.info(f"⚠️ 기존 크립토 포지션 존재 ({current_position:.8f}), 매수 스킵")
                    try:
                        _update_notion_trade_page(page_id or "", status="Skipped", position="Long")
                    except Exception:
                        pass
                    return {
                        "status": "skipped",
                        "reason": "existing_position",
                        "symbol": symbol,
                        "exchange": "upbit",
                        "current_position": current_position
                    }
                
                # Available KRW 조회
                available_krw = get_current_balance("KRW")
                if available_krw < 5000:  # 최소 거래 금액
                    try:
                        _update_notion_trade_page(page_id or "", status="Error")
                    except Exception:
                        pass
                    raise HTTPException(status_code=400, detail=f"Insufficient Upbit KRW balance: {available_krw}")
                
                # 동적 Kelly Fraction 계산
                logger.info(f"📊 Kelly Fraction 계산 시작 (Upbit 잔고: {available_krw:,.0f}원)")
                kelly_amount, kelly_stats = calculate_dynamic_kelly_fraction(symbol, available_krw)
                
                logger.info(f"💰 최적 Kelly 매수: {kelly_amount:,.0f}원")

                # 매수 실행
                approx_entry = get_upbit_last_price(symbol)
                approx_qty = float(kelly_amount) / approx_entry if (approx_entry and approx_entry > 0) else None
                trade_details = place_upbit_order(symbol, "buy", kelly_amount, "market")

                # Notion 기록 (성공 시)
                try:
                    order_id = None
                    if isinstance(trade_details, dict):
                        order_id = trade_details.get('uuid') or trade_details.get('id') or trade_details.get('order_id')
                    _update_notion_trade_page(
                        page_id or "",
                        status="Filled",
                        position="Long",
                        strategy=strategy_name,
                        interval=interval_name,
                        entry_price=approx_entry,
                        exit_price=None,
                        quantity=float(approx_qty) if approx_qty is not None else float(kelly_amount),
                        fee=None,
                        order_id=str(order_id or ""),
                    )
                except Exception:
                    pass

                return {
                    "status": "success",
                    "strategy": "signal_buy",
                    "symbol": symbol,
                    "exchange": "upbit",
                    "side": "buy",
                    "quantity": kelly_amount,
                    "kelly_stats": kelly_stats,
                    "details": trade_details
                }
                
            elif symbol_type == "stock":
                # === 주식 매수 로직 ===
                if not all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
                    raise HTTPException(status_code=500, detail="KIS API 설정이 완료되지 않았습니다")
                
                # Check if market is open
                if not is_kis_market_open():
                    try:
                        _update_notion_trade_page(page_id or "", status="Skipped")
                    except Exception:
                        pass
                    return {
                        "status": "skipped",
                        "reason": "market_closed",
                        "symbol": symbol,
                        "exchange": "kis",
                        "message": "Korean stock market is closed. Trading hours: 09:00-15:30 KST, Mon-Fri"
                    }
                
                # 현재 포지션 확인
                current_position = get_kis_current_position(symbol)
                if current_position > 0 and not ALLOW_DUPLICATE_BUY:
                    logger.info(f"⚠️ 기존 주식 포지션 존재 ({current_position}주), 매수 스킵")
                    try:
                        _update_notion_trade_page(page_id or "", status="Skipped", position="Long")
                    except Exception:
                        pass
                    return {
                        "status": "skipped",
                        "reason": "existing_position",
                        "symbol": symbol,
                        "exchange": "kis",
                        "current_position": current_position
                    }
                
                # Available KRW 조회
                available_krw = get_kis_available_cash()
                if available_krw < 10000:  # 주식 최소 거래 금액
                    try:
                        _update_notion_trade_page(page_id or "", status="Error")
                    except Exception:
                        pass
                    raise HTTPException(status_code=400, detail=f"Insufficient KIS KRW balance: {available_krw}")
                
                # Kelly Fraction 계산 (개별 주식 변동성 사용)
                logger.info(f"📊 Kelly Fraction 계산 시작 (KIS 잔고: {available_krw:,.0f}원)")
                # 개별 주식의 변동성을 yfinance에서 가져와서 사용
                kelly_amount, kelly_stats = calculate_dynamic_kelly_fraction(symbol, available_krw)
                
                # 주식 현재가 조회 및 매수 수량 계산
                price_info = get_kis_stock_price(symbol)
                if not price_info:
                    raise HTTPException(status_code=500, detail=f"주가 정보를 조회할 수 없습니다: {symbol}")
                
                current_price = int(price_info.get('stck_prpr', '0'))
                if current_price == 0:
                    raise HTTPException(status_code=500, detail=f"유효하지 않은 주가: {symbol}")
                
                # 매수 수량 계산 (정수로)
                max_quantity = int(kelly_amount // current_price)
                if max_quantity == 0:
                    raise HTTPException(status_code=400, detail=f"매수 금액이 부족합니다. 현재가: {current_price:,}원, 할당액: {kelly_amount:,.0f}원")
                
                logger.info(f"💰 주식 매수: {max_quantity}주 x {current_price:,}원 = {max_quantity * current_price:,}원")
                
                # 매수 실행
                trade_details = place_kis_order(symbol, "buy", max_quantity)
                if not trade_details:
                    raise HTTPException(status_code=500, detail=f"KIS 매수 주문 실패: {symbol}")
                
                # Notion 기록 (성공 시)
                try:
                    order_id = None
                    if isinstance(trade_details, dict):
                        output = trade_details.get('output', {})
                        order_id = output.get('ODNO') or trade_details.get('id')
                    _update_notion_trade_page(
                        page_id or "",
                        status="Filled",
                        position="Long",
                        strategy=strategy_name,
                        interval=interval_name,
                        entry_price=float(current_price),
                        exit_price=None,
                        quantity=float(max_quantity),
                        fee=None,
                        order_id=str(order_id or ""),
                    )
                except Exception:
                    pass

                return {
                    "status": "success",
                    "strategy": "signal_buy",
                    "symbol": symbol,
                    "exchange": "kis",
                    "side": "buy",
                    "quantity": max_quantity,
                    "price": current_price,
                    "total_amount": max_quantity * current_price,
                    "kelly_stats": kelly_stats,
                    "details": trade_details
                }
            
        elif alert_name == "signal_exit" or (alert_name and ("exit" in alert_name or "sell" in alert_name)):
            # 매도 신호 로직 (모든 전략 호환)
            logger.info(f"📤 Exit Signal 수신: {symbol} ({symbol_type})")
            # 1차 기록: Placed
            page_id = _create_notion_trade_page(
                title=f"{symbol} SELL",
                timestamp=_now_in_tz(),
                asset=symbol,
                status="Placed",
                position="Exit",
                strategy=strategy_name,
                interval=interval_name,
                entry_price=None,
                exit_price=None,
                quantity=None,
                fee=None,
                order_id="",
                webhook_json=data,
            )
            
            if symbol_type == "crypto":
                # === 크립토 매도 로직 ===
                if not upbit:
                    raise HTTPException(status_code=500, detail="Upbit 클라이언트가 초기화되지 않았습니다")
                
                # 현재 포지션 확인
                current_position = get_current_position(symbol)
                if current_position <= 0:
                    logger.info(f"⚠️ 매도할 크립토 포지션 없음")
                    return {
                        "status": "skipped",
                        "reason": "no_position",
                        "symbol": symbol,
                        "exchange": "upbit",
                        "current_position": current_position
                    }
                
                logger.info(f"💸 전량 매도: {current_position:.8f} {symbol.split('-')[1]}")
                
                # 전량 매도 실행
                approx_exit = get_upbit_last_price(symbol)
                trade_details = place_upbit_order(symbol, "sell", current_position, "market")

                # Notion 기록 (성공 시)
                try:
                    order_id = None
                    if isinstance(trade_details, dict):
                        order_id = trade_details.get('uuid') or trade_details.get('id') or trade_details.get('order_id')
                    _update_notion_trade_page(
                        page_id or "",
                        status="Filled",
                        position="Exit",
                        strategy=strategy_name,
                        interval=interval_name,
                        entry_price=None,
                        exit_price=approx_exit,
                        quantity=float(current_position),
                        fee=None,
                        order_id=str(order_id or ""),
                    )
                except Exception:
                    pass

                return {
                    "status": "success",
                    "strategy": "signal_exit",
                    "symbol": symbol,
                    "exchange": "upbit",
                    "side": "sell",
                    "quantity": current_position,
                    "details": trade_details
                }
                
            elif symbol_type == "stock":
                # === 주식 매도 로직 ===
                if not all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
                    raise HTTPException(status_code=500, detail="KIS API 설정이 완료되지 않았습니다")
                
                # Check if market is open
                if not is_kis_market_open():
                    try:
                        _update_notion_trade_page(page_id or "", status="Skipped")
                    except Exception:
                        pass
                    return {
                        "status": "skipped",
                        "reason": "market_closed", 
                        "symbol": symbol,
                        "exchange": "kis",
                        "message": "Korean stock market is closed. Trading hours: 09:00-15:30 KST, Mon-Fri"
                    }
                
                # 현재 포지션 확인
                current_position = get_kis_current_position(symbol)
                if current_position <= 0:
                    logger.info(f"⚠️ 매도할 주식 포지션 없음")
                    try:
                        _update_notion_trade_page(page_id or "", status="Skipped", position="Exit")
                    except Exception:
                        pass
                    return {
                        "status": "skipped",
                        "reason": "no_position",
                        "symbol": symbol,
                        "exchange": "kis",
                        "current_position": current_position
                    }
                
                logger.info(f"💸 주식 전량 매도: {current_position}주")
                
                # 전량 매도 실행
                trade_details = place_kis_order(symbol, "sell", int(current_position))
                if not trade_details:
                    raise HTTPException(status_code=500, detail=f"KIS 매도 주문 실패: {symbol}")
                
                # Notion 기록 (성공 시)
                try:
                    order_id = None
                    if isinstance(trade_details, dict):
                        output = trade_details.get('output', {})
                        order_id = output.get('ODNO') or trade_details.get('id')
                    _update_notion_trade_page(
                        page_id or "",
                        status="Filled",
                        position="Exit",
                        strategy="Kelly",
                        interval="",
                        entry_price=None,
                        exit_price=None,
                        quantity=float(current_position),
                        fee=None,
                        order_id=str(order_id or ""),
                    )
                except Exception:
                    pass

                return {
                    "status": "success",
                    "strategy": "signal_exit",
                    "symbol": symbol,
                    "exchange": "kis",
                    "side": "sell",
                    "quantity": int(current_position),
                    "details": trade_details
                }
            
        else:
            # 기존 수동 거래 로직 (호환성 유지)
            required_fields = ["symbol", "side", "quantity"]
            for field in required_fields:
                if field not in data:
                    raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
            
            side = data.get("side")
            quantity = float(data.get("quantity"))
            
            if quantity <= 0:
                raise HTTPException(status_code=400, detail="Quantity must be positive")
            
            if symbol_type == "crypto":
                # 크립토 수동 거래
                if not upbit:
                    raise HTTPException(status_code=500, detail="Upbit 클라이언트가 초기화되지 않았습니다")
                
                trade_details = place_upbit_order(symbol, side, quantity, "market")
                
                return {
                    "status": "success",
                    "exchange": "upbit",
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity,
                    "details": trade_details
                }
                
            elif symbol_type == "stock":
                # 주식 수동 거래
                if not all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
                    raise HTTPException(status_code=500, detail="KIS API 설정이 완료되지 않았습니다")
                
                # Check if market is open
                if not is_kis_market_open():
                    return {
                        "status": "skipped",
                        "reason": "market_closed",
                        "symbol": symbol,
                        "exchange": "kis", 
                        "message": "Korean stock market is closed. Trading hours: 09:00-15:30 KST, Mon-Fri"
                    }
                
                # 주식은 정수 수량만 허용
                quantity_int = int(quantity)
                if quantity_int <= 0:
                    raise HTTPException(status_code=400, detail="Stock quantity must be positive integer")
                
                trade_details = place_kis_order(symbol, side, quantity_int)
                if not trade_details:
                    raise HTTPException(status_code=500, detail=f"KIS 주문 실패: {symbol}")
                
                return {
                    "status": "success",
                    "exchange": "kis",
                    "symbol": symbol,
                    "side": side,
                    "quantity": quantity_int,
                    "details": trade_details
                }
    
    except HTTPException:
        # HTTPException은 FastAPI에서 자동 처리되므로 그대로 다시 발생
        # Notion에 에러 상태 반영 (가능한 경우)
        try:
            # 최근에 생성한 page_id가 로컬 스코프에 있을 수 있으므로 best-effort로 처리
            if 'page_id' in locals() and page_id:
                _update_notion_trade_page(page_id, status="Error")
        except Exception:
            pass
        raise
    
    except ValueError as e:
        logger.error(f"데이터 형식 오류: {e}")
        try:
            if 'page_id' in locals() and page_id:
                _update_notion_trade_page(page_id, status="Error")
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Invalid data format: {str(e)}")
    
    except Exception as e:
        logger.error(f"예상치 못한 오류: {e}")
        try:
            if 'page_id' in locals() and page_id:
                _update_notion_trade_page(page_id, status="Error")
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# --- 잔고 조회 엔드포인트 (디버깅용) ---
@app.get("/balances")
async def get_balances():
    """현재 잔고 조회 (디버깅용) - 모든 거래소"""
    result = {}
    
    # Upbit 잔고 조회
    if upbit:
        try:
            upbit_balances = upbit.get_balances()
            result["upbit"] = {
                "status": "success",
                "balances": upbit_balances
            }
        except Exception as e:
            result["upbit"] = {
                "status": "error",
                "error": str(e)
            }
    else:
        result["upbit"] = {
            "status": "not_configured",
            "message": "Upbit API 키가 설정되지 않았습니다."
        }
    
    # KIS 잔고 조회
    if all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
        try:
            kis_balance = get_kis_account_balance()
            if kis_balance:
                # 주요 정보만 추출
                output2 = kis_balance.get('output2', [{}])[0]
                positions = []
                for item in kis_balance.get('output1', []):
                    if float(item.get('hldg_qty', 0)) > 0:
                        positions.append({
                            "stock_code": item.get('pdno', ''),
                            "stock_name": item.get('prdt_name', ''),
                            "quantity": float(item.get('hldg_qty', 0)),
                            "avg_price": float(item.get('pchs_avg_pric', 0)),
                            "current_price": float(item.get('prpr', 0)),
                            "market_value": float(item.get('evlu_amt', 0)),
                            "profit_loss": float(item.get('evlu_pfls_amt', 0))
                        })
                
                result["kis"] = {
                    "status": "success",
                    "available_cash": float(output2.get('prvs_rcdl_excc_amt', 0)),
                    "total_asset_value": float(output2.get('tot_evlu_amt', 0)),
                    "positions": positions
                }
            else:
                result["kis"] = {
                    "status": "error",
                    "error": "Failed to retrieve KIS balance"
                }
        except Exception as e:
            result["kis"] = {
                "status": "error", 
                "error": str(e)
            }
    else:
        result["kis"] = {
            "status": "not_configured",
            "message": "KIS API 키가 설정되지 않았습니다."
        }
    
    return result

# 서버 실행
if __name__ == "__main__":
    logger.info("="*60)
    logger.info("🚀 TradingView Multi-Exchange Webhook 서버 시작")
    logger.info("="*60)
    
    # API 키 상태 체크
    if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
        logger.warning("⚠️ Upbit API 키가 설정되지 않았습니다.")
        logger.info("   환경변수 UPBIT_ACCESS_KEY와 UPBIT_SECRET_KEY를 설정해주세요.")
    else:
        logger.info("✅ Upbit API 키 확인됨")
    
    if not all([KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX]):
        logger.warning("⚠️ KIS API 키가 설정되지 않았습니다.")
        logger.info("   환경변수 KIS_APPKEY, KIS_APPSECRET, KIS_ACCOUNT_PREFIX, KIS_ACCOUNT_SUFFIX를 설정해주세요.")
    else:
        logger.info("✅ KIS API 키 확인됨")
    
    logger.info("📊 지원되는 심볼 형식:")
    logger.info("   • 크립토: KRW-BTC, KRW-ETH, BTC-ETH (Upbit)")
    logger.info("   • 주식: 005930, 000660, 035420 (KIS)")
    logger.info("📡 범용 알림 이름:")
    logger.info("   • signal_buy: 매수 신호 (모든 전략 호환)")
    logger.info("   • signal_exit: 매도 신호 (모든 전략 호환)")
    display_port = int(os.getenv('PORT', 8000))
    logger.info(f"📡 웹훅 엔드포인트: http://localhost:{display_port}/webhook")
    logger.info("="*60)
    
    port = int(os.getenv('PORT', 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)