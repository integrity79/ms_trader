# main.py
import os
import json
import builtins
import sys
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from dotenv import load_dotenv
load_dotenv()

# 모든 print() 호출에 시각을 자동으로 붙이고, 화면에 찍히는 것과 동일한 내용을 날짜별
# 로그 파일에도 남긴다. 이 모듈을 가장 먼저 import하는 진입점(trader_main.py, test_daemon.py)을
# 통해 trader_short/trader_mid/realtime_quotes의 로그에도 공통 적용된다. systemd 등으로
# 백그라운드 서비스로 돌릴 때 화면 출력만으로는 나중에 확인할 방법이 없어서 파일로도 남긴다.
_original_print = builtins.print
LOG_DIR = os.getenv("LOG_DIR", "logs")
_log_lock = threading.Lock()
_log_file = None
_log_file_date = ""


def _get_log_file():
    """오늘 날짜 로그 파일 핸들을 반환한다. 날짜가 바뀌면 자동으로 새 파일로 교체해서
    데몬이 여러 날 연속으로 떠 있어도 하루 단위로 로그가 쌓인다."""
    global _log_file, _log_file_date
    today_str = datetime.now().strftime("%Y%m%d")
    if _log_file is not None and _log_file_date == today_str:
        return _log_file
    if _log_file is not None:
        try:
            _log_file.close()
        except Exception:
            pass
        _log_file = None
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        _log_file = open(os.path.join(LOG_DIR, f"trader_{today_str}.log"), "a", encoding="utf-8")
    except Exception as e:
        _original_print(f"⚠️ [로그 파일] 열기 실패: {e}")
    _log_file_date = today_str  # 실패했어도 같은 날 매번 재시도하며 화면 출력을 늦추지 않도록 기록해둔다.
    return _log_file


def _timestamped_print(*args, **kwargs):
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    line = f"[{timestamp}]"
    _original_print(line, *args, **kwargs)

    # 로그 파일 쓰기가 실패하거나 느려져도 매매 로직에는 절대 영향을 주면 안 되므로 예외를 삼킨다.
    if kwargs.get("file") not in (None, sys.stdout):
        return
    try:
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        text = sep.join([line, *(str(a) for a in args)]) if args else line
        with _log_lock:
            log_file = _get_log_file()
            if log_file is not None:
                log_file.write(text + end)
                log_file.flush()
    except Exception:
        pass


builtins.print = _timestamped_print

# pykrx(네이버 금융 스크래핑)가 내부적으로 쓰는 requests 호출에는 timeout이 지정되어 있지 않아,
# 상대 서버가 응답을 주지 않으면 소켓 레벨에서 영원히 블로킹된다. requests.Session.request에
# 기본 타임아웃을 강제로 주입해 그런 호출도 반드시 실패로 끝나도록 한다. 우리 자체 키움/텔레그램
# API 호출은 이미 전부 timeout=을 명시하고 있으므로 이 기본값의 영향을 받지 않는다.
_DEFAULT_HTTP_TIMEOUT_SECONDS = max(float(os.getenv("HTTP_REQUEST_TIMEOUT_SECONDS", "10")), 1.0)
_original_session_request = requests.Session.request


def _session_request_with_default_timeout(self, method, url, **kwargs):
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = _DEFAULT_HTTP_TIMEOUT_SECONDS
    return _original_session_request(self, method, url, **kwargs)


requests.Session.request = _session_request_with_default_timeout

from trader_short import ShortTermTrader
from trader_mid import MidTermTrader
from state_store import atomic_write_json
from realtime_quotes import KiwoomQuoteStream


RUN_STATE_FILE = "trader_daemon_state.json"
ORDER_GUARD_FILE = "trader_order_guard.json"
PENDING_ORDERS_FILE = "trader_pending_orders.json"


class KiwoomSharedClient:
    """공통 키움 REST API 클라이언트 (단일 토큰 및 Rate Limit 관리)"""
    def __init__(self, app_key: str, secret_key: str, account_no: str, is_mock: bool = True):
        self.app_key = app_key
        self.secret_key = secret_key
        self.account_no = account_no.replace("-", "").strip()
        self.is_mock = is_mock
        self.base_url = "https://mockapi.kiwoom.com" if is_mock else "https://api.kiwoom.com"
        self.access_token = None
        self.token_expiry = None
        self.order_preflight = None
        self.notifier = None  # bootstrap_daemon()에서 채워짐. 심각한 오류 발생 시 텔레그램 알림용.
        self.quote_request_interval = max(float(os.getenv("QUOTE_REQUEST_INTERVAL", "0.35")), 0.0)
        self.quote_cache_ttl_seconds = max(float(os.getenv("QUOTE_CACHE_TTL_SECONDS", "2")), 0.0)
        self.quote_retry_count = max(int(os.getenv("QUOTE_RETRY_COUNT", "3")), 0)
        self.quote_retry_backoff_seconds = max(float(os.getenv("QUOTE_RETRY_BACKOFF_SECONDS", "1")), 0.0)
        self._last_quote_request_at = 0.0
        self._quote_cache = {}
        self._quote_cache_lock = threading.Lock()
        self.quote_stream_max_age_seconds = max(float(os.getenv("QUOTE_STREAM_MAX_AGE_SECONDS", "10")), 0.0)
        self._quote_stream = None
        self.order_confirm_timeout = max(float(os.getenv("ORDER_CONFIRM_TIMEOUT_SECONDS", "15")), 0.0)
        self.order_confirm_poll_interval = max(float(os.getenv("ORDER_CONFIRM_POLL_SECONDS", "1.5")), 0.1)
        self.max_daily_order_count = max(int(os.getenv("MAX_DAILY_ORDER_COUNT", "20")), 0)
        self.max_cash_usage_rate = min(max(float(os.getenv("MAX_CASH_USAGE_RATE", "0.95")), 0.0), 1.0)
        self.max_ticker_exposure_rate = min(max(float(os.getenv("MAX_TICKER_EXPOSURE_RATE", "0.15")), 0.0), 1.0)
        self.max_single_order_value = max(float(os.getenv("MAX_SINGLE_ORDER_VALUE", "0")), 0.0)
        self.max_daily_buy_value = max(float(os.getenv("MAX_DAILY_BUY_VALUE", "0")), 0.0)
        self.max_daily_loss_rate = max(float(os.getenv("MAX_DAILY_LOSS_RATE", "0")), 0.0)
        self.max_daily_loss_value = max(float(os.getenv("MAX_DAILY_LOSS_VALUE", "0")), 0.0)
        self._daily_order_date = ""
        self._daily_order_count = 0
        self._daily_buy_value = 0.0
        self._daily_start_equity = 0.0
        self._order_guard_error = None
        self._pending_order_error = None
        self._pending_orders = []
        try:
            self._daily_order_date, self._daily_order_count, self._daily_buy_value, self._daily_start_equity = self._load_order_guard()
        except RuntimeError as e:
            self._order_guard_error = e
        try:
            self._pending_orders = self._load_pending_orders()
        except RuntimeError as e:
            self._pending_order_error = e

    def _notify_error(self, key: str, text: str, cooldown_seconds: float = None) -> None:
        """notifier가 연결돼 있으면 심각한 오류를 텔레그램으로 보고한다(같은 key는 쿨다운 적용).
        notifier가 아직 없거나(부트스트랩 이전) 전송 자체가 실패해도 매매 로직에는 영향 없다."""
        if self.notifier is None:
            return
        try:
            self.notifier.send_error(key, text, cooldown_seconds=cooldown_seconds)
        except Exception:
            pass

    def get_token(self) -> str:
        now = datetime.now()
        if self.access_token and self.token_expiry and now < (self.token_expiry - timedelta(minutes=10)):
            return self.access_token

        url = f"{self.base_url}/oauth2/token"
        headers = {"Content-Type": "application/json;charset=UTF-8"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "secretkey": self.secret_key
        }
        try:
            res = requests.post(url, headers=headers, json=data, timeout=10)
            res.raise_for_status()
            res_json = res.json()
            if res_json.get("token"):
                self.access_token = res_json["token"]
                self.token_expiry = now + timedelta(hours=23)
                print(f"[키움 REST 마스터] 토큰 갱신 완료", flush=True)
                return self.access_token
            print(f"❌ [토큰 발급 거절] {res_json.get('return_msg', res_json)}", flush=True)
            self._notify_error("token_issue", f"🚨 [토큰 발급 거절] {res_json.get('return_msg', res_json)}")
        except Exception as e:
            print(f"❌ [토큰 발급 오류] {e}", flush=True)
            self._notify_error("token_issue", f"🚨 [토큰 발급 오류] {e}")
        return None

    def get_headers(self, api_id: str) -> dict:
        token = self.get_token()
        if not token:
            raise RuntimeError("유효한 키움 접근 토큰을 발급하지 못했습니다.")
        return {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}",
            "cont-yn": "N",
            "next-key": "",
            "api-id": api_id
        }

    def _post_with_rate_limit(self, url: str, api_id: str, body: dict) -> dict:
        """키움 REST 호출 공통 처리: 모든 API가 같은 호출 빈도 제한을 공유하므로 간격 준수 및 429 재시도를 함께 적용한다."""
        for attempt in range(self.quote_retry_count + 1):
            elapsed = time.monotonic() - self._last_quote_request_at
            if elapsed < self.quote_request_interval:
                time.sleep(self.quote_request_interval - elapsed)
            self._last_quote_request_at = time.monotonic()
            res = requests.post(url, headers=self.get_headers(api_id), json=body, timeout=10)
            if res.status_code == 429:
                if attempt < self.quote_retry_count:
                    wait_seconds = self.quote_retry_backoff_seconds * (2 ** attempt)
                    print(f"⚠️ [API 호출 제한] {api_id}: {wait_seconds:.1f}초 후 재시도", flush=True)
                    time.sleep(wait_seconds)
                    continue
                raise RuntimeError("키움 API 호출 제한(429)에 도달했습니다.")
            res.raise_for_status()
            return res.json()
        raise RuntimeError("키움 API 호출 제한(429)에 도달했습니다.")

    def _fetch_account_snapshot(self) -> dict:
        """잔고와 보유종목이 같은 kt00018 응답에 함께 담겨 오므로, 둘 다 필요한 호출부(_can_submit_order)에서
        API를 두 번 쏘지 않도록 원본 응답을 한 번만 가져와 공유한다."""
        url = f"{self.base_url}/api/dostk/acnt"
        body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}
        data = self._post_with_rate_limit(url, "kt00018", body)
        if data.get("return_code") not in (None, 0, "0"):
            raise RuntimeError(data.get("return_msg", "키움 계좌 조회가 거절되었습니다."))
        return data

    @staticmethod
    def _parse_balance_from_snapshot(data: dict) -> dict:
        def parse_amt(val) -> int:
            if val is None or val == "":
                return 0
            if isinstance(val, (int, float)): return int(val)
            clean = str(val).replace(",", "").replace("+", "").strip()
            return int(clean) if clean else 0

        tot = parse_amt(data.get("prsm_dpst_aset_amt", 0))
        stk = parse_amt(data.get("tot_evlt_amt", 0))
        return {"total_equity": float(tot), "deposit": float(tot - stk), "stock_eval": float(stk)}

    def _parse_holdings_from_snapshot(self, data: dict) -> dict:
        holdings = {}
        for item in data.get("acnt_evlt_remn_indv_tot", []):
            ticker = self._normalize_ticker(item.get("stk_cd", ""))
            quantity = self._parse_number(item.get("rmnd_qty", 0))
            if ticker and quantity > 0:
                holdings[ticker] = quantity
        return holdings

    def _parse_holdings_detail_from_snapshot(self, data: dict) -> dict:
        """수량뿐 아니라 매입가(pur_pric)까지 포함한 보유종목 상세. 지연 미체결 복구에 사용."""
        holdings = {}
        for item in data.get("acnt_evlt_remn_indv_tot", []):
            ticker = self._normalize_ticker(item.get("stk_cd", ""))
            if not ticker:
                continue
            holdings[ticker] = {
                "qty": self._parse_number(item.get("rmnd_qty", 0)),
                "avg_price": abs(self._parse_number(item.get("pur_pric", 0))),
            }
        return holdings

    def get_account_balance(self) -> dict:
        try:
            return self._parse_balance_from_snapshot(self._fetch_account_snapshot())
        except Exception as e:
            print(f"❌ [잔고조회 오류] {e}", flush=True)
            raise RuntimeError("잔고 조회 실패: 신규 주문을 중단합니다.") from e

    def get_account_holdings(self) -> dict:
        try:
            return self._parse_holdings_from_snapshot(self._fetch_account_snapshot())
        except Exception as e:
            print(f"❌ [보유종목 조회 오류] {e}", flush=True)
            raise RuntimeError("보유종목 조회 실패: 신규 주문을 중단합니다.") from e

    def get_current_quote(self, ticker: str) -> dict:
        if not ticker:
            raise ValueError("현재가 조회 종목코드가 비어 있습니다.")

        ticker = self._normalize_ticker(ticker)
        with self._quote_cache_lock:
            cached_quote = self._quote_cache.get(ticker)
        stream_connected = bool(self._quote_stream and self._quote_stream.is_connected())
        if stream_connected:
            # 실시간 체결 구독 중에는 "그 종목 자체가 계속 갱신되고 있는 한" 새 체결이 없다는 것이
            # "가격 불변"을 의미한다. 다만 소켓 연결 자체는 살아있어도 그 종목만 조용히 구독이
            # 끊기거나 서버 쪽 이슈로 체결이 안 들어올 수 있으므로, 일정 시간(quote_stream_max_age_seconds)
            # 갱신이 없으면 신뢰하지 않고 REST로 다시 확인한다.
            if cached_quote:
                age = time.monotonic() - cached_quote["received_at"]
                if age < self.quote_stream_max_age_seconds:
                    return cached_quote["quote"]
                print(f"ℹ️ [현재가 캐시 미스] {ticker}: 실시간 구독 중이나 {age:.0f}초간 갱신이 없어 서버에서 재조회합니다.", flush=True)
            elif ticker in self._quote_stream.tickers:
                # 구독은 되어 있는데(보유 종목 등) 아직 첫 체결이 안 들어온 경우.
                print(f"ℹ️ [현재가 캐시 미스] {ticker}: 실시간 구독 중이나 아직 첫 체결이 없어 서버에서 조회합니다.", flush=True)
            else:
                # 후보 스캔 대상처럼 애초에 실시간 구독 대상이 아닌 종목. 보유 종목만 상시
                # 구독하는 설계이므로, 이 경우는 정상적으로 매번 REST 조회로 처리된다.
                print(f"ℹ️ [현재가 조회] {ticker}: 실시간 구독 대상이 아니라 서버에서 조회합니다.", flush=True)
        else:
            cache_max_age = self.quote_cache_ttl_seconds
            if cached_quote and time.monotonic() - cached_quote["received_at"] < cache_max_age:
                return cached_quote["quote"]
            print(f"ℹ️ [현재가 캐시 미스] {ticker}: 실시간 시세 미연결 상태이며 캐시가 없거나 만료되어 서버에서 조회합니다.", flush=True)


        url = f"{self.base_url}/api/dostk/stkinfo"
        try:
            data = self._post_with_rate_limit(url, "ka10001", {"stk_cd": ticker})
            if data.get("return_code") not in (None, 0, "0"):
                raise RuntimeError(data.get("return_msg", "키움 현재가 조회가 거절되었습니다."))

            quote = {
                "ticker": self._normalize_ticker(data.get("stk_cd", ticker)),
                "name": data.get("stk_nm", ""),
                "current_price": abs(self._parse_number(data.get("cur_prc", 0))),
                "open_price": abs(self._parse_number(data.get("open_pric", 0))),
                "high_price": abs(self._parse_number(data.get("high_pric", 0))),
                "low_price": abs(self._parse_number(data.get("low_pric", 0))),
                "volume": self._parse_number(data.get("trde_qty", 0)),
            }
            if quote["current_price"] <= 0:
                raise RuntimeError(f"유효하지 않은 현재가 응답: {data}")
            with self._quote_cache_lock:
                self._quote_cache[ticker] = {"quote": quote, "received_at": time.monotonic()}
            return quote
        except (requests.RequestException, TypeError, ValueError, RuntimeError) as e:
            print(f"❌ [현재가 조회 오류] {ticker}: {e}", flush=True)
            raise RuntimeError("현재가 조회 실패: 주문 판단을 중단합니다.") from e

    def start_quote_stream(self, tickers) -> None:
        if self._quote_stream is None:
            self._quote_stream = KiwoomQuoteStream(
                self.get_token,
                self.is_mock,
                self._quote_cache,
                self._quote_cache_lock,
                notifier=self.notifier,
            )
        self._quote_stream.start(tickers)

    def update_quote_subscription(self, add=(), remove=()) -> None:
        """포지션이 새로 생기거나(진입) 완전히 청산될 때 실시간 구독을 그때그때 맞춰준다.
        보유하지도 않는 종목까지 상시 구독하면 트래픽만 늘어나므로, 구독 대상은 항상
        '지금 실제로 들고 있는 종목'으로만 유지한다."""
        if self._quote_stream is not None:
            self._quote_stream.update_subscription(add=add, remove=remove)

    def get_daily_ohlcv(self, ticker: str, start_date: str, end_date: str, upd_stkpc_tp: str = "1") -> pd.DataFrame:
        """ka10081(주식일봉차트조회)로 일봉을 가져온다. pykrx의 get_market_ohlcv_by_date()와
        동일한 컬럼/인덱스 형태(시가/고가/저가/종가/거래량, 날짜 오름차순 DatetimeIndex)로
        맞춰서, 기존에 pykrx 응답을 쓰던 지표 계산 코드를 그대로 재사용할 수 있게 한다.
        한 번 호출에 최근 최대 600영업일(약 2년 반) 분량이 오므로, 우리가 쓰는 20~90일
        범위는 페이징 없이 단일 호출로 충분하다(실측 확인됨)."""
        ticker = self._normalize_ticker(ticker)
        url = f"{self.base_url}/api/dostk/chart"
        body = {"stk_cd": ticker, "base_dt": end_date, "upd_stkpc_tp": upd_stkpc_tp}
        try:
            data = self._post_with_rate_limit(url, "ka10081", body)
            if data.get("return_code") not in (None, 0, "0"):
                raise RuntimeError(data.get("return_msg", "키움 일봉 조회가 거절되었습니다."))

            records = []
            for row in data.get("stk_dt_pole_chart_qry", []):
                dt = row.get("dt", "")
                if not (start_date <= dt <= end_date):
                    continue
                records.append({
                    "날짜": dt,
                    "시가": abs(self._parse_number(row.get("open_pric", 0))),
                    "고가": abs(self._parse_number(row.get("high_pric", 0))),
                    "저가": abs(self._parse_number(row.get("low_pric", 0))),
                    "종가": abs(self._parse_number(row.get("cur_prc", 0))),
                    "거래량": self._parse_number(row.get("trde_qty", 0)),
                })

            df = pd.DataFrame(records)
            if df.empty:
                return df
            df = df.set_index("날짜")
            df.index = pd.to_datetime(df.index, format="%Y%m%d")
            return df.sort_index()
        except Exception as e:
            print(f"❌ [일봉조회 오류] {ticker}: {e}", flush=True)
            raise RuntimeError("일봉 조회 실패") from e

    def fetch_condition_list(self) -> list:
        """HTS에 저장된 조건검색식 목록을 [(seq, name), ...]로 반환한다."""
        if not self._quote_stream:
            raise RuntimeError("실시간 시세 연결이 없어 조건검색 목록을 조회할 수 없습니다.")
        return self._quote_stream.fetch_condition_list()

    def run_condition_search(self, seq: str, stex_tp: str = "K") -> list:
        """조건검색식(seq)을 1회 실행해 매칭 종목을 [{"ticker","name","current_price"}, ...]로 반환한다.
        종목명/현재가도 같이 오므로, 후보군이 작을 때는 별도 이름조회 없이 바로 쓸 수 있다."""
        if not self._quote_stream:
            raise RuntimeError("실시간 시세 연결이 없어 조건검색을 실행할 수 없습니다.")
        raw = self._quote_stream.run_condition_search(seq, stex_tp=stex_tp) or []
        results = []
        for item in raw:
            ticker = self._normalize_ticker(item.get("9001", ""))
            if not ticker:
                continue
            results.append({
                "ticker": ticker,
                "name": item.get("302", ""),
                "current_price": abs(self._parse_number(item.get("10", 0))),
            })
        return results

    def send_order(self, ticker: str, qty: int, price: int = 0, is_buy: bool = True) -> str:
        if not ticker or qty <= 0:
            raise ValueError(f"잘못된 주문 값: ticker={ticker!r}, qty={qty}")

        url = f"{self.base_url}/api/dostk/ordr"
        api_id = "kt10000" if is_buy else "kt10001"
        body = {
            "dmst_stex_tp": "KRX",
            "stk_cd": ticker,
            "ord_qty": str(qty),
            "ord_uv": "" if price == 0 else str(price),
            "trde_tp": "3" if price == 0 else "0"
        }
        try:
            res_json = self._post_with_rate_limit(url, api_id, body)
            order_no = res_json.get("ord_no", "")
            if res_json.get("return_code") in (None, 0, "0") and order_no:
                return order_no
            raise RuntimeError(res_json.get("return_msg", f"주문번호가 없는 응답: {res_json}"))
        except Exception as e:
            side = "매수" if is_buy else "매도"
            print(f"❌ [주문 실패] {side} {ticker} {qty}주: {e}", flush=True)
            self._notify_error(f"order_failed:{ticker}:{side}", f"🚨 [주문 실패] {side} {ticker} {qty}주: {e}")
            return ""

    def _auto_recover_position(self, ticker: str, is_buy: bool, avg_price: int, qty: int, strategy: str) -> bool:
        """지연 체결된 수량을 해당 전략의 JSON 장부에 강제 복구합니다."""
        trader = getattr(self, f"{strategy}_trader", None)
        if not trader:
            return False
            
        if not is_buy: # 매도 지연 복구
            if ticker in trader.positions:
                pos = trader.positions[ticker]
                if qty >= pos["shares"]:
                    del trader.positions[ticker]
                    self.update_quote_subscription(remove=[ticker])
                else:
                    pos["shares"] -= qty
                    pos["invested"] = pos["shares"] * float(pos["avg_price"])
                trader.save_positions()
                return True
            return False

        # 매수 지연 복구
        fee_rate = getattr(trader, "buy_fee_rate", 0.0)
        invested = qty * avg_price * (1 + fee_rate)

        if ticker in trader.positions: # 물타기(추가 매수) 였던 경우
            pos = trader.positions[ticker]
            pos["shares"] += qty
            pos["invested"] += invested
            pos["avg_price"] = pos["invested"] / pos["shares"]
            max_tranches = getattr(trader, "max_tranches", None) or len(getattr(trader, "tranche_weights", [])) or pos.get("tranches", 1)
            pos["tranches"] = min(pos.get("tranches", 1) + 1, max_tranches)
        else: # 1차 신규 매수였던 경우
            try:
                name = self.get_current_quote(ticker).get("name", ticker)
            except Exception:
                name = ticker
            today_str = datetime.now().strftime("%Y%m%d")

            trader.positions[ticker] = {
                "name": name,
                "entry_date": today_str,
                "hold_days": 0,
                "p1": avg_price,
                "avg_price": float(avg_price * (1 + fee_rate)),
                "shares": qty,
                "invested": invested,
                "tranches": 1,
                "rebalanced": False,
                "local_low": avg_price
            }
            self.update_quote_subscription(add=[ticker])
        trader.save_positions()
        return True

    def execute_and_confirm_order(self, ticker: str, qty: int, price: int = 0, is_buy: bool = True, strategy: str = "unknown") -> tuple:

        can_submit, estimated_buy_value = self._can_submit_order(ticker, qty, is_buy)
        if not can_submit:
            return False, 0, 0
        
        if not self._reserve_order_slot(estimated_buy_value):
            return False, 0, 0

        if self.order_preflight:
            try:
                if not self.order_preflight():
                    print(f"⚠️ [주문 차단] 포지션 대사 불일치: {ticker} {qty}주", flush=True)
                    return False, 0, 0
            except Exception as e:
                print(f"⚠️ [주문 차단] 주문 전 대사 오류: {e}", flush=True)
                return False, 0, 0

        ord_no = self.send_order(ticker, qty, price=price, is_buy=is_buy)
        if not ord_no:
            return False, 0, 0

        deadline = time.monotonic() + self.order_confirm_timeout
        
        # 전량 체결될 때까지, 또는 타임아웃까지 계속 폴링한다. 첫 조회에서 일부만 체결된 걸
        # 봤다고 바로 "부분체결"로 확정하고 멈추면, 사실은 시간 초과 전에 나머지도 마저
        # 체결됐을 주문까지 장부에는 절반만 반영되는 사고가 난다(실제로 발생 확인됨) —
        # 남은 시간 동안은 계속 지켜보고, 타임아웃 시점의 최종 체결량으로만 판단한다.
        best_average_price, best_filled_qty = 0, 0
        while True:
            confirmed, average_price, filled_qty = self.get_order_fills(ticker, ord_no)
            if confirmed:
                best_average_price, best_filled_qty = average_price, filled_qty
                if filled_qty >= qty:
                    return True, average_price, filled_qty
            if time.monotonic() >= deadline:
                if best_filled_qty > 0:
                    print(f"⚠️ [부분체결] 주문번호 {ord_no}: 요청 {qty}주 / 체결 {best_filled_qty}주 ({self.order_confirm_timeout:.0f}초 대기 후 확정)", flush=True)
                    self._notify_error(
                        f"partial_fill:{ord_no}",
                        f"⚠️ [부분체결] {ticker} 주문번호 {ord_no}: 요청 {qty}주 / 체결 {best_filled_qty}주 (나머지는 미체결 감시열에 등록됨)",
                        cooldown_seconds=0,
                    )
                    self._record_pending_order(ord_no, ticker, qty, is_buy, best_filled_qty, strategy)
                    return True, best_average_price, best_filled_qty
                print(f"⚠️ [미체결/타임아웃] {ticker} {qty}주 지연 체결 감시열 등록 ({self.order_confirm_timeout}초 초과)", flush=True)
                self._notify_error(
                    f"order_timeout:{ord_no}",
                    f"⚠️ [미체결/타임아웃] {ticker} {qty}주 주문번호 {ord_no}: {self.order_confirm_timeout:.0f}초 내 체결 확인 안 됨, 미체결 감시열에 등록됨",
                    cooldown_seconds=0,
                )
                self._record_pending_order(ord_no, ticker, qty, is_buy, 0, strategy)
                return False, 0, 0
            time.sleep(self.order_confirm_poll_interval)

    def _record_pending_order(self, ord_no: str, ticker: str, qty: int, is_buy: bool, accounted_qty: int, strategy: str = "unknown") -> None:
        order = {
            "ord_no": str(ord_no),
            "ticker": ticker,
            "requested_qty": qty,
            "is_buy": is_buy,
            "accounted_qty": accounted_qty,
            "strategy": strategy, # 장부 복구를 위해 누가 주문했는지 저장
            "submitted_at": datetime.now().isoformat(timespec="seconds"),
        }
        self._pending_orders = [item for item in self._pending_orders if str(item.get("ord_no")) != str(ord_no)]
        self._pending_orders.append(order)
        try:
            atomic_write_json(PENDING_ORDERS_FILE, {"orders": self._pending_orders})
        except Exception as e:
            self._pending_order_error = e
            raise RuntimeError("미체결 주문 상태를 저장하지 못했습니다.") from e

    def _resolve_stale_pending_order(self, order: dict, holdings_detail: dict) -> tuple:
        """ka10076 체결조회는 당일 주문만 반환하므로, 날짜가 지난 미체결은 계좌 실보유현황과
        전략 장부의 차이로 대신 판단한다. 국내 정규장 주문은 당일 유효이며 미체결 시 장마감에
        자동 실효되므로, 날짜가 바뀐 시점엔 이미 '체결되었거나' '취소되었거나' 둘 중 하나다."""
        ticker = self._normalize_ticker(order["ticker"])
        is_buy = order.get("is_buy", True)
        strategy = order.get("strategy", "unknown")
        requested_qty = int(order.get("requested_qty", 0))
        accounted_qty = int(order.get("accounted_qty", 0))
        remaining_requested = max(requested_qty - accounted_qty, 0)

        trader = getattr(self, f"{strategy}_trader", None)
        ledger_qty = int(trader.positions.get(ticker, {}).get("shares", 0)) if trader else 0
        held = holdings_detail.get(ticker, {"qty": 0, "avg_price": 0})

        if is_buy:
            unaccounted = held["qty"] - ledger_qty
            if unaccounted <= 0:
                return False, 0, 0
            filled_qty = min(unaccounted, remaining_requested) if remaining_requested > 0 else unaccounted
            return True, held["avg_price"], filled_qty

        unaccounted = ledger_qty - held["qty"]
        if unaccounted <= 0:
            return False, 0, 0
        filled_qty = min(unaccounted, remaining_requested) if remaining_requested > 0 else unaccounted
        return True, 0, filled_qty

    def reconcile_pending_orders(self) -> bool:
        if not self._pending_orders:
            return False

        still_pending = []
        status_lines = []
        today_str = datetime.now().strftime("%Y%m%d")
        holdings_detail = None

        for order in self._pending_orders:
            ticker = order["ticker"]
            ord_no = order["ord_no"]
            is_buy = order.get("is_buy", True)
            strategy = order.get("strategy", "unknown")

            confirmed, avg_price, filled_qty = self.get_order_fills(ticker, ord_no)

            submitted_date = str(order.get("submitted_at", ""))[:10].replace("-", "")
            is_stale = bool(submitted_date) and submitted_date != today_str
            if not confirmed and is_stale:
                if holdings_detail is None:
                    try:
                        holdings_detail = self._parse_holdings_detail_from_snapshot(self._fetch_account_snapshot())
                    except Exception as e:
                        holdings_detail = {}
                        print(f"⚠️ [미체결 대사] 지연 주문 정리용 보유현황 조회 실패: {e}", flush=True)
                confirmed, avg_price, filled_qty = self._resolve_stale_pending_order(order, holdings_detail)
                if not confirmed:
                    status_lines.append(f"{ticker} 전일 이전 주문 미체결 확인 → 장마감 자동실효로 판단, 대기 해제")
                    continue

            if not confirmed:
                status_lines.append(f"{ticker} 대기중")
                still_pending.append(order)
                continue

            unaccounted_qty = filled_qty - int(order.get("accounted_qty", 0))
            if unaccounted_qty > 0:
                # 3초 타임아웃 뒤늦게 체결된 수량을 감지하고 장부에 오토 리커버리 수행
                if self._auto_recover_position(ticker, is_buy, avg_price, unaccounted_qty, strategy):
                    status_lines.append(f"★ {ticker} {unaccounted_qty}주 장부 복구 완료 ({strategy} 전략)")
                else:
                    status_lines.append(f"{ticker} 복구 실패(수동 확인 필요)")
                    still_pending.append(order)
            else:
                status_lines.append(f"{ticker} 체결 확인 및 대기 해제")

        self._pending_orders = still_pending
        try:
            atomic_write_json(PENDING_ORDERS_FILE, {"orders": self._pending_orders})
        except Exception:
            pass

        if status_lines:
            print("🔄 [미체결 자동 대사] " + " | ".join(status_lines), flush=True)

        return len(still_pending) > 0

    def get_order_fills(self, ticker: str, ord_no: str) -> tuple:
        url = f"{self.base_url}/api/dostk/acnt"
        body = {
            "stk_cd": ticker,
            "qry_tp": "1",
            "sell_tp": "0",
            # ka10076의 ord_no는 "이 주문번호를 조회"가 아니라 "이 주문번호보다 과거에 체결된 내역"을
            # 반환하는 페이징 커서다. 여기에 우리가 확인하려는 주문번호를 그대로 넣으면 API가 그
            # 주문 자체를 결과에서 제외해 버려 항상 미체결로 보인다. 비워서 최근 체결 전체를 받은 뒤
            # 아래에서 ord_no로 클라이언트 측 필터링한다.
            "ord_no": "",
            "stex_tp": "0",
        }
        try:
            data = self._post_with_rate_limit(url, "ka10076", body)
            if data.get("return_code") not in (None, 0, "0"):
                raise RuntimeError(data.get("return_msg", "키움 체결 조회가 거절되었습니다."))

            fills = [fill for fill in data.get("cntr", []) if str(fill.get("ord_no", "")) == str(ord_no)]
            filled_qty = sum(self._parse_number(fill.get("cntr_qty", 0)) for fill in fills)
            filled_value = sum(
                self._parse_number(fill.get("cntr_pric", 0)) * self._parse_number(fill.get("cntr_qty", 0))
                for fill in fills
            )
            if filled_qty <= 0 or filled_value <= 0:
                return False, 0, 0

            average_price = int(round(filled_value / filled_qty))
            return True, average_price, filled_qty
        except Exception as e:
            print(f"❌ [체결조회 실패] 주문번호 {ord_no}: {e}", flush=True)
            return False, 0, 0

    def _can_submit_order(self, ticker: str, qty: int, is_buy: bool) -> tuple:
        if self._order_guard_error:
            print(f"⚠️ [주문 차단] 일일 주문 상태 오류: {self._order_guard_error}", flush=True)
            self._notify_error("order_guard_error", f"🚨 [주문 차단] 일일 주문 상태 파일 오류로 모든 자동 주문이 막혀 있습니다: {self._order_guard_error}")
            return False, 0.0

        if self._pending_order_error:
            print(f"⚠️ [주문 차단] 미체결 주문 상태 오류: {self._pending_order_error}", flush=True)
            self._notify_error("pending_order_error", f"🚨 [주문 차단] 미체결 주문 상태 파일 오류로 모든 자동 주문이 막혀 있습니다: {self._pending_order_error}")
            return False, 0.0
        
        if self.reconcile_pending_orders():
            print("⚠️ [주문 차단] 미체결 주문이 남아 있습니다.", flush=True)
            return False, 0.0

        if os.getenv("TRADING_EMERGENCY_STOP", "false").lower() in ("true", "1", "yes"):
            print("⚠️ [주문 차단] TRADING_EMERGENCY_STOP이 활성화되어 있습니다.", flush=True)
            return False, 0.0

        today_str = datetime.now().strftime("%Y%m%d")
        if self._daily_order_date != today_str:
            self._daily_order_date = today_str
            self._daily_order_count = 0
            self._daily_buy_value = 0.0
            self._daily_start_equity = 0.0
        if self.max_daily_order_count and self._daily_order_count >= self.max_daily_order_count:
            print(f"⚠️ [주문 차단] 일일 주문 한도({self.max_daily_order_count}건)를 초과했습니다.", flush=True)
            return False, 0.0

        if not is_buy:
            return True, 0.0

        try:
            quote = self.get_current_quote(ticker)
            estimated_buy_value = quote["current_price"] * qty
            snapshot = self._fetch_account_snapshot()
            balance = self._parse_balance_from_snapshot(snapshot)
            if self._is_daily_loss_limit_exceeded(balance["total_equity"]):
                return False, 0.0
            usable_cash = max(balance["deposit"], 0.0) * self.max_cash_usage_rate
            holdings = self._parse_holdings_from_snapshot(snapshot)
            projected_exposure = (holdings.get(quote["ticker"], 0) + qty) * quote["current_price"]
            exposure_limit = balance["total_equity"] * self.max_ticker_exposure_rate
        except RuntimeError as e:
            print(f"⚠️ [매수 차단] 위험 한도 조회 실패: {e}", flush=True)
            return False, 0.0

        if estimated_buy_value > usable_cash:
            print(f"⚠️ [매수 차단] 주문금액 {estimated_buy_value:,.0f}원 / 사용 가능 현금 {usable_cash:,.0f}원", flush=True)
            return False, 0.0
        if self.max_single_order_value and estimated_buy_value > self.max_single_order_value:
            print(f"⚠️ [매수 차단] 건별 한도 {self.max_single_order_value:,.0f}원 초과", flush=True)
            return False, 0.0
        if self.max_daily_buy_value and self._daily_buy_value + estimated_buy_value > self.max_daily_buy_value:
            print(f"⚠️ [매수 차단] 일일 매수 한도 {self.max_daily_buy_value:,.0f}원 초과", flush=True)
            return False, 0.0
        if projected_exposure > exposure_limit:
            print(f"⚠️ [매수 차단] 종목 노출 {projected_exposure:,.0f}원 / 한도 {exposure_limit:,.0f}원", flush=True)
            return False, 0.0
        return True, estimated_buy_value

    def _reserve_order_slot(self, estimated_buy_value: float) -> bool:
        self._daily_order_count += 1
        self._daily_buy_value += estimated_buy_value
        try:
            atomic_write_json(
                ORDER_GUARD_FILE,
                {
                    "date": self._daily_order_date,
                    "count": self._daily_order_count,
                    "buy_value": self._daily_buy_value,
                    "start_equity": self._daily_start_equity,
                },
            )
            return True
        except Exception as e:
            self._daily_order_count -= 1
            self._daily_buy_value -= estimated_buy_value
            self._order_guard_error = e
            print(f"⚠️ [주문 차단] 일일 주문 상태 저장 실패: {e}", flush=True)
            return False

    @staticmethod
    def _load_order_guard() -> tuple:
        if not os.path.exists(ORDER_GUARD_FILE):
            return "", 0, 0.0, 0.0
        try:
            with open(ORDER_GUARD_FILE, "r", encoding="utf-8") as file:
                state = json.load(file)
            date = state.get("date", "")
            count = state.get("count", 0)
            buy_value = state.get("buy_value", 0.0)
            start_equity = state.get("start_equity", 0.0)
            if not isinstance(date, str) or not isinstance(count, int) or count < 0 or not isinstance(buy_value, (int, float)) or buy_value < 0 or not isinstance(start_equity, (int, float)) or start_equity < 0:
                raise ValueError("잘못된 일일 주문 상태 형식")
            return date, count, float(buy_value), float(start_equity)
        except (OSError, json.JSONDecodeError, ValueError) as e:
            raise RuntimeError(f"일일 주문 상태 파일을 읽을 수 없습니다: {ORDER_GUARD_FILE}") from e

    def _is_daily_loss_limit_exceeded(self, current_equity: float) -> bool:
        if not self.max_daily_loss_rate and not self.max_daily_loss_value:
            return False
        if self._daily_start_equity <= 0:
            self._daily_start_equity = current_equity
            try:
                atomic_write_json(
                    ORDER_GUARD_FILE,
                    {
                        "date": self._daily_order_date,
                        "count": self._daily_order_count,
                        "buy_value": self._daily_buy_value,
                        "start_equity": self._daily_start_equity,
                    },
                )
            except Exception as e:
                self._order_guard_error = e
                print(f"⚠️ [주문 차단] 일일 손실 기준 저장 실패: {e}", flush=True)
                return True

        daily_loss = max(self._daily_start_equity - current_equity, 0.0)
        daily_loss_rate = daily_loss / self._daily_start_equity if self._daily_start_equity else 0.0
        if self.max_daily_loss_value and daily_loss >= self.max_daily_loss_value:
            print(f"⚠️ [매수 차단] 일일 손실 {daily_loss:,.0f}원 / 한도 {self.max_daily_loss_value:,.0f}원", flush=True)
            return True
        if self.max_daily_loss_rate and daily_loss_rate >= self.max_daily_loss_rate:
            print(f"⚠️ [매수 차단] 일일 손실률 {daily_loss_rate * 100:.2f}% / 한도 {self.max_daily_loss_rate * 100:.2f}%", flush=True)
            return True
        return False

    @staticmethod
    def _load_pending_orders() -> list:
        if not os.path.exists(PENDING_ORDERS_FILE):
            return []
        try:
            with open(PENDING_ORDERS_FILE, "r", encoding="utf-8") as file:
                orders = json.load(file).get("orders", [])
            if not isinstance(orders, list):
                raise ValueError("잘못된 미체결 주문 상태 형식")
            return orders
        except (OSError, json.JSONDecodeError, ValueError) as e:
            raise RuntimeError(f"미체결 주문 상태 파일을 읽을 수 없습니다: {PENDING_ORDERS_FILE}") from e

    @staticmethod
    def _parse_number(value) -> int:
        if value is None or value == "":
            return 0
        if isinstance(value, (int, float)):
            return int(value)
        return int(str(value).replace(",", "").replace("+", "").strip())

    @staticmethod
    def _normalize_ticker(ticker) -> str:
        ticker = str(ticker).strip()
        return ticker[1:] if ticker.startswith("A") and ticker[1:].isdigit() else ticker


class TelegramNotifier:
    ERROR_COOLDOWN_SECONDS = max(float(os.getenv("ERROR_ALERT_COOLDOWN_SECONDS", "600")), 0.0)

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self._last_error_alert_at = {}  # key -> monotonic time, 같은 종류 오류 반복 알림 방지용

    def send(self, text: str):
        if not self.token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            requests.post(url, json={"chat_id": self.chat_id, "text": text}, timeout=5)
        except Exception:
            pass

    def send_error(self, key: str, text: str, cooldown_seconds: float = None) -> None:
        """같은 key의 오류는 쿨다운 시간(기본 ERROR_ALERT_COOLDOWN_SECONDS=600초) 동안 한 번만
        전송한다. 재시도 루프에서 같은 오류가 계속 반복될 때마다 텔레그램이 도배되는 것을 막기 위함이다."""
        cooldown = self.ERROR_COOLDOWN_SECONDS if cooldown_seconds is None else cooldown_seconds
        now = time.monotonic()
        last_at = self._last_error_alert_at.get(key, 0.0)
        if now - last_at < cooldown:
            return
        self._last_error_alert_at[key] = now
        self.send(text)


def reconcile_positions(client, short_trader, mid_trader, notifier) -> bool:
    """브로커 보유수량과 전략 장부가 일치할 때만 자동 주문을 허용한다.
    계좌 조회 자체가 실패하면(네트워크 오류 등) RuntimeError를 그대로 올린다 — 호출부가
    "조회 실패(재시도 필요)"와 "조회는 됐지만 불일치 확인됨"을 구분해서 처리해야 한다."""
    # [추가] 계좌 잔고를 대사하기 전에, 지연 체결된 건이 있다면 먼저 키움 API로 확정하고 장부에 복구한다.
    if hasattr(client, "reconcile_pending_orders"):
        client.reconcile_pending_orders()
        
    expected = {}
    duplicate_tickers = set(short_trader.positions) & set(mid_trader.positions)
    for positions in (short_trader.positions, mid_trader.positions):
        for ticker, position in positions.items():
            normalized_ticker = client._normalize_ticker(ticker)
            expected[normalized_ticker] = expected.get(normalized_ticker, 0) + int(position.get("shares", 0))

    # 계좌 조회 자체가 실패한 것(네트워크 오류 등)과 "조회는 됐는데 실제로 안 맞음"은 다르다.
    # 전자를 여기서 False로 삼켜버리면 호출부(main 루프)가 "오늘 대사 완료"로 확정해버려서
    # 순간적인 네트워크 오류 한 번으로 그날 종일 매매가 막힌다. 그대로 예외를 올려서
    # 호출부가 재시도 여부를 판단하게 한다.
    actual = client.get_account_holdings()

    mismatches = []
    for ticker in sorted(set(expected) | set(actual)):
        expected_quantity = expected.get(ticker, 0)
        actual_quantity = actual.get(ticker, 0)
        if expected_quantity != actual_quantity:
            mismatches.append(f"{ticker}: 장부 {expected_quantity}주 / 계좌 {actual_quantity}주")

    if duplicate_tickers:
        mismatches.extend(f"{ticker}: 단기·중기 전략에 중복 등록" for ticker in sorted(duplicate_tickers))

    if mismatches:
        message = "🚨 [포지션 대사 불일치] 자동 주문을 중단합니다.\n" + "\n".join(mismatches)
        print(message, flush=True)
        notifier.send(message)
        return False

    print(f"✅ [포지션 대사 완료] 계좌 및 전략 장부 {len(actual)}종목 일치", flush=True)
    return True


def load_run_state() -> dict:
    if not os.path.exists(RUN_STATE_FILE):
        return {"date": "", "routines": {}}
    try:
        with open(RUN_STATE_FILE, "r", encoding="utf-8") as file:
            state = json.load(file)
        if not isinstance(state, dict) or not isinstance(state.get("routines", {}), dict):
            raise ValueError("잘못된 실행 상태 형식")
        return state
    except (OSError, json.JSONDecodeError, ValueError) as e:
        raise RuntimeError(f"데몬 실행 상태 파일을 읽을 수 없습니다: {RUN_STATE_FILE}") from e


def initialize_daily_run_state(run_state: dict, today_str: str) -> dict:
    if run_state.get("date") == today_str:
        return run_state
    run_state = {"date": today_str, "routines": {}}
    atomic_write_json(RUN_STATE_FILE, run_state)
    return run_state


def get_interrupted_routines(run_state: dict) -> list:
    return [name for name, status in run_state["routines"].items() if status == "started"]


def run_daily_routine_once(run_state: dict, routine_name: str, routine) -> bool:
    status = run_state["routines"].get(routine_name)
    if status == "completed":
        return False
    if status == "started":
        raise RuntimeError(f"{routine_name} 루틴이 이전 실행 중 중단되었습니다. 계좌와 주문내역을 확인하세요.")

    run_state["routines"][routine_name] = "started"
    atomic_write_json(RUN_STATE_FILE, run_state)
    routine()
    run_state["routines"][routine_name] = "completed"
    atomic_write_json(RUN_STATE_FILE, run_state)
    return True


def load_daemon_config() -> dict:
    """환경변수를 읽어 데몬 실행에 필요한 설정을 검증하고 반환한다. main()/test()가 공유."""
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    account_no = os.getenv("KIWOOM_ACCOUNT_NO", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    if not is_mock and os.getenv("ENABLE_LIVE_TRADING", "") != "CONFIRM":
        raise RuntimeError("실전투자는 ENABLE_LIVE_TRADING=CONFIRM 설정이 필요합니다.")
    short_closing_start = int(os.getenv("SHORT_CLOSING_START_MINUTE", "15"))
    short_closing_end = int(os.getenv("SHORT_CLOSING_END_MINUTE", "18"))
    mid_closing_start = int(os.getenv("MID_CLOSING_START_MINUTE", "18"))
    mid_closing_end = int(os.getenv("MID_CLOSING_END_MINUTE", "20"))
    if not (0 <= short_closing_start < short_closing_end <= mid_closing_start < mid_closing_end <= 59):
        raise ValueError("마감 루틴 시간은 0~59분 범위에서 단기 후 중기 순으로 겹치지 않아야 합니다.")

    return {
        "app_key": app_key,
        "secret_key": secret_key,
        "account_no": account_no,
        "is_mock": is_mock,
        "tg_token": os.getenv("TELEGRAM_BOT_TOKEN", ""),
        "tg_chat_id": os.getenv("TELEGRAM_CHAT_ID", ""),
        "short_closing_start": short_closing_start,
        "short_closing_end": short_closing_end,
        "mid_closing_start": mid_closing_start,
        "mid_closing_end": mid_closing_end,
    }


def bootstrap_daemon(config: dict) -> tuple:
    """공통 클라이언트/트레이더 초기화, 후보 이력 적재, 실시간 시세 구독, 시작 알림까지 main()/test()가 공유하는 준비 절차."""
    client = KiwoomSharedClient(config["app_key"], config["secret_key"], config["account_no"], is_mock=config["is_mock"])
    notifier = TelegramNotifier(config["tg_token"], config["tg_chat_id"])
    client.notifier = notifier  # 클라이언트 내부(토큰 발급, 주문 등)에서도 심각한 오류를 바로 보고할 수 있도록 연결

    short_trader = ShortTermTrader(client, notifier)
    mid_trader = MidTermTrader(client, notifier)

    client.short_trader = short_trader
    client.mid_trader = mid_trader
    print("▶ [부트스트랩] 잔여 미체결 주문 확인 중...", flush=True)
    client.reconcile_pending_orders()  # 데몬 재시작 시 잔여 미체결이 있다면 즉시 복구 수행

    client.order_preflight = lambda: reconcile_positions(client, short_trader, mid_trader, notifier)

    # 실시간 구독은 실제로 보유 중인 종목만 상시 유지한다. 전체 유니버스(단기 100 + 중기 50)를
    # 하루 종일 구독하면 트래픽만 늘어날 뿐, 후보 스캔은 하루 1회 REST 조회로 충분하고
    # 장중 감시가 필요한 대상은 어차피 보유 종목뿐이다. 신규 진입/청산 시점마다
    # update_quote_subscription()으로 그때그때 추가·해지한다.
    # 조건검색(CNSRLST/CNSRREQ)도 이 웹소켓 연결을 그대로 쓰므로, 아래의 후보 일봉 사전
    # 적재(조건검색 호출)보다 반드시 먼저 연결돼 있어야 한다.
    print("▶ [부트스트랩] 실시간 시세 구독 시작 중... (보유 종목만)", flush=True)
    client.start_quote_stream(set(short_trader.positions) | set(mid_trader.positions))

    today_str = datetime.now().strftime("%Y%m%d")
    print("▶ [부트스트랩] 거래일 확인 중...", flush=True)
    try:
        is_trading = short_trader.is_trading_day(today_str)
    except Exception as e:
        # 부트스트랩 시점의 사전 적재는 실패해도 나중에 스캔 중 REST로 보충되므로,
        # 확인 실패를 "휴장"으로 단정하지 않고 그냥 사전 적재만 건너뛴다.
        print(f"⚠️ [부트스트랩] 거래일 확인 실패, 후보 일봉 사전 적재는 건너뜁니다: {e}", flush=True)
        is_trading = False
    if is_trading:
        print("▶ [부트스트랩] 단기형 후보 일봉 사전 적재 중...", flush=True)
        short_trader.prepare_candidate_history()
        print("▶ [부트스트랩] 중기형 후보 일봉 사전 적재 중...", flush=True)
        mid_trader.prepare_candidate_history()
    else:
        print("ℹ️ [부트스트랩] 휴장일이거나 거래일 확인 실패로 후보 일봉 사전 적재를 건너뜁니다.", flush=True)
    print("▶ [부트스트랩] 완료", flush=True)

    mode_str = "모의투자" if config["is_mock"] else "실전투자"
    print("="*65, flush=True)
    print(f" [투트랙 자동매매 마스터 데몬] 가동 시작 ({mode_str})", flush=True)
    print(f" • 계좌: {config['account_no']} (단기 50% / 중기 50% 분할 운용)", flush=True)
    print("="*65, flush=True)

    notifier.send(f"🚀 [마스터 데몬 시작] 투트랙 자동매매 엔진({mode_str})이 정상 가동되었습니다.")

    return client, notifier, short_trader, mid_trader


def initialize_daemon_run_state(client, short_trader, mid_trader, notifier) -> tuple:
    """당일 실행 상태를 적재하고, 포지션 대사 및 중단된 루틴 여부에 따라 매매 활성화 여부를 결정한다.
    반환하는 두 번째 값(reconciled_date)은 대사 확인이 실제로 성공했을 때만 오늘 날짜이고,
    실패(네트워크 오류 등)했을 때는 빈 문자열이다 — main()/test()의 루프가 이 값을
    reconciled_date 시드로 그대로 쓰므로, 빈 문자열이면 첫 루프에서 즉시 재시도하게 된다."""
    today_str = datetime.now().strftime("%Y%m%d")
    run_state = initialize_daily_run_state(load_run_state(), today_str)
    try:
        trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
        reconciled_date = today_str
    except Exception as e:
        trading_enabled = False
        reconciled_date = ""
        print(f"⚠️ [포지션 대사 확인 실패] {today_str}: {e} (일시적 오류로 간주, 데몬 루프에서 재시도합니다)", flush=True)
        notifier.send_error("reconcile_check", f"⚠️ [포지션 대사 확인 실패] {today_str}: {e}\n데몬 루프에서 재시도합니다.")
    interrupted_routines = get_interrupted_routines(run_state)
    if interrupted_routines:
        message = "🚨 [재시작 안전 차단] 미완료 루틴: " + ", ".join(interrupted_routines)
        print(message, flush=True)
        notifier.send(message)
        trading_enabled = False
    return run_state, reconciled_date, trading_enabled


def main():
    config = load_daemon_config()
    short_closing_start = config["short_closing_start"]
    mid_closing_start = config["mid_closing_start"]

    client, notifier, short_trader, mid_trader = bootstrap_daemon(config)

    run_state, reconciled_date, trading_enabled = initialize_daemon_run_state(client, short_trader, mid_trader, notifier)
    market_status_date = ""
    market_is_open = False

    while True:
        try:
            now = datetime.now()
            today_str = now.strftime("%Y%m%d")

            # 평일(월~금) 체크
            if now.weekday() < 5:
                trading_day_check_failed = False
                if market_status_date != today_str:
                    try:
                        market_is_open = short_trader.is_trading_day(today_str)
                        market_status_date = today_str  # 확인에 성공했을 때만 당일 상태로 확정한다.
                        if not market_is_open:
                            print(f"ℹ️ [휴장 확인] {today_str}: 자동 매매를 건너뜁니다.", flush=True)
                    except Exception as e:
                        # 네트워크 오류 등으로 확인 자체에 실패한 것과 "확인된 휴장"은 다르다.
                        # market_status_date를 확정하지 않아야 다음 주기에 다시 확인을 시도한다 —
                        # 여기서 확정해버리면 실제 거래일인데도 하루 종일 매매가 막혀버린다.
                        market_is_open = False
                        trading_day_check_failed = True
                        print(f"⚠️ [거래일 확인 실패] {today_str}: {e} (일시적 오류로 간주, 잠시 후 재시도)", flush=True)
                        notifier.send_error("trading_day_check", f"⚠️ [거래일 확인 실패] {today_str}: {e}\n30초마다 재시도 중입니다. 반복되면 네트워크 상태를 확인해주세요.")

                if not market_is_open:
                    # 확인된 휴장은 어차피 오늘 할 일이 없으니 길게 쉬어도 되지만, 확인 자체가
                    # 실패한 경우(네트워크 순간 오류 등)는 실제 거래일일 수 있으므로 짧게 재시도한다.
                    time.sleep(30 if trading_day_check_failed else 300)
                    continue

                # -------------------------------
                # 일일 루틴 실행 순서 관리
                # -------------------------------
                if reconciled_date != today_str:
                    run_state = initialize_daily_run_state(run_state, today_str)
                    try:
                        trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
                        reconciled_date = today_str  # 대사 확인에 성공했을 때만 당일로 확정한다.
                    except Exception as e:
                        # 조회 실패(네트워크 오류 등)를 "불일치 확인됨"과 똑같이 취급해 오늘로
                        # 확정해버리면, 순간적인 오류 한 번으로 그날 종일 매매가 막힌다.
                        # reconciled_date를 확정하지 않아 다음 주기에 다시 시도하게 한다.
                        trading_enabled = False
                        print(f"⚠️ [포지션 대사 확인 실패] {today_str}: {e} (일시적 오류로 간주, 잠시 후 재시도)", flush=True)
                        notifier.send_error("reconcile_check", f"⚠️ [포지션 대사 확인 실패] {today_str}: {e}\n5분마다 재시도 중입니다. 반복되면 네트워크 상태를 확인해주세요.")

                if not trading_enabled:
                    time.sleep(300)
                    continue

                # -------------------------------------------------------------
                # [개선된 스케줄러]: 상태(State)와 순차적 if문을 사용해 지연 시에도 건너뜀 방지
                # -------------------------------------------------------------
                
                # 당일 루틴 완료 여부 상태 확인
                is_warmup_done = run_state.get("routines", {}).get("warmup") == "completed"
                is_short_done = run_state.get("routines", {}).get("short_closing") == "completed"
                is_mid_done = run_state.get("routines", {}).get("mid_closing") == "completed"

                # 최종 마감 기한 (정규장 마감 15:30 이전인 15:29까지 유예 시간 부여)
                market_close_minute = 30 

                # 1) 15:00 ~ 15:29 : KRX 세션 웜업 (당일 1회)
                if not is_warmup_done and now.hour == 15 and 0 <= now.minute < market_close_minute:
                    if run_daily_routine_once(run_state, "warmup", short_trader.warmup_krx_session):
                        time.sleep(5)
                    continue

                # 2) 15:15 ~ 15:29 : [Track 1] 단기형 마감 근접 루틴 (당일 1회)
                # 설정된 short_closing_start 시간이 지났고, 아직 안 돌았으면 무조건 실행
                if not is_short_done and now.hour == 15 and short_closing_start <= now.minute < market_close_minute:
                    if run_daily_routine_once(run_state, "short_closing", short_trader.run_closing_routine):
                        time.sleep(5)
                    continue

                # 3) 15:18 ~ 15:29 : [Track 2] 중기형 마감 근접 루틴 (당일 1회)
                # 단기형 지연으로 시간이 밀렸더라도 아직 안 돌았으면 이어서 실행됨
                if not is_mid_done and now.hour == 15 and mid_closing_start <= now.minute < market_close_minute:
                    if run_daily_routine_once(run_state, "mid_closing", mid_trader.run_daily_routine):
                        time.sleep(5)
                    continue

                # 4) 14:50 ~ 14:59 : 단기/중기 후보 일봉 사전 적재 (당일 1회)
                if now.hour == 14 and now.minute >= 50:
                    short_trader.prepare_candidate_history()
                    mid_trader.prepare_candidate_history()
                    time.sleep(60)
                    continue

                # 5) 09:01 ~ 15:00 : [Track 1] 실시간 감시 (단기 루틴 시작 직전까지만 가동)
                if (now.hour == 9 and now.minute >= 1) or (9 < now.hour < 15) or (now.hour == 15 and now.minute < short_closing_start):
                    short_trader.run_intraday_monitoring()
                    time.sleep(60)
                    continue

                # 실행할 조건이 없는 유휴 시간
                time.sleep(30)


            else:
                # 주말 대기
                time.sleep(300)

        except Exception as e:
            print(f"❌ [마스터 데몬 예외] {e}", flush=True)
            notifier.send_error("main_loop_exception", f"🚨 [마스터 데몬 예외] {e}\n10초 후 자동 재시도합니다. 반복되면 로그를 확인해주세요.")
            time.sleep(10)


if __name__ == "__main__":
    main()