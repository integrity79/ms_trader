# main.py
import os
import json
import threading
import time
import inspect  # <--- 이 줄을 새로 추가합니다.
from datetime import datetime, timedelta
from matplotlib import ticker
import requests
from dotenv import load_dotenv
load_dotenv()

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
        except Exception as e:
            print(f"❌ [토큰 발급 오류] {e}", flush=True)
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

    def get_account_balance(self) -> dict:
        url = f"{self.base_url}/api/dostk/acnt"
        body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}
        try:
            data = self._post_with_rate_limit(url, "kt00018", body)

            if data.get("return_code") not in (None, 0, "0"):
                raise RuntimeError(data.get("return_msg", "키움 잔고 조회가 거절되었습니다."))

            def parse_amt(val) -> int:
                if val is None or val == "":
                    return 0
                if isinstance(val, (int, float)): return int(val)
                clean = str(val).replace(",", "").replace("+", "").strip()
                return int(clean) if clean else 0

            tot = parse_amt(data.get("prsm_dpst_aset_amt", 0))
            stk = parse_amt(data.get("tot_evlt_amt", 0))
            return {"total_equity": float(tot), "deposit": float(tot - stk), "stock_eval": float(stk)}
        except Exception as e:
            print(f"❌ [잔고조회 오류] {e}", flush=True)
            raise RuntimeError("잔고 조회 실패: 신규 주문을 중단합니다.") from e

    def get_account_holdings(self) -> dict:
        url = f"{self.base_url}/api/dostk/acnt"
        body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}
        try:
            data = self._post_with_rate_limit(url, "kt00018", body)
            if data.get("return_code") not in (None, 0, "0"):
                raise RuntimeError(data.get("return_msg", "키움 보유종목 조회가 거절되었습니다."))

            holdings = {}
            for item in data.get("acnt_evlt_remn_indv_tot", []):
                ticker = self._normalize_ticker(item.get("stk_cd", ""))
                quantity = self._parse_number(item.get("rmnd_qty", 0))
                if ticker and quantity > 0:
                    holdings[ticker] = quantity
            return holdings
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
            # 실시간 체결 구독 중에는 새 체결이 없다는 것 자체가 "가격 불변"을 의미하므로
            # 나이 제한 없이 마지막 캐시 값을 그대로 신뢰한다.
            if cached_quote:
                return cached_quote["quote"]
            print(f"ℹ️ [현재가 캐시 미스] {ticker}: 실시간 체결이 아직 없어 서버에서 조회합니다.", flush=True)
        else:
            cache_max_age = self.quote_cache_ttl_seconds
            if cached_quote and time.monotonic() - cached_quote["received_at"] < cache_max_age:
                return cached_quote["quote"]
            print(f"ℹ️ [현재가 캐시 미스] {ticker}: 실시간 시세 미연결 상태이며 캐시가 없거나 만료되어 서버에서 조회합니다.", flush=True)


        url = f"{self.base_url}/api/dostk/stkinfo"
        try:
            data = self._post_with_rate_limit(url, "ka10001", {"stk_cd": ticker})
            time.sleep(1)
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
            )
        self._quote_stream.start(tickers)

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
            return ""

    def _detect_strategy_from_caller(self) -> str:
        """호출 스택을 분석하여 단기/중기 전략 중 누가 주문을 요청했는지 자동 감지합니다."""
        try:
            for frame_info in inspect.stack()[1:7]:
                if "trader_short" in frame_info.filename:
                    return "short"
                if "trader_mid" in frame_info.filename:
                    return "mid"
        except Exception:
            pass
        return "unknown"

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
            pos["tranches"] = pos.get("tranches", 1) + 1
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
        trader.save_positions()
        return True

    def execute_and_confirm_order(self, ticker: str, qty: int, price: int = 0, is_buy: bool = True) -> tuple:

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

        strategy = self._detect_strategy_from_caller() # 호출처 추적 추가
        deadline = time.monotonic() + self.order_confirm_timeout
        
        while True:
            confirmed, average_price, filled_qty = self.get_order_fills(ticker, ord_no)
            if confirmed:
                if filled_qty < qty:
                    print(f"⚠️ [부분체결] 주문번호 {ord_no}: 요청 {qty}주 / 체결 {filled_qty}주", flush=True)
                    self._record_pending_order(ord_no, ticker, qty, is_buy, filled_qty, strategy)
                return True, average_price, filled_qty
            if time.monotonic() >= deadline:
                print(f"⚠️ [미체결/타임아웃] {ticker} {qty}주 지연 체결 감시열 등록 ({self.order_confirm_timeout}초 초과)", flush=True)
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

    def reconcile_pending_orders(self) -> bool:
        if not self._pending_orders:
            return False

        still_pending = []
        status_lines = []

        for order in self._pending_orders:
            ticker = order["ticker"]
            ord_no = order["ord_no"]
            is_buy = order.get("is_buy", True)
            strategy = order.get("strategy", "unknown")

            confirmed, avg_price, filled_qty = self.get_order_fills(ticker, ord_no)
            
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
            "ord_no": ord_no,
            "stex_tp": "1",
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
            return False, 0.0

        if self._pending_order_error:
            print(f"⚠️ [주문 차단] 미체결 주문 상태 오류: {self._pending_order_error}", flush=True)
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
            balance = self.get_account_balance()
            if self._is_daily_loss_limit_exceeded(balance["total_equity"]):
                return False, 0.0
            usable_cash = max(balance["deposit"], 0.0) * self.max_cash_usage_rate
            holdings = self.get_account_holdings()
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
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    def send(self, text: str):
        if not self.token or not self.chat_id:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            requests.post(url, json={"chat_id": self.chat_id, "text": text}, timeout=5)
        except Exception:
            pass


def reconcile_positions(client, short_trader, mid_trader, notifier) -> bool:
    """브로커 보유수량과 전략 장부가 일치할 때만 자동 주문을 허용한다."""
    # [추가] 계좌 잔고를 대사하기 전에, 지연 체결된 건이 있다면 먼저 키움 API로 확정하고 장부에 복구한다.
    if hasattr(client, "reconcile_pending_orders"):
        client.reconcile_pending_orders()
        
    expected = {}
    duplicate_tickers = set(short_trader.positions) & set(mid_trader.positions)
    for positions in (short_trader.positions, mid_trader.positions):
        for ticker, position in positions.items():
            normalized_ticker = client._normalize_ticker(ticker)
            expected[normalized_ticker] = expected.get(normalized_ticker, 0) + int(position.get("shares", 0))

    try:
        actual = client.get_account_holdings()
    except RuntimeError as e:
        message = f"🚨 [포지션 대사 실패] {e} 자동 주문을 중단합니다."
        print(message, flush=True)
        notifier.send(message)
        return False

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


def main():
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    account_no = os.getenv("KIWOOM_ACCOUNT_NO", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    if not is_mock and os.getenv("ENABLE_LIVE_TRADING", "") != "CONFIRM":
        raise RuntimeError("실전투자는 ENABLE_LIVE_TRADING=CONFIRM 설정이 필요합니다.")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    short_closing_start = int(os.getenv("SHORT_CLOSING_START_MINUTE", "15"))
    short_closing_end = int(os.getenv("SHORT_CLOSING_END_MINUTE", "18"))
    mid_closing_start = int(os.getenv("MID_CLOSING_START_MINUTE", "18"))
    mid_closing_end = int(os.getenv("MID_CLOSING_END_MINUTE", "20"))
    if not (0 <= short_closing_start < short_closing_end <= mid_closing_start < mid_closing_end <= 59):
        raise ValueError("마감 루틴 시간은 0~59분 범위에서 단기 후 중기 순으로 겹치지 않아야 합니다.")

    # 1. 공통 클라이언트 및 알림 객체 생성
    client = KiwoomSharedClient(app_key, secret_key, account_no, is_mock=is_mock)
    notifier = TelegramNotifier(tg_token, tg_chat_id)

    # 2. 두 트레이더 모듈 초기화 (의존성 주입)
    short_trader = ShortTermTrader(client, notifier)
    mid_trader = MidTermTrader(client, notifier)

    # --- 새로 추가하는 부분 ---
    client.short_trader = short_trader 
    client.mid_trader = mid_trader
    client.reconcile_pending_orders() # 데몬 재시작 시 잔여 미체결이 있다면 즉시 복구 수행
    # -----------------------

    client.order_preflight = lambda: reconcile_positions(client, short_trader, mid_trader, notifier)

    today_str = datetime.now().strftime("%Y%m%d")
    if short_trader.is_trading_day(today_str):
        short_trader.prepare_candidate_history()
        mid_trader.prepare_candidate_history()
    client.start_quote_stream(
        set(short_trader.fetch_universe())
        | set(mid_trader.universe_map)
        | set(short_trader.positions)
        | set(mid_trader.positions)
    )

    mode_str = "모의투자" if is_mock else "실전투자"
    print("="*65, flush=True)
    print(f" [투트랙 자동매매 마스터 데몬] 가동 시작 ({mode_str})", flush=True)
    print(f" • 계좌: {account_no} (단기 50% / 중기 50% 분할 운용)", flush=True)
    print("="*65, flush=True)

    notifier.send(f"🚀 [마스터 데몬 시작] 투트랙 자동매매 엔진({mode_str})이 정상 가동되었습니다.")

    run_state = initialize_daily_run_state(load_run_state(), today_str)
    reconciled_date = today_str
    trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
    market_status_date = ""
    market_is_open = False
    interrupted_routines = get_interrupted_routines(run_state)
    if interrupted_routines:
        message = "🚨 [재시작 안전 차단] 미완료 루틴: " + ", ".join(interrupted_routines)
        print(message, flush=True)
        notifier.send(message)
        trading_enabled = False

    while True:
        try:
            now = datetime.now()
            today_str = now.strftime("%Y%m%d")

            # 평일(월~금) 체크
            if now.weekday() < 5:
                if market_status_date != today_str:
                    market_is_open = short_trader.is_trading_day(today_str)
                    market_status_date = today_str
                    if not market_is_open:
                        print(f"ℹ️ [휴장 또는 거래일 미확인] {today_str}: 자동 매매를 건너뜁니다.", flush=True)

                if not market_is_open:
                    time.sleep(300)
                    continue

                # -------------------------------
                # 일일 루틴 실행 순서 관리
                # -------------------------------
                if reconciled_date != today_str:
                    run_state = initialize_daily_run_state(run_state, today_str)
                    trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
                    reconciled_date = today_str

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
            time.sleep(10)


def test():
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    account_no = os.getenv("KIWOOM_ACCOUNT_NO", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    if not is_mock and os.getenv("ENABLE_LIVE_TRADING", "") != "CONFIRM":
        raise RuntimeError("실전투자는 ENABLE_LIVE_TRADING=CONFIRM 설정이 필요합니다.")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    short_closing_start = int(os.getenv("SHORT_CLOSING_START_MINUTE", "15"))
    short_closing_end = int(os.getenv("SHORT_CLOSING_END_MINUTE", "18"))
    mid_closing_start = int(os.getenv("MID_CLOSING_START_MINUTE", "18"))
    mid_closing_end = int(os.getenv("MID_CLOSING_END_MINUTE", "20"))
    if not (0 <= short_closing_start < short_closing_end <= mid_closing_start < mid_closing_end <= 59):
        raise ValueError("마감 루틴 시간은 0~59분 범위에서 단기 후 중기 순으로 겹치지 않아야 합니다.")

    # 1. 공통 클라이언트 및 알림 객체 생성
    client = KiwoomSharedClient(app_key, secret_key, account_no, is_mock=is_mock)
    notifier = TelegramNotifier(tg_token, tg_chat_id)

    # 2. 두 트레이더 모듈 초기화 (의존성 주입)
    short_trader = ShortTermTrader(client, notifier)
    mid_trader = MidTermTrader(client, notifier)

    # --- 새로 추가하는 부분 ---
    client.short_trader = short_trader 
    client.mid_trader = mid_trader
    client.reconcile_pending_orders() # 데몬 재시작 시 잔여 미체결이 있다면 즉시 복구 수행
    # -----------------------

    client.order_preflight = lambda: reconcile_positions(client, short_trader, mid_trader, notifier)

    today_str = datetime.now().strftime("%Y%m%d")
    if short_trader.is_trading_day(today_str):
        short_trader.prepare_candidate_history()
        mid_trader.prepare_candidate_history()

    client.start_quote_stream(
        set(short_trader.fetch_universe())
        | set(mid_trader.universe_map)
        | set(short_trader.positions)
        | set(mid_trader.positions)
    )

    mode_str = "모의투자" if is_mock else "실전투자"
    print("="*65, flush=True)
    print(f" [투트랙 자동매매 마스터 데몬] 가동 시작 ({mode_str})", flush=True)
    print(f" • 계좌: {account_no} (단기 50% / 중기 50% 분할 운용)", flush=True)
    print("="*65, flush=True)

    notifier.send(f"🚀 [마스터 데몬 시작] 투트랙 자동매매 엔진({mode_str})이 정상 가동되었습니다.")

    run_state = initialize_daily_run_state(load_run_state(), today_str)
    reconciled_date = today_str
    trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
    market_status_date = ""
    market_is_open = False
    interrupted_routines = get_interrupted_routines(run_state)
    if interrupted_routines:
        message = "🚨 [재시작 안전 차단] 미완료 루틴: " + ", ".join(interrupted_routines)
        print(message, flush=True)
        notifier.send(message)
        trading_enabled = False

    while True:
        try:
            now = datetime.now()
            today_str = now.strftime("%Y%m%d")

            # 평일(월~금) 체크
            if now.weekday() < 5:
                if market_status_date != today_str:
                    market_is_open = short_trader.is_trading_day(today_str)
                    market_status_date = today_str
                
                if not market_is_open:
                    print(f"ℹ️ [휴장 또는 거래일 미확인] {today_str}: 매매 테스트를 건너뜁니다.", flush=True)
                    break  

                if reconciled_date != today_str:
                    run_state = initialize_daily_run_state(run_state, today_str)
                    trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
                    reconciled_date = today_str

                if not trading_enabled:
                    print(f"ℹ️ [매매 비활성] {today_str}: 매매가 비활성화되어 루틴을 건너뜁니다.", flush=True)
                    return

                if client.reconcile_pending_orders():
                    print("⚠️ [주문 차단] 미체결 주문이 남아 있습니다.", flush=True)
                    time.sleep(3)
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
                # 1) KRX 세션 웜업 
                if not is_warmup_done :
                    if run_daily_routine_once(run_state, "warmup", short_trader.warmup_krx_session):
                        time.sleep(5)
                    continue

                # 2) [Track 1] 단기형 마감 근접 루틴 (당일 1회)
                # 설정된 short_closing_start 시간이 지났고, 아직 안 돌았으면 무조건 실행
                if not is_short_done :
                    if run_daily_routine_once(run_state, "short_closing", short_trader.run_closing_routine):
                        time.sleep(5)
                    continue

                # 3) [Track 2] 중기형 마감 근접 루틴 (당일 1회)
                # 단기형 지연으로 시간이 밀렸더라도 아직 안 돌았으면 이어서 실행됨
                if not is_mid_done :
                    if run_daily_routine_once(run_state, "mid_closing", mid_trader.run_daily_routine):
                        time.sleep(5)
                    continue

                # 실행할 조건이 없는 유휴 시간
                time.sleep(30)

            else :
                print(f"ℹ️ [주말 또는 휴장] {today_str}: 매매 루틴을 건너뜁니다.", flush=True)
                break

        except Exception as e:
            print(f"❌ [마스터 데몬 예외] {e}", flush=True)
            break

if __name__ == "__main__":
    test()