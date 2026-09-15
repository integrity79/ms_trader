# realtime_quotes.py

import asyncio
import json
import os
import threading
import time

import websockets


class KiwoomQuoteStream:
    RECONNECT_MIN_SECONDS = 3.0
    RECONNECT_MAX_SECONDS = 30.0
    CONNECT_WAIT_SECONDS = 10.0
    # 키움 REST 웹소켓은 REG 1건(grp_no 1개)당 등록 가능한 종목 수가 제한되어 있다.
    # 그룹번호(grp_no)를 나눠 여러 번 REG를 보내면 그룹당 한도를 넘는 종목도 등록할 수 있다.
    REG_CHUNK_SIZE = max(int(os.getenv("QUOTE_STREAM_REG_CHUNK_SIZE", "100")), 1)

    def __init__(self, token_provider, is_mock: bool, quote_cache: dict, cache_lock, notifier=None):
        host = "mockapi.kiwoom.com" if is_mock else "api.kiwoom.com"
        self.uri = f"wss://{host}:10000/api/dostk/websocket"
        self.token_provider = token_provider
        self.quote_cache = quote_cache
        self.cache_lock = cache_lock
        self.notifier = notifier  # 재연결이 계속 실패할 때 텔레그램으로 보고하기 위함(선택)
        self.tickers = set()
        self.thread = None
        self._connected = threading.Event()
        self._stop = False
        # 종목별로 어떤 grp_no에 등록했는지 추적한다. REMOVE는 grp_no가 REG 때와 정확히
        # 일치해야 하며(실측 확인됨), 같은 그룹 안에서도 종목 단위로 개별 해지가 가능하다.
        self._ticker_grp = {}
        self._next_grp_no = 1
        self._ws = None
        self._loop = None
        self._sub_lock = threading.Lock()
        # 조건검색(CNSRLST/CNSRREQ) 요청-응답을 이벤트루프 스레드 안에서만 다룬다.
        # (trnm, seq) 조합별로 응답을 기다리는 Future를 큐에 쌓아두고, 수신 루프에서 해소한다.
        # trnm만으로 매칭하면, 이전에 타임아웃되어 이미 포기한 요청의 응답이 뒤늦게 도착했을 때
        # 그걸 전혀 다른(지금 막 보낸) 요청의 답으로 잘못 삼켜버리는 문제가 실제로 관찰되어서
        # seq까지 함께 매칭한다. CNSRLST는 seq가 없으므로 (trnm, None)으로 취급된다.
        self._condition_waiters = {}

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def fetch_condition_list(self, timeout: float = 10.0) -> list:
        """저장된 조건검색식 목록을 [(seq, name), ...]로 반환한다."""
        message = self._request_condition("CNSRLST", {"trnm": "CNSRLST"}, timeout)
        return [(str(item[0]), item[1]) for item in message.get("data", [])]

    def run_condition_search(self, seq: str, stex_tp: str = "K", timeout: float = 20.0) -> list:
        """조건검색식(seq)을 1회 실행해 매칭 종목 데이터(dict) 리스트를 반환한다."""
        payload = {
            "trnm": "CNSRREQ", "seq": str(seq), "search_type": "0",
            "stex_tp": stex_tp, "cont_yn": "N", "next_key": "",
        }
        message = self._request_condition("CNSRREQ", payload, timeout)
        # 매칭 0건일 때 서버가 "data" 키 자체를 생략하지 않고 null로 내려주는 경우가 있어
        # (관찰됨), .get(..., [])만으로는 못 걸러진다 — 존재하되 null인 경우까지 빈 리스트로 처리.
        return message.get("data") or []

    def _request_condition(self, trnm: str, payload: dict, timeout: float) -> dict:
        loop, ws = self._loop, self._ws
        if loop is None or ws is None:
            raise RuntimeError("실시간 시세 연결이 준비되지 않아 조건검색을 요청할 수 없습니다.")
        future = asyncio.run_coroutine_threadsafe(
            self._await_condition_response(trnm, payload, timeout), loop
        )
        try:
            message = future.result(timeout=timeout + 5)
        except asyncio.TimeoutError:
            raise RuntimeError(f"조건검색 응답을 {timeout:.0f}초 내에 받지 못했습니다({trnm}, seq={payload.get('seq', '')}).")
        if message.get("return_code") not in (0, "0", None):
            raise RuntimeError(message.get("return_msg") or f"조건검색 요청이 거절되었습니다({trnm}).")
        return message

    @staticmethod
    def _seq_key(value):
        # 스펙 예시 응답에 'seq': '2  '처럼 공백 패딩이 붙는 경우가 있어, 요청/응답 양쪽 다
        # 이 함수를 거쳐야 패딩 차이로 매칭이 어긋나지 않는다.
        return str(value).strip() if value is not None else None

    async def _await_condition_response(self, trnm: str, payload: dict, timeout: float) -> dict:
        key = (trnm, self._seq_key(payload.get("seq")))
        response_future = self._loop.create_future()
        self._condition_waiters.setdefault(key, []).append(response_future)
        try:
            await self._ws.send(json.dumps(payload))
            return await asyncio.wait_for(response_future, timeout=timeout)
        finally:
            waiters = self._condition_waiters.get(key)
            if waiters and response_future in waiters:
                waiters.remove(response_future)

    def start(self, tickers) -> None:
        if self.thread and self.thread.is_alive():
            print("ℹ️ [실시간 시세 스레드 이미 실행 중]", flush=True)
            self.update_subscription(add=tickers)
            return

        self.update_subscription(add=tickers)

        print("ℹ️ [실시간 시세 스레드 시작]", flush=True)
        self.thread = threading.Thread(target=self._run, name="kiwoom-quote-stream", daemon=True)
        self.thread.start()
        # 최초 연결이 아직 안 됐다면 잠시 대기해 첫 라운드 루틴에서 캐시가 완전히 비는 것을 줄인다.

        print("ℹ️ [실시간 시세 연결 대기]", flush=True)
        self._connected.wait(timeout=self.CONNECT_WAIT_SECONDS)

    def update_subscription(self, add=(), remove=()) -> None:
        """실행 중인 연결에도 즉시 반영되도록 구독 종목을 동적으로 추가/해지한다.
        연결 전(스레드 시작 전)이면 self.tickers만 갱신해두고, 다음 연결 시 일괄 등록된다."""
        add_norm = {str(t).lstrip("A") for t in add if t}
        remove_norm = {str(t).lstrip("A") for t in remove if t}

        with self._sub_lock:
            to_add = sorted(add_norm - self.tickers)
            to_remove = sorted(remove_norm & self.tickers)
            if not to_add and not to_remove:
                return

            grp_no = None
            if to_add:
                grp_no = str(self._next_grp_no)
                self._next_grp_no += 1
                for ticker in to_add:
                    self._ticker_grp[ticker] = grp_no
                self.tickers |= set(to_add)

            remove_by_grp = {}
            for ticker in to_remove:
                grp = self._ticker_grp.pop(ticker, None)
                if grp:
                    remove_by_grp.setdefault(grp, []).append(ticker)
            self.tickers -= set(to_remove)

            with self.cache_lock:
                for ticker in to_remove:
                    self.quote_cache.pop(ticker, None)

            loop, ws = self._loop, self._ws

        if loop is None or ws is None:
            if to_add:
                print(f"ℹ️ [실시간 시세 구독 대기] 아직 연결 전이라 다음 연결 시 반영됩니다: +{len(to_add)}종목", flush=True)
            return

        if to_add:
            asyncio.run_coroutine_threadsafe(self._send_reg(ws, grp_no, to_add), loop)
        for grp, items in remove_by_grp.items():
            asyncio.run_coroutine_threadsafe(self._send_remove(ws, grp, items), loop)

    @staticmethod
    async def _send_reg(ws, grp_no: str, items: list) -> None:
        try:
            await ws.send(json.dumps({
                "trnm": "REG", "grp_no": grp_no, "refresh": "1",
                "data": [{"item": items, "type": ["0B"]}],
            }))
            print(f"➕ [실시간 시세 구독 추가] {len(items)}종목: {', '.join(items)}", flush=True)
        except Exception as e:
            print(f"⚠️ [실시간 시세 구독 추가 실패] {e}", flush=True)

    @staticmethod
    async def _send_remove(ws, grp_no: str, items: list) -> None:
        try:
            await ws.send(json.dumps({
                "trnm": "REMOVE", "grp_no": grp_no,
                "data": [{"item": items, "type": ["0B"]}],
            }))
            print(f"➖ [실시간 시세 구독 해지] {len(items)}종목: {', '.join(items)}", flush=True)
        except Exception as e:
            print(f"⚠️ [실시간 시세 구독 해지 실패] {e}", flush=True)

    def _run(self) -> None:
        print("ℹ️ [실시간 시세 스레드 실행]", flush=True)
        asyncio.run(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:

        backoff = self.RECONNECT_MIN_SECONDS
        consecutive_failures = 0
        print(f"ℹ️ [실시간 시세 재연결 루프 시작] 초기 백오프: {backoff:.0f}초", flush=True)
        while not self._stop:
            try:
                await self._receive()
                backoff = self.RECONNECT_MIN_SECONDS
                consecutive_failures = 0
            except Exception as error:
                print(f"⚠️ [실시간 시세 연결 종료] {error}", flush=True)
                consecutive_failures += 1
                # 연결이 계속(백오프가 최대치에 도달할 만큼) 실패하면 장중 감시 자체가 안 되고
                # 있다는 뜻이므로 텔레그램으로 알린다. 매번 알리면 도배되니 notifier의 쿨다운에 맡긴다.
                if self.notifier is not None and backoff >= self.RECONNECT_MAX_SECONDS:
                    try:
                        self.notifier.send_error(
                            "quote_stream_reconnect",
                            f"🚨 [실시간 시세 재연결 실패] {consecutive_failures}회 연속 실패 중: {error}",
                        )
                    except Exception:
                        pass
            finally:
                self._connected.clear()
            if self._stop:
                break
            print(f"ℹ️ [실시간 시세 재연결] {backoff:.0f}초 후 재시도", flush=True)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self.RECONNECT_MAX_SECONDS)

    @staticmethod
    def _chunk(items: list, size: int) -> list:
        return [items[i:i + size] for i in range(0, len(items), size)]

    async def _receive(self) -> None:

        print("ℹ️ [실시간 시세 연결 시도]", flush=True)
        async with websockets.connect(self.uri, open_timeout=10) as websocket:

            print("ℹ️ [실시간 시세 웹소켓 연결 성공, 로그인 시도]", flush=True)
            await websocket.send(json.dumps({"trnm": "LOGIN", "token": self.token_provider()}))
            login = json.loads(await asyncio.wait_for(websocket.recv(), timeout=10))
            if login.get("trnm") != "LOGIN" or login.get("return_code") not in (0, "0"):
                print(f"❌ [실시간 시세 로그인 실패] {login}", flush=True)
                return

            print("ℹ️ [실시간 시세 로그인 성공]", flush=True)

            # grp_no는 연결(세션)마다 서버가 새로 관리하므로 재연결 시 이전 배정을 버리고
            # 현재 구독하려는 종목 전체를 다시 그룹으로 나눠 배정한다.
            with self._sub_lock:
                self._ticker_grp = {}
                self._next_grp_no = 1
                tickers_snapshot = sorted(self.tickers)
                chunks = self._chunk(tickers_snapshot, self.REG_CHUNK_SIZE)
                grp_assignments = []
                for chunk in chunks:
                    grp_no = str(self._next_grp_no)
                    self._next_grp_no += 1
                    for ticker in chunk:
                        self._ticker_grp[ticker] = grp_no
                    grp_assignments.append((grp_no, chunk))
                self._ws = websocket
                self._loop = asyncio.get_running_loop()

            try:
                for grp_no, chunk in grp_assignments:
                    await websocket.send(json.dumps({
                        "trnm": "REG",
                        "grp_no": grp_no,
                        "refresh": "1",
                        "data": [{"item": chunk, "type": ["0B"]}],
                    }))

                print(
                    f"✅ [실시간 시세 연결] {len(tickers_snapshot)}종목 구독 완료 "
                    f"({len(grp_assignments)}개 그룹, 그룹당 최대 {self.REG_CHUNK_SIZE}종목)",
                    flush=True,
                )

                self._connected.set()

                while True:
                    message = json.loads(await websocket.recv())
                    if message.get("trnm") == "PING":
                        await websocket.send(json.dumps(message))
                    elif message.get("trnm") == "REG":
                        if message.get("return_code") not in (0, "0", None):
                            print(f"⚠️ [실시간 시세 구독 거절] {message}", flush=True)
                    elif message.get("trnm") == "REMOVE":
                        if message.get("return_code") not in (0, "0", None):
                            print(f"⚠️ [실시간 시세 구독해지 거절] {message}", flush=True)
                    elif message.get("trnm") == "REAL":
                        self._update_quote(message)
                    elif message.get("trnm") in ("CNSRLST", "CNSRREQ"):
                        key = (message["trnm"], self._seq_key(message.get("seq")))
                        waiters = self._condition_waiters.get(key)
                        if waiters:
                            waiter = waiters.pop(0)
                            if not waiter.done():
                                waiter.set_result(message)
                        else:
                            # seq가 다르면(예: 이미 타임아웃되어 포기한 이전 요청의 지연 응답)
                            # 지금 기다리는 요청과 절대 섞이면 안 되므로 조용히 버린다.
                            print(f"ℹ️ [조건검색] 대기 중인 요청이 없는 응답 수신(지연 도착 등으로 무시): {message}", flush=True)
            finally:
                with self._sub_lock:
                    self._ws = None
                    self._loop = None
                # 연결이 끊기면 대기 중이던 조건검색 요청은 더 이상 응답받을 수 없으니 실패 처리한다.
                for waiters in self._condition_waiters.values():
                    for waiter in waiters:
                        if not waiter.done():
                            waiter.set_exception(RuntimeError("실시간 시세 연결이 종료되어 조건검색 응답을 받지 못했습니다."))
                    waiters.clear()

    def _update_quote(self, message: dict) -> None:
        data = message.get("data", [])
        if isinstance(data, dict):
            data = [data]
        for item in data:
            ticker = str(item.get("item") or item.get("stk_cd") or item.get("9001") or "").lstrip("A")
            values = item.get("values", item)
            if not ticker or not isinstance(values, dict):
                continue

            current_price = self._number(values.get("cur_prc", values.get("10")))
            if current_price <= 0:
                continue
            quote = {
                "ticker": ticker,
                "name": values.get("stk_nm", item.get("name", "")),
                "current_price": current_price,
                "open_price": self._number(values.get("open_pric", values.get("16", values.get("18")))),
                "high_price": self._number(values.get("high_pric", values.get("17", values.get("16")))),
                "low_price": self._number(values.get("low_pric", values.get("18", values.get("17")))),
                "volume": self._number(values.get("trde_qty", values.get("13"))),
            }
            with self.cache_lock:
                previous = self.quote_cache.get(ticker, {}).get("quote", {})
                quote["open_price"] = quote["open_price"] or previous.get("open_price", 0)
                quote["high_price"] = quote["high_price"] or previous.get("high_price", 0)
                quote["low_price"] = quote["low_price"] or previous.get("low_price", 0)
                self.quote_cache[ticker] = {"quote": quote, "received_at": time.monotonic()}

    @staticmethod
    def _number(value) -> int:
        if value is None or value == "":
            return 0
        return abs(int(str(value).replace(",", "").replace("+", "").strip()))