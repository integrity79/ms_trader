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

    def __init__(self, token_provider, is_mock: bool, quote_cache: dict, cache_lock):
        host = "mockapi.kiwoom.com" if is_mock else "api.kiwoom.com"
        self.uri = f"wss://{host}:10000/api/dostk/websocket"
        self.token_provider = token_provider
        self.quote_cache = quote_cache
        self.cache_lock = cache_lock
        self.tickers = set()
        self.thread = None
        self._connected = threading.Event()
        self._stop = False

    def is_connected(self) -> bool:
        return self._connected.is_set()

    def start(self, tickers) -> None:

        print(f"ℹ️ [실시간 시세 구독 요청] {len(tickers)}종목 추가", flush=True)
        self.tickers.update(str(ticker).lstrip("A") for ticker in tickers if ticker)

        if self.thread and self.thread.is_alive():
            print("ℹ️ [실시간 시세 스레드 이미 실행 중]", flush=True)
            return

        print("ℹ️ [실시간 시세 스레드 시작]", flush=True)
        self.thread = threading.Thread(target=self._run, name="kiwoom-quote-stream", daemon=True)
        self.thread.start()
        # 최초 연결이 아직 안 됐다면 잠시 대기해 첫 라운드 루틴에서 캐시가 완전히 비는 것을 줄인다.

        print("ℹ️ [실시간 시세 연결 대기]", flush=True)
        self._connected.wait(timeout=self.CONNECT_WAIT_SECONDS)

    def _run(self) -> None:
        print("ℹ️ [실시간 시세 스레드 실행]", flush=True)
        asyncio.run(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:

        backoff = self.RECONNECT_MIN_SECONDS
        print(f"ℹ️ [실시간 시세 재연결 루프 시작] 초기 백오프: {backoff:.0f}초", flush=True)
        while not self._stop:
            try:
                await self._receive()
                backoff = self.RECONNECT_MIN_SECONDS
            except Exception as error:
                print(f"⚠️ [실시간 시세 연결 종료] {error}", flush=True)
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

            chunks = self._chunk(sorted(self.tickers), self.REG_CHUNK_SIZE)
            for group_index, chunk in enumerate(chunks, start=1):
                await websocket.send(json.dumps({
                    "trnm": "REG",
                    "grp_no": str(group_index),
                    "refresh": "1",
                    "data": [{"item": chunk, "type": ["0B"]}],
                }))

            print(
                f"✅ [실시간 시세 연결] {len(self.tickers)}종목 구독 완료 "
                f"({len(chunks)}개 그룹, 그룹당 최대 {self.REG_CHUNK_SIZE}종목)",
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
                elif message.get("trnm") == "REAL":
                    self._update_quote(message)

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