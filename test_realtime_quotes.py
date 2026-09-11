import os
import time

from dotenv import load_dotenv

from trader_main import KiwoomSharedClient


def main() -> int:
    load_dotenv()
    tickers = [ticker.strip() for ticker in os.getenv("WEBSOCKET_TEST_TICKERS", "005930,000660").split(",") if ticker.strip()]
    timeout_seconds = max(float(os.getenv("WEBSOCKET_TEST_TIMEOUT_SECONDS", "30")), 1.0)
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")

    client = KiwoomSharedClient(
        os.getenv("KIWOOM_APP_KEY", ""),
        os.getenv("KIWOOM_SECRET_KEY", ""),
        os.getenv("KIWOOM_ACCOUNT_NO", ""),
        is_mock=is_mock,
    )
    print(f"[WebSocket 시세 테스트] 대상: {', '.join(tickers)} | 제한시간: {timeout_seconds:.0f}초", flush=True)
    client.start_quote_stream(tickers)

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with client._quote_cache_lock:
            received = {
                ticker: client._quote_cache[ticker]
                for ticker in tickers
                if ticker in client._quote_cache
            }
        if len(received) == len(tickers):
            break
        time.sleep(0.2)

    with client._quote_cache_lock:
        received = {
            ticker: client._quote_cache[ticker]
            for ticker in tickers
            if ticker in client._quote_cache
        }

    missing = [ticker for ticker in tickers if ticker not in received]
    if missing:
        print(f"FAIL: {timeout_seconds:.0f}초 내 REAL 시세를 받지 못한 종목: {', '.join(missing)}", flush=True)
        return 1

    for ticker in tickers:
        quote = received[ticker]["quote"]
        age_seconds = time.monotonic() - received[ticker]["received_at"]
        required_prices = ("current_price", "open_price", "high_price", "low_price")
        if any(quote.get(field, 0) <= 0 for field in required_prices):
            print(f"FAIL: {ticker} 수신 시세가 불완전합니다: {quote}", flush=True)
            return 1
        print(
            f"PASS: {ticker} 현재 {quote['current_price']:,}원 | "
            f"시가 {quote['open_price']:,}원 | 고가 {quote['high_price']:,}원 | "
            f"저가 {quote['low_price']:,}원 | 수신경과 {age_seconds:.1f}초",
            flush=True,
        )

    print("PASS: WebSocket REAL 시세 캐시 검증 완료. 주문은 제출하지 않았습니다.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())