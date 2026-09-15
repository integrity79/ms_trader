import os
import time

from dotenv import load_dotenv

from trader_main import KiwoomSharedClient


def _wait_for_tickers(client, tickers, timeout_seconds):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with client._quote_cache_lock:
            received = {t: client._quote_cache[t] for t in tickers if t in client._quote_cache}
        if len(received) == len(tickers):
            return received
        time.sleep(0.2)
    with client._quote_cache_lock:
        return {t: client._quote_cache[t] for t in tickers if t in client._quote_cache}


def _wait_for_silence(client, ticker, watch_seconds):
    """watch_seconds 동안 해당 종목의 캐시가 갱신되지 않으면 True (=구독 해지가 실제로 먹힘)."""
    with client._quote_cache_lock:
        baseline = client._quote_cache.get(ticker)
    baseline_at = baseline["received_at"] if baseline else None
    deadline = time.monotonic() + watch_seconds
    while time.monotonic() < deadline:
        with client._quote_cache_lock:
            current = client._quote_cache.get(ticker)
        if current is not None and current["received_at"] != baseline_at:
            return False
        time.sleep(0.2)
    return True


def step_initial_subscription(client, tickers, timeout_seconds) -> bool:
    print(f"\n[1/4] 초기 구독 검증: {', '.join(tickers)}", flush=True)
    client.start_quote_stream(tickers)

    if not client._quote_stream.is_connected():
        print("FAIL: 제한시간 내 웹소켓 연결 실패", flush=True)
        return False

    received = _wait_for_tickers(client, tickers, timeout_seconds)
    missing = [t for t in tickers if t not in received]
    if missing:
        print(f"FAIL: {timeout_seconds:.0f}초 내 REAL 시세를 받지 못한 종목: {', '.join(missing)}", flush=True)
        return False

    for ticker in tickers:
        quote = received[ticker]["quote"]
        required = ("current_price", "open_price", "high_price", "low_price")
        if any(quote.get(field, 0) <= 0 for field in required):
            print(f"FAIL: {ticker} 수신 시세가 불완전합니다: {quote}", flush=True)
            return False
        age = time.monotonic() - received[ticker]["received_at"]
        print(
            f"PASS: {ticker} 현재 {quote['current_price']:,}원 | 시가 {quote['open_price']:,}원 | "
            f"고가 {quote['high_price']:,}원 | 저가 {quote['low_price']:,}원 | 수신경과 {age:.1f}초",
            flush=True,
        )
    return True


def step_unsubscribe(client, ticker_to_drop, keep_ticker, watch_seconds) -> bool:
    print(f"\n[2/4] 동적 구독 해지 검증: {ticker_to_drop} 해지, {keep_ticker}는 유지", flush=True)
    client.update_quote_subscription(remove=[ticker_to_drop])

    with client._quote_cache_lock:
        still_cached = ticker_to_drop in client._quote_cache
    if still_cached:
        print(f"FAIL: 해지 직후에도 캐시에 {ticker_to_drop}가 남아있습니다(캐시 정리 누락).", flush=True)
        return False
    print(f"PASS: 해지 직후 {ticker_to_drop} 캐시 즉시 정리됨", flush=True)

    dropped_silent = _wait_for_silence(client, ticker_to_drop, watch_seconds)
    if not dropped_silent:
        print(f"FAIL: 해지했는데도 {ticker_to_drop} 시세가 계속 수신됩니다.", flush=True)
        return False
    print(f"PASS: 해지 후 {watch_seconds:.0f}초간 {ticker_to_drop} 추가 수신 없음", flush=True)

    with client._quote_cache_lock:
        kept = client._quote_cache.get(keep_ticker)
    if kept is None:
        print(f"FAIL: 관련 없는 {keep_ticker} 구독까지 같이 끊긴 것으로 보입니다(캐시가 비었음).", flush=True)
        return False
    print(f"PASS: {keep_ticker}는 해지 대상이 아니므로 계속 구독 중", flush=True)
    return True


def step_resubscribe(client, ticker_to_restore, timeout_seconds) -> bool:
    print(f"\n[3/4] 재구독 검증: {ticker_to_restore} 다시 구독", flush=True)
    client.update_quote_subscription(add=[ticker_to_restore])
    received = _wait_for_tickers(client, [ticker_to_restore], timeout_seconds)
    if ticker_to_restore not in received:
        print(f"FAIL: 재구독 후 {timeout_seconds:.0f}초 내 {ticker_to_restore} 시세를 다시 받지 못했습니다.", flush=True)
        return False
    print(f"PASS: 재구독 후 {ticker_to_restore} 시세 정상 재수신", flush=True)
    return True


def step_stale_cache_fallback(client, ticker, max_age_seconds) -> bool:
    print(f"\n[4/4] 오래된 캐시 강제 재조회(REST 폴백) 검증: {ticker}", flush=True)
    with client._quote_cache_lock:
        cached = client._quote_cache.get(ticker)
    if cached is None:
        print(f"FAIL: {ticker} 캐시가 비어 있어 테스트할 수 없습니다.", flush=True)
        return False

    # received_at을 인위적으로 max_age_seconds보다 오래된 것처럼 만들어,
    # "연결은 살아있지만 이 종목만 갱신이 끊긴" 상황을 재현한다.
    with client._quote_cache_lock:
        client._quote_cache[ticker]["received_at"] -= (max_age_seconds + 2.0)

    quote = client.get_current_quote(ticker)
    if not quote or quote.get("current_price", 0) <= 0:
        print(f"FAIL: 강제 재조회 결과가 비정상입니다: {quote}", flush=True)
        return False

    with client._quote_cache_lock:
        refreshed_at = client._quote_cache[ticker]["received_at"]
    if time.monotonic() - refreshed_at > 1.0:
        print("FAIL: REST 재조회 후에도 캐시 시각이 갱신되지 않았습니다.", flush=True)
        return False

    print(f"PASS: {max_age_seconds:.0f}초 이상 갱신 없던 캐시를 감지해 REST로 재조회하고 캐시를 새로 채움", flush=True)
    return True


def main() -> int:
    load_dotenv()
    tickers = [t.strip() for t in os.getenv("WEBSOCKET_TEST_TICKERS", "005930,000660").split(",") if t.strip()]
    if len(tickers) < 2:
        print("FAIL: WEBSOCKET_TEST_TICKERS에는 최소 2종목이 필요합니다(구독 해지 격리 검증용).", flush=True)
        return 1
    timeout_seconds = max(float(os.getenv("WEBSOCKET_TEST_TIMEOUT_SECONDS", "30")), 1.0)
    watch_seconds = max(float(os.getenv("WEBSOCKET_TEST_WATCH_SECONDS", "6")), 1.0)
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")

    client = KiwoomSharedClient(
        os.getenv("KIWOOM_APP_KEY", ""),
        os.getenv("KIWOOM_SECRET_KEY", ""),
        os.getenv("KIWOOM_ACCOUNT_NO", ""),
        is_mock=is_mock,
    )
    print(f"[실시간 시세 구독 관리 테스트] 대상: {', '.join(tickers)} | 제한시간: {timeout_seconds:.0f}초", flush=True)

    ticker_a, ticker_b = tickers[0], tickers[1]

    ok = step_initial_subscription(client, tickers, timeout_seconds)
    ok = ok and step_unsubscribe(client, ticker_a, ticker_b, watch_seconds)
    ok = ok and step_resubscribe(client, ticker_a, timeout_seconds)
    ok = ok and step_stale_cache_fallback(client, ticker_b, client.quote_stream_max_age_seconds)

    if not ok:
        print("\nFAIL: 실시간 시세 구독 관리 테스트 실패", flush=True)
        return 1

    print("\nPASS: 실시간 시세 구독 관리 테스트 전체 통과. 주문은 제출하지 않았습니다.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
