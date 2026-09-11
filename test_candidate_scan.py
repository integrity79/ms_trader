# test_candidate_scan.py
"""
단기형/중기형 신규 종목 탐색 로직만 단독으로 실행해 보는 진단 스크립트.
실제 주문(매수/매도)은 절대 호출하지 않고, scan_new_candidates()의 판단 과정만 verbose로 출력한다.

사용법:
  python test_candidate_scan.py            # 단기형 + 중기형 모두 스캔
  python test_candidate_scan.py short       # 단기형만 스캔
  python test_candidate_scan.py mid         # 중기형만 스캔
"""
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from trader_main import KiwoomSharedClient, TelegramNotifier
from trader_short import ShortTermTrader
from trader_mid import MidTermTrader


def print_banner(title: str):
    print("\n" + "=" * 65)
    print(f"  {title}")
    print("=" * 65)


def build_client() -> KiwoomSharedClient:
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    account_no = os.getenv("KIWOOM_ACCOUNT_NO", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    if not app_key or not secret_key or not account_no:
        raise RuntimeError("KIWOOM_APP_KEY/KIWOOM_SECRET_KEY/KIWOOM_ACCOUNT_NO 환경변수가 필요합니다.")
    client = KiwoomSharedClient(app_key, secret_key, account_no, is_mock=is_mock)
    if not client.get_token():
        raise RuntimeError("키움 접근 토큰 발급에 실패했습니다. 인증 정보를 확인해 주세요.")
    return client


def run_short_scan(client: KiwoomSharedClient, notifier: TelegramNotifier, trader: ShortTermTrader = None):
    print_banner("[단기형] 후보 종목 탐색 테스트")
    trader = trader or ShortTermTrader(client, notifier)

    universe = trader.fetch_universe()
    print(f"• 유니버스 크기: {len(universe)}종목 (UNIVERSE_SIZE={trader.universe_size})")
    if not universe:
        print("-> [경고] 유니버스 자체가 비어 있습니다. fetch_universe()의 pykrx 응답을 확인하세요.")
        return

    print("• 후보 지표용 일봉 사전 적재 중...")
    trader.prepare_candidate_history()
    print(f"• 사전 적재된 종목 수: {len(trader._candidate_histories)}/{len(universe)}")

    print("• 종목별 스캔 상세:")
    candidates = trader.scan_new_candidates(verbose=True)

    print_banner("[단기형] 스캔 결과")
    if not candidates:
        print("-> 조건을 충족한 후보가 없습니다. (이격도<=92 또는 RSI<=32, 그리고 스토캐스틱 골든크로스/아래꼬리 지지 중 하나 충족 필요)")
    else:
        for c in candidates:
            print(f"  ✅ [{c['ticker']}] {c['name']} 현재가 {c['price']:,}원 | RSI {c['rsi']:.1f} | 이격도 {c['disparity']:.1f}")


def run_mid_scan(client: KiwoomSharedClient, notifier: TelegramNotifier, trader: MidTermTrader = None):
    print_banner("[중기형] 후보 종목 탐색 테스트")
    trader = trader or MidTermTrader(client, notifier)

    print(f"• 유니버스 크기: {len(trader.universe_map)}종목 (KOSPI 시총 상위)")
    if not trader.universe_map:
        print("-> [경고] 유니버스 자체가 비어 있습니다. get_or_update_kospi50()의 pykrx 응답을 확인하세요.")
        return

    print("• 후보 지표용 일봉 사전 적재 중...")
    trader.prepare_candidate_history()
    print(f"• 사전 적재된 종목 수: {len(trader._candidate_histories)}/{len(trader.universe_map)}")

    print("• 종목별 스캔 상세:")
    candidates = trader.scan_new_candidates(verbose=True)

    print_banner("[중기형] 스캔 결과")
    if not candidates:
        print(f"-> 조건을 충족한 후보가 없습니다. (20일 고점 대비 낙폭 -{trader.oversold_drop_rate*100:.0f}% 이하, 양봉 마감 필요)")
    else:
        for c in candidates:
            print(f"  ✅ [{c['ticker']}] {c['name']} 현재가 {c['price']:,}원 | 낙폭 {c['drop_rate']*100:.1f}%")


def main():
    target = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    if target not in ("all", "short", "mid"):
        print("사용법: python test_candidate_scan.py [short|mid]")
        return

    print(f"[진단 시작] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (target={target})")
    client = build_client()
    notifier = TelegramNotifier("", "")  # 텔레그램 발송 없이 조용히 동작

    short_trader = ShortTermTrader(client, notifier) if target in ("all", "short") else None
    mid_trader = MidTermTrader(client, notifier) if target in ("all", "mid") else None

    # 실시간 시세도 실제 운영과 동일한 조건으로 검증하기 위해 대상 유니버스 전체를 구독한다.
    tickers = set()
    if short_trader:
        tickers |= set(short_trader.fetch_universe()) | set(short_trader.positions)
    if mid_trader:
        tickers |= set(mid_trader.universe_map) | set(mid_trader.positions)
    print(f"• 실시간 시세 구독 대상: {len(tickers)}종목")
    client.start_quote_stream(tickers)

    if short_trader:
        run_short_scan(client, notifier, short_trader)
    if mid_trader:
        run_mid_scan(client, notifier, mid_trader)


if __name__ == "__main__":
    main()
