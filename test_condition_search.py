# test_condition_search.py
"""
키움 조건검색식(CNSRLST/CNSRREQ) 단독 테스트 스크립트.
후보 스캔/지표 계산/주문 로직 없이, 조건검색 API 자체(목록 조회 + 1회 실행)만 확인한다.

주의: 짧은 시간에 반복 호출하면 서버 쪽 요청 제한에 걸려 응답이 안 올 수 있다.
      한 번 실행하고 최소 수십 초~몇 분 간격을 두고 다음 조건을 테스트할 것.

사용법:
  python test_condition_search.py                 # 저장된 조건검색식 목록만 조회
  python test_condition_search.py 4                # seq=4 조건검색 1회 실행
  python test_condition_search.py 스윙_눌림목        # 이름으로 조건검색 1회 실행 (목록에서 seq를 찾아 실행)
"""
import os
import sys
import time

from dotenv import load_dotenv

from trader_main import KiwoomSharedClient


def main() -> int:
    load_dotenv()
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")

    client = KiwoomSharedClient(
        os.getenv("KIWOOM_APP_KEY", ""),
        os.getenv("KIWOOM_SECRET_KEY", ""),
        os.getenv("KIWOOM_ACCOUNT_NO", ""),
        is_mock=is_mock,
    )

    # 조건검색은 실시간 시세와 같은 웹소켓 연결을 쓰지만, 이 테스트에서는 종목 구독이
    # 필요 없으므로 빈 목록으로 연결만 연다(다른 종목 시세 트래픽 없이 조건검색만 확인).
    print("[조건검색 테스트] 웹소켓 연결 중...", flush=True)
    client.start_quote_stream([])
    if not client._quote_stream or not client._quote_stream.is_connected():
        print("FAIL: 실시간 시세 연결에 실패해 조건검색을 테스트할 수 없습니다.", flush=True)
        return 1

    print("[조건검색 테스트] 저장된 조건검색식 목록 조회 중...", flush=True)
    try:
        conditions = client.fetch_condition_list()
    except Exception as e:
        print(f"FAIL: 조건검색식 목록 조회 실패: {e}", flush=True)
        return 1

    print(f"PASS: 조건검색식 {len(conditions)}개 확인", flush=True)
    for seq, name in conditions:
        print(f"  seq={seq:>3}  {name}")

    if len(sys.argv) < 2:
        print(
            "\n조건검색을 실행하려면 인자로 seq 또는 이름을 넘기세요. "
            "예) python test_condition_search.py 4",
            flush=True,
        )
        return 0

    target = sys.argv[1].strip()
    seq = target
    if not target.isdigit():
        matched = [s for s, name in conditions if name == target]
        if not matched:
            print(f"FAIL: '{target}' 이름의 조건검색식을 목록에서 찾지 못했습니다.", flush=True)
            return 1
        seq = matched[0]

    name = next((n for s, n in conditions if s == seq), "?")
    print(f"\n[조건검색 테스트] seq={seq} ('{name}') 실행 중... (최대 20초 대기)", flush=True)
    started_at = time.monotonic()
    try:
        results = client.run_condition_search(seq, stex_tp="K")
    except Exception as e:
        print(f"FAIL: 조건검색 실행 실패 ({time.monotonic() - started_at:.1f}초 소요): {e}", flush=True)
        return 1

    print(f"PASS: {time.monotonic() - started_at:.1f}초 만에 응답 수신, 매칭 {len(results)}종목", flush=True)
    for item in results:
        print(f"  [{item['ticker']}] {item['name']}  현재가 {item['current_price']:,}원")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
