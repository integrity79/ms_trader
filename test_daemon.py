# test_daemon.py
"""
투트랙 자동매매 데몬의 진단/테스트 실행기.
trader_main.py의 핵심 모듈(설정 로딩, 클라이언트/트레이더 부트스트랩, 실행 상태 관리)을 그대로 공유하되,
루프 동작만 테스트에 맞게 다르게 가져간다:
  - 상시 대기하는 main()과 달리 휴장/매매 비활성/일일 루틴 완료 시 즉시 종료한다.
  - 시간대 게이트 없이 아직 완료되지 않은 일일 루틴을 곧바로 순서대로 실행한다.

핵심 모듈을 수정하면 이 파일과 trader_main.main()이 함께 그 수정 내용을 따라가므로,
여기서 검증이 끝나면 trader_main.py를 건드리지 않고 바로 데몬(main)을 가동해도 된다.

사용법:
  python test_daemon.py
"""
import time
from datetime import datetime

from trader_main import (
    RUN_STATE_FILE,
    load_daemon_config,
    bootstrap_daemon,
    initialize_daemon_run_state,
    initialize_daily_run_state,
    reconcile_positions,
    run_daily_routine_once,
)


def test():
    config = load_daemon_config()

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
                if market_status_date != today_str:
                    try:
                        market_is_open = short_trader.is_trading_day(today_str)
                        market_status_date = today_str  # 확인에 성공했을 때만 당일 상태로 확정한다.
                    except Exception as e:
                        # 네트워크 오류 등 확인 실패는 "확인된 휴장"이 아니므로 종료하지 않고 재시도한다.
                        print(f"⚠️ [거래일 확인 실패] {today_str}: {e} (일시적 오류로 간주, 잠시 후 재시도)", flush=True)
                        time.sleep(5)
                        continue

                if not market_is_open:
                    print(f"ℹ️ [휴장 확인] {today_str}: 매매 테스트를 건너뜁니다.", flush=True)
                    break

                if reconciled_date != today_str:
                    run_state = initialize_daily_run_state(run_state, today_str)
                    try:
                        trading_enabled = reconcile_positions(client, short_trader, mid_trader, notifier)
                        reconciled_date = today_str
                    except Exception as e:
                        print(f"⚠️ [포지션 대사 확인 실패] {today_str}: {e} (일시적 오류로 간주, 잠시 후 재시도)", flush=True)
                        time.sleep(5)
                        continue

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

                # 1) KRX 세션 웜업
                if not is_warmup_done:
                    if run_daily_routine_once(run_state, "warmup", short_trader.warmup_krx_session):
                        time.sleep(5)
                    continue

                # 2) [Track 1] 단기형 마감 근접 루틴 (당일 1회)
                if not is_short_done:
                    if run_daily_routine_once(run_state, "short_closing", short_trader.run_closing_routine):
                        time.sleep(5)
                    continue

                # 3) [Track 2] 중기형 마감 근접 루틴 (당일 1회)
                if not is_mid_done:
                    if run_daily_routine_once(run_state, "mid_closing", mid_trader.run_daily_routine):
                        time.sleep(5)
                    continue

                # 오늘자 일일 루틴(웜업/단기 마감/중기 마감)이 전부 완료 상태로 기록되어 있으면
                # 더 실행할 작업이 없다. 조용히 sleep만 반복하면 멈춘 것처럼 보이므로 명시적으로 알리고 끝낸다.
                print(
                    "✅ [테스트 종료] 오늘자 일일 루틴(웜업/단기 마감/중기 마감)이 이미 모두 완료 상태입니다. "
                    "더 실행할 테스트 작업이 없어 종료합니다.",
                    flush=True,
                )
                print(
                    f"   (다시 처음부터 테스트하려면 {RUN_STATE_FILE}에서 오늘({today_str}) 날짜의 루틴 상태를 초기화하세요.)",
                    flush=True,
                )
                return

            else:
                print(f"ℹ️ [주말 또는 휴장] {today_str}: 매매 루틴을 건너뜁니다.", flush=True)
                break

        except Exception as e:
            print(f"❌ [마스터 데몬 예외] {e}", flush=True)
            break


if __name__ == "__main__":
    test()
