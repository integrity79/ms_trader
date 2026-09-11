# MS Trader

키움 REST API와 KRX 일봉 데이터를 사용하는 단기/중기 자동매매 데몬입니다. 기본 설정은 모의투자이며, 실전 주문에는 별도 승인 설정이 필요합니다.

## 구성

- `trader_main.py`: 데몬 스케줄, 키움 API 클라이언트, 주문 안전장치
- `trader_short.py`: 단기 과매도 반등 및 3단 분할매수 전략
- `trader_mid.py`: 중기 과매도 반등 및 5단 분할매수 전략
- `state_store.py`: JSON 상태 파일 원자적 저장
- `realtime_quotes.py`: WebSocket `0B` 체결 시세 수신 및 캐시
- `backtest_short_term.py`, `backtest_mid_term.py`: 전략 검증용 백테스터
- `test_trader_diag.py`: 키움/텔레그램/데이터 연결 진단 도구
- `test_realtime_quotes.py`: 주문 없는 WebSocket 시세 수신 검증 도구

## 전략 개요

단기 전략은 시가총액 상위 종목에서 RSI, 20일 이격도, 스토캐스틱 또는 아래꼬리 조건을 이용해 종가 근접 매수 후보를 찾습니다. 이후 평단 기준 하락 시 최대 3회까지 분할매수하고, 목표수익률, 20일선 회복, 최종 손절, 보유기간 만료 조건으로 청산합니다.

중기 전략은 KOSPI 시가총액 상위 50종목에서 최근 20일 종가 최고값 대비 하락한 양봉 종목을 찾습니다. 반등 조건을 확인해 최대 5회 분할매수하며, 보유기간별 목표수익률과 장기 보유 리밸런싱 규칙을 적용합니다.

신규 후보의 과거 일봉은 14:50 이후 사전 적재합니다. 실제 주문 판단에는 키움 WebSocket `0B` 체결 구독으로 수신한 현재가, 시가, 고가, 저가, 거래량을 우선 반영합니다. WebSocket 시세가 10초 이상 갱신되지 않은 경우에만 키움 `ka10001` 현재가 REST를 폴백으로 사용합니다.

## 실행

가상환경을 활성화한 뒤 데몬을 실행합니다.

```bash
source devenv/bin/activate
python trader_main.py
```

연결 상태만 점검할 때는 다음을 실행합니다. 이 도구의 주문 체결 테스트는 모의 계좌 주문을 낼 수 있으므로 내용을 확인한 뒤 사용합니다.

```bash
python test_trader_diag.py
```

WebSocket `0B` 체결 시세만 확인하고 주문을 내지 않으려면 장중에 다음 테스트를 실행합니다.

```bash
python test_realtime_quotes.py
```

기본 대상은 `.env`의 `WEBSOCKET_TEST_TICKERS=005930,000660`이며, 수신 제한시간은 `WEBSOCKET_TEST_TIMEOUT_SECONDS=30`입니다.
거래 시간 외에는 `REAL` 체결 메시지가 오지 않아 테스트가 실패할 수 있습니다.

## 기본 일정

평일 중 실제 KRX 거래일이 확인된 경우에만 실행합니다.

| 시간 | 작업 |
| --- | --- |
| 09:01-15:07 | 단기 보유 종목 감시 |
| 14:50-14:59 | 단기/중기 후보 일봉 사전 적재 |
| 15:08-15:09 | KRX 데이터 세션 웜업 |
| 15:15-15:17 | 단기 마감 근접 루틴 |
| 15:18-15:19 | 중기 마감 근접 루틴 |

시간은 `.env`의 `SHORT_CLOSING_*`, `MID_CLOSING_*`로 조정할 수 있습니다. 두 구간은 겹치면 시작 시 오류로 중단됩니다.

## 환경 설정

`.env`에 키움 인증 정보와 아래 운용값을 설정합니다. `.env`는 비밀정보를 포함하므로 Git에 추가하거나 공유하지 마십시오.

```dotenv
IS_MOCK=True

MAX_CASH_USAGE_RATE=0.95
MAX_TICKER_EXPOSURE_RATE=0.15
MAX_SINGLE_ORDER_VALUE=10000000
MAX_DAILY_BUY_VALUE=20000000
MAX_DAILY_ORDER_COUNT=20
MAX_DAILY_LOSS_RATE=0.03
MAX_DAILY_LOSS_VALUE=0
TRADING_EMERGENCY_STOP=false

BUY_FEE_RATE=0.0
SELL_FEE_TAX_RATE=0.0023
ORDER_CONFIRM_TIMEOUT_SECONDS=15
ORDER_CONFIRM_POLL_SECONDS=1.5
QUOTE_REQUEST_INTERVAL=0.35
QUOTE_CACHE_TTL_SECONDS=2
QUOTE_RETRY_COUNT=3
QUOTE_RETRY_BACKOFF_SECONDS=1
QUOTE_STREAM_MAX_AGE_SECONDS=10
WEBSOCKET_TEST_TICKERS=005930,000660
WEBSOCKET_TEST_TIMEOUT_SECONDS=30
```

- 금액 설정은 원화입니다. `MAX_SINGLE_ORDER_VALUE=0`, `MAX_DAILY_BUY_VALUE=0`, `MAX_DAILY_LOSS_VALUE=0`은 해당 금액 한도를 비활성화합니다.
- 비율 설정은 소수입니다. 예를 들어 `0.03`은 3%입니다.
- `TRADING_EMERGENCY_STOP=true`이면 모든 자동 주문을 즉시 차단합니다.
- 실전 모드는 `IS_MOCK=False`와 `ENABLE_LIVE_TRADING=CONFIRM`이 모두 필요합니다.
- 실제 수수료 및 세금에 맞춰 `BUY_FEE_RATE`, `SELL_FEE_TAX_RATE`를 조정해야 합니다.
- WebSocket 연결이 종료되거나 시세가 `QUOTE_STREAM_MAX_AGE_SECONDS`보다 오래되면 REST 폴백을 사용합니다. REST `429` 응답은 `QUOTE_RETRY_*` 설정으로 지수 백오프 재시도합니다.

## 주문 안전장치

- 주문 전 전략 장부와 실제 계좌 보유수량을 대사합니다.
- 주문번호 기준으로 `ka10076` 체결 내역을 폴링하고, 실제 체결수량과 가중평균 체결가만 장부에 반영합니다.
- 예수금 사용률, 종목별 총자산 노출, 건별/일별 매수금액, 일별 주문 건수, 일일 손실 한도를 검사합니다.
- 주문 시간 초과나 부분체결 주문은 미체결 상태 파일에 남기고, 후속 자동 주문을 차단합니다.
- 포지션과 실행 상태 파일은 원자적으로 저장합니다.

## 상태 파일과 복구

| 파일 | 용도 |
| --- | --- |
| `trader_short_positions.json` | 단기 전략 포지션 장부 |
| `trader_mid_positions.json` | 중기 전략 포지션 장부 |
| `trader_order_guard.json` | 일일 주문 수, 누적 매수액, 시작 순자산 |
| `trader_pending_orders.json` | 시간 초과 또는 부분체결 주문 |
| `trader_daemon_state.json` | 일별 루틴 시작/완료 상태 |

다음 상황에서는 자동 주문이 차단됩니다.

- 계좌 보유수량과 두 전략 JSON 장부의 수량이 다름
- 같은 종목이 단기와 중기 장부에 동시에 있음
- 미체결 주문이 남아 있음
- 이전 실행에서 마감 루틴이 `started` 상태로 중단됨
- 상태 JSON을 읽거나 저장하지 못함

차단되면 키움 주문/체결내역과 계좌 보유수량을 먼저 확인합니다. 실제 체결분을 전략 장부에 반영하고, 미체결 주문의 최종 상태를 확인한 뒤 상태 파일을 정리합니다. 상태 파일을 삭제해 차단을 우회하면 실제 계좌와 장부가 어긋날 수 있습니다.

## 실전 전 점검

1. `IS_MOCK=True`로 모의투자를 충분히 검증합니다.
2. 부분체결, 미체결, 데몬 강제 종료 후 재시작, API 실패 상황을 점검합니다.
3. 실계좌 기준의 수수료, 세금, 주문 가능 금액, 위험 한도를 `.env`에 설정합니다.
4. `MAX_SINGLE_ORDER_VALUE`, `MAX_DAILY_BUY_VALUE`, `MAX_DAILY_LOSS_VALUE`를 원화 금액으로 명시합니다.
5. 실전 전환 시에만 `IS_MOCK=False` 및 `ENABLE_LIVE_TRADING=CONFIRM`을 설정합니다.

이 프로그램은 자동 주문 도구이며 수익을 보장하지 않습니다. 전략 변경은 백테스터와 모의투자 검증을 함께 수행한 뒤 반영해야 합니다.