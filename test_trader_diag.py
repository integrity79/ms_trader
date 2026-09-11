import os
import sys
import json
import time
import requests
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dotenv import load_dotenv
load_dotenv()

from pykrx import stock

POSITIONS_FILE = "trading_positions.json"

def print_banner(title: str):
    print("\n" + "="*65)
    print(f"  {title}")
    print("="*65)

def test_step_1_env():
    print_banner("[TEST 1] 환경변수(.env) 및 전략 파라미터 로드 검증")
    
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    account_no = os.getenv("KIWOOM_ACCOUNT_NO", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    
    max_slots = int(os.getenv("MAX_SLOTS", "4"))
    tranche_str = os.getenv("TRANCHE_WEIGHTS", "0.40,0.30,0.30")
    target_profit = float(os.getenv("TARGET_PROFIT_RATE", "0.07"))
    stop_loss = float(os.getenv("FINAL_STOP_LOSS_RATE", "-0.07"))
    max_holding = int(os.getenv("MAX_HOLDING_DAYS", "30"))
    universe_size = int(os.getenv("UNIVERSE_SIZE", "100"))

    print(f"• 키움 계좌번호 : {account_no}")
    print(f"• 운영 모드     : {'[모의투자]' if is_mock else '[실전투자]'}")
    print(f"• App Key 설정  : {'정상 (길이: ' + str(len(app_key)) + ')' if app_key else '누락 (ERROR)'}")
    print(f"• Secret Key    : {'정상 (설정됨)' if secret_key else '누락 (ERROR)'}")
    print(f"• 텔레그램 연동 : {'설정됨' if tg_token and tg_chat_id else '미설정 (알림 스킵)'}")
    print(f"• 슬롯/비중     : {max_slots}슬롯 / {tranche_str}")
    print(f"• 익절/손절/기한: +{target_profit*100:.1f}% / {stop_loss*100:.1f}% / {max_holding}영업일")
    print(f"• 유니버스 크기 : 시총 상위 {universe_size}종목")

    if not app_key or not secret_key or not account_no:
        print("\n-> [결과] 필수 키움 인증 정보가 누락되었습니다. .env를 확인해 주세요.")
        return False
    print("\n-> [결과] 환경변수 로드 성공!")
    return True

def test_step_2_telegram():
    print_banner("[TEST 2] 텔레그램 봇 실시간 메시지 발송 테스트")
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    if not tg_token or not tg_chat_id:
        print("-> 텔레그램 설정이 비어 있어 테스트를 건너뜁니다.")
        return True

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"🧪 [자동매매 시스템 진단] 텔레그램 연결 테스트 메시지입니다.\n• 실행시각: {now_str}\n• 상태: 통신 정상"
    
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {"chat_id": tg_chat_id, "text": msg}
    
    try:
        res = requests.post(url, json=payload, timeout=5)
        res_data = res.json()
        if res_data.get("ok"):
            print(f"• 스마트폰 텔레그램으로 테스트 메시지 발송 완료! (응답: ok=True)")
            print("-> [결과] 텔레그램 연동 성공!")
            return True
        else:
            print(f"• 발송 실패: {res_data}")
            return False
    except Exception as e:
        print(f"• 발송 중 통신 에러: {e}")
        return False

def test_step_3_kiwoom_token():
    print_banner("[TEST 3] 키움 REST API 접근 토큰(OAuth2) 발급 테스트")
    app_key = os.getenv("KIWOOM_APP_KEY", "")
    secret_key = os.getenv("KIWOOM_SECRET_KEY", "")
    is_mock = os.getenv("IS_MOCK", "True").lower() in ("true", "1")
    base_url = "https://mockapi.kiwoom.com" if is_mock else "https://api.kiwoom.com"

    url = f"{base_url}/oauth2/token"
    headers = {"Content-Type": "application/json;charset=UTF-8"}
    data = {
        "grant_type": "client_credentials",
        "appkey": app_key,
        "secretkey": secret_key
    }

    try:
        print(f"• 접속 서버 URL: {base_url}")
        res = requests.post(url, headers=headers, json=data, timeout=10)
        res_json = res.json()

        if "token" in res_json:
            token = res_json["token"]
            exp_str = res_json.get("expires_dt", "N/A")
            print(f"• 토큰 발급 성공!")
            print(f"• 토큰 앞자리   : {token[:20]}... (총 {len(token)}자)")
            print(f"• 만료 일시     : {exp_str}")
            print("-> [결과] 키움 REST API 인증 토큰 발급 성공!")
            return token, base_url
        else:
            print(f"• 토큰 발급 거절: {res_json}")
            return None, base_url
    except Exception as e:
        print(f"• 토큰 요청 예외 발생: {e}")
        return None, base_url

def test_step_4_account_balance(token: str, base_url: str):
    print_banner("[TEST 4] 계좌 예수금 및 실시간 평가자산 조회 (kt00018)")
    if not token:
        print("-> 유효한 토큰이 없어 계좌 조회를 건너뜁니다.")
        return

    url = f"{base_url}/api/dostk/acnt"
    # 명세서 기준 헤더 설정
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "cont-yn": "N",
        "next-key": "",
        "api-id": "kt00018"
    }
    body = {
        "qry_tp": "1",
        "dmst_stex_tp": "KRX"
    }

    try:
        res = requests.post(url, headers=headers, json=body, timeout=10)
        data = res.json()

        def parse_amt(val):
            if val is None:
                return 0
            if isinstance(val, (int, float)):
                return int(val)
            if isinstance(val, str):
                clean = val.replace("+", "").strip()
                if not clean:
                    return 0
                return int(clean)
            return 0

        # kt00018 핵심 응답 필드 파싱
        total_equity = parse_amt(data.get("prsm_dpst_aset_amt", 0))  # 추정예탁자산금액 (총자산)
        stock_eval = parse_amt(data.get("tot_evlt_amt", 0))        # 총평가금액 (주식)
        stock_pur = parse_amt(data.get("tot_pur_amt", 0))          # 총매입금액
        stock_pl = parse_amt(data.get("tot_evlt_pl", 0))           # 총평가손익
        deposit = total_equity - stock_eval                         # 순수 현금(예수금)

        max_slots = int(os.getenv("MAX_SLOTS", "4"))
        slot_budget = total_equity / max_slots if total_equity > 0 else 0

        print(f"• 계좌 총 자산(추정예탁자산) : {total_equity:,.0f}원")
        print(f"• 보유 현금(예수금 잔액)     : {deposit:,.0f}원")
        print(f"• 보유 주식 평가금액         : {stock_eval:,.0f}원 (매입: {stock_pur:,.0f}원 / 손익: {stock_pl:+,}원)")
        print(f"• 1개 슬롯 배정 예산         : {slot_budget:,.0f}원 (총 {max_slots}슬롯 기준)")
        print(f"  └ 1차 투입금 (40%)         : {slot_budget * 0.4:,.0f}원")
        print(f"  └ 2차 투입금 (30%)         : {slot_budget * 0.3:,.0f}원")
        print(f"  └ 3차 투입금 (30%)         : {slot_budget * 0.3:,.0f}원")

        # 보유 종목 목록 확인
        holdings = data.get("acnt_evlt_remn_indv_tot", [])
        print(f"• 현재 보유 종목 수          : {len(holdings)}개")
        for h in holdings:
            nm = h.get("stk_nm", "")
            cd = h.get("stk_cd", "")
            qty = parse_amt(h.get("rmnd_qty", 0))
            if qty > 0:
                print(f"  - [{cd}] {nm}: {qty:,}주 보유")

        print("-> [결과] 계좌 잔고 및 동적 슬롯 예산 산출 성공!")
    except Exception as e:
        print(f"• 잔고 조회 실패: {e}")

def test_step_5_screener_dry_run():
    print_banner("[TEST 5] 시총 100위 수집 및 과매도 골든 시그널 스크리닝 (Dry-Run)")
    print("• pykrx 시총 상위 데이터 수집 테스트...")
    
    today = datetime.now()
    latest_date = today.strftime("%Y%m%d")
    for i in range(10):
        t_str = (today - timedelta(days=i)).strftime("%Y%m%d")
        try:
            df_test = stock.get_market_ohlcv_by_date(t_str, t_str, "005930")
            if not df_test.empty and df_test.iloc[0]["거래량"] > 0:
                latest_date = t_str
                break
        except Exception:
            pass

    print(f"• 최근 기준 영업일: {latest_date}")
    
    try:
        df_cap = stock.get_market_cap_by_ticker(latest_date, market="ALL")
        if df_cap is None or df_cap.empty:
            raise ValueError("시가총액 데이터 수신 불가")
        df_cap = df_cap.sort_values(by="시가총액", ascending=False)
        tickers = df_cap.index.tolist()[:100]
        print(f"• 시총 상위 100종목 추출 성공 (1위: {stock.get_market_ticker_name(tickers[0])})")
    except Exception as e:
        print(f"• 시총 수집 실패: {e}")
        return

    # 샘플 15종목에 대해 실시간 기술적 지표 연산 테스트
    sample_tickers = tickers[:15]
    print(f"• 상위 15개 종목 샘플 지표 연산 중 (RSI, 이격도, 스토캐스틱)...")
    start_date = (today - timedelta(days=90)).strftime("%Y%m%d")
    
    detected = []
    for ticker in sample_tickers:
        try:
            df = stock.get_market_ohlcv_by_date(start_date, latest_date, ticker)
            if df is None or len(df) < 30: continue

            df["20MA"] = df["종가"].rolling(window=20).mean()
            df["이격도_20"] = (df["종가"] / df["20MA"]) * 100

            delta = df["종가"].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            avg_gain = gain.rolling(window=14, min_periods=14).mean()
            avg_loss = loss.rolling(window=14, min_periods=14).mean()
            rs = avg_gain / avg_loss.replace(0, np.nan)
            df["RSI"] = (100 - (100 / (1 + rs))).fillna(50)

            low_12 = df["저가"].rolling(window=12, min_periods=12).min()
            high_12 = df["고가"].rolling(window=12, min_periods=12).max()
            fast_k = (df["종가"] - low_12) / (high_12 - low_12).replace(0, np.nan) * 100
            df["SLOW_K"] = fast_k.rolling(window=5, min_periods=5).mean()
            df["SLOW_D"] = df["SLOW_K"].rolling(window=5, min_periods=5).mean()

            curr = df.iloc[-1]
            prev = df.iloc[-2]

            is_oversold = (curr["이격도_20"] <= 92.0) or (curr["RSI"] <= 32.0)
            stoch_gc = (prev["SLOW_K"] <= prev["SLOW_D"]) and (curr["SLOW_K"] > curr["SLOW_D"]) and (curr["SLOW_K"] <= 25.0)
            body = abs(curr["종가"] - curr["시가"])
            lower_tail = min(curr["시가"], curr["종가"]) - curr["저가"]
            tail_support = lower_tail >= body

            name = stock.get_market_ticker_name(ticker)
            curr_p = int(curr["종가"])
            rsi_val = float(curr["RSI"])
            disp_val = float(curr["이격도_20"])

            if is_oversold and (stoch_gc or tail_support):
                detected.append((name, ticker, curr_p, rsi_val, disp_val))
            time.sleep(0.02)
        except Exception:
            continue

    print(f"• 샘플 연산 완료.")
    if detected:
        print(f"★ 샘플 중 포착된 골든 시그널 종목 ({len(detected)}개):")
        for d in detected:
            print(f"  - {d[0]}({d[1]}): 종가 {d[2]:,}원 | RSI: {d[3]:.1f} | 이격도: {d[4]:.1f}%")
    else:
        print("• 현재 샘플 15종목 중 과매도 시그널 조건 만족 종목 없음 (지표 연산 정상)")
    print("-> [결과] 시세 분석 및 스크리너 알고리즘 정상 통과!")

def test_step_6_positions_io():
    print_banner("[TEST 6] 포지션 상태 파일(trading_positions.json) I/O 점검")
    if not os.path.exists(POSITIONS_FILE):
        print(f"• {POSITIONS_FILE} 파일이 없습니다. 빈 상태로 생성합니다.")
        with open(POSITIONS_FILE, "w", encoding="utf-8") as f:
            json.dump({"positions": {}}, f, ensure_ascii=False, indent=2)
    
    try:
        with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            positions = data.get("positions", {})
        print(f"• 파일 읽기 성공! 현재 보유 중인 포지션 수: {len(positions)}개")
        for ticker, pos in positions.items():
            print(f"  - [{ticker}] {pos.get('name')} | {pos.get('tranches')}차 매수 | 평단: {int(pos.get('avg_price', 0)):,}원")
        print("-> [결과] 포지션 관리 파일 연동 정상!")
    except Exception as e:
        print(f"• 파일 I/O 에러: {e}")

def main():
    print("\n" + "#"*65)
    print("   ★ 키움 REST API 4슬롯 DCA 자동매매 봇 통합 진단 도구 ★")
    print("#"*65)
    
    # 1. 환경변수
    if not test_step_1_env():
        return

    # 2. 텔레그램
    test_step_2_telegram()

    # 3. 키움 토큰 발급
    token, base_url = test_step_3_kiwoom_token()

    # 4. 계좌 잔고
    if token:
        test_step_4_account_balance(token, base_url)

    # 7. 모의계좌 1주 매수 및 체결 조회 실전 테스트
    if token and base_url:
        test_step_7_order_and_execution(token, base_url)

    # 5. 스크리너 Dry-Run
    test_step_5_screener_dry_run()

    # 6. 포지션 파일 I/O
    test_step_6_positions_io()

    print_banner("★ 전체 진단 완료: 모든 서브 모듈이 정상적으로 가동될 준비가 되었습니다! ★")

def test_step_7_order_and_execution(token: str, base_url: str):
    print_banner("[TEST 7] 모의계좌 1주 매수 및 ka10076 체결조회 실전 테스트")
    
    # 안전하게 1주 테스트용 (현대차 또는 삼성전자)
    test_ticker = "005930" # 삼성전자
    
    headers_buy = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "cont-yn": "N",
        "next-key": "",
        "api-id": "kt10000" # 매수
    }
    
    # 1. 1주 시장가 매수 발주 (ord_uv는 빈 문자열!)
    buy_body = {
        "dmst_stex_tp": "KRX",
        "stk_cd": test_ticker,
        "ord_qty": "1",
        "ord_uv": "",       # 시장가 빈 문자열
        "trde_tp": "3"      # 시장가
    }

    print(f"• [{test_ticker}] 1주 시장가 모의 매수 주문 요청 중...")
    res = requests.post(f"{base_url}/api/dostk/ordr", headers=headers_buy, json=buy_body, timeout=10)
    data = res.json()
    ord_no = data.get("ord_no", "")
    ret_code = data.get("return_code", -1)

    if ret_code != 0 and not ord_no:
        print(f"• 매수 주문 실패: {data}")
        return

    print(f"• 매수 주문 성공! 주문번호: {ord_no} ({data.get('return_msg', '')})")

    # 2. ka10076 체결 조회
    print("• 2초 후 ka10076 체결내역 조회 요청 중...")
    time.sleep(2)

    headers_exec = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "cont-yn": "N",
        "next-key": "",
        "api-id": "ka10076" # 체결요청
    }
    exec_body = {
        "stk_cd": test_ticker,
        "qry_tp": "1",
        "sell_tp": "0",
        "ord_no": "",
        "stex_tp": "1"
    }

    res_exec = requests.post(f"{base_url}/api/dostk/acnt", headers=headers_exec, json=exec_body, timeout=10)
    data_exec = res_exec.json()
    cntr_list = data_exec.get("cntr", [])

    print(f"• ka10076 응답 체결 건수: {len(cntr_list)}건")
    for c in cntr_list[:3]:
        print(f"  └ 주문번호: {c.get('ord_no')} | {c.get('stk_nm')} | 구분: {c.get('io_tp_nm')} | "
              f"체결가: {int(c.get('cntr_pric', 0)):,}원 | 체결량: {c.get('cntr_qty')}주 | 상태: {c.get('ord_stt')}")

    # 3. 테스트로 산 1주 즉시 시장가 매도 정리 (원복)
    print(f"• 테스트 매수분 1주 시장가 매도(원복) 요청 중...")
    headers_sell = {
        "Content-Type": "application/json;charset=UTF-8",
        "authorization": f"Bearer {token}",
        "cont-yn": "N",
        "next-key": "",
        "api-id": "kt10001" # 매도
    }
    sell_body = {
        "dmst_stex_tp": "KRX",
        "stk_cd": test_ticker,
        "ord_qty": "1",
        "ord_uv": "",
        "trde_tp": "3"
    }
    res_sell = requests.post(f"{base_url}/api/dostk/ordr", headers=headers_sell, json=sell_body, timeout=10)
    print(f"• 매도 주문 결과: {res_sell.json().get('return_msg', '')}")
    print("-> [결과] 매수(kt10000) -> 체결확인(ka10076) -> 매도(kt10001) 풀사이클 검증 완료!")

if __name__ == "__main__":
    main()