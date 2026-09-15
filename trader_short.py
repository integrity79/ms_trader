# trader_short.py
import os
import json
import math
import time
from datetime import datetime, timedelta
import requests
import pandas as pd
import numpy as np

#from dotenv import load_dotenv
#load_dotenv()

from pykrx import stock
from state_store import atomic_write_json

POSITIONS_FILE = "trader_short_positions.json"

class ShortTermTrader:
    strategy_name = "short"

    def __init__(self, client, notifier):
        self.client = client
        self.notifier = notifier
        
        self.capital_ratio = float(os.getenv("SHORT_CAPITAL_RATIO", "0.50"))
        self.max_slots = int(os.getenv("MAX_SLOTS", "4"))
        tranche_str = os.getenv("TRANCHE_WEIGHTS", "0.40,0.30,0.30")
        try:
            self.tranche_weights = [float(w.strip()) for w in tranche_str.split(",") if w.strip()]
        except Exception:
            self.tranche_weights = [0.40, 0.30, 0.30]
        if len(self.tranche_weights) != 3 or any(not math.isfinite(weight) or weight < 0 for weight in self.tranche_weights) or not math.isclose(sum(self.tranche_weights), 1.0, abs_tol=0.000001):
            raise ValueError("TRANCHE_WEIGHTS는 음수가 아닌 3개 값이며 합계가 1.0이어야 합니다.")

        self.target_profit_rate = float(os.getenv("TARGET_PROFIT_RATE", "0.07"))
        self.final_stop_loss_rate = float(os.getenv("FINAL_STOP_LOSS_RATE", "-0.05"))
        self.buy_fee_rate = float(os.getenv("BUY_FEE_RATE", "0.0"))
        self.sell_fee_tax_rate = float(os.getenv("SELL_FEE_TAX_RATE", "0.0023"))
        self.max_holding_days = int(os.getenv("MAX_HOLDING_DAYS", "30"))
        self.universe_size = int(os.getenv("UNIVERSE_SIZE", "100"))
        if not 0 < self.capital_ratio <= 1 or self.max_slots <= 0 or self.universe_size <= 0:
            raise ValueError("단기형 자본비율, 슬롯 수, 유니버스 크기는 유효한 양수여야 합니다.")
        self.positions = self.load_positions()
        self._candidate_histories = {}
        self._candidate_history_date = ""
        # 키움 조건검색식으로 신규 후보를 좁혀서(전체 유니버스 스캔 대신) 대량 호출을 줄인다.
        # 이름으로 설정하면 CNSRLST 목록에서 seq를 찾아 캐시해둔다. 미설정/조회 실패 시에는
        # 기존 방식(fetch_universe 전체 스캔)으로 자동 폴백한다.
        self.condition_name = os.getenv("SHORT_CONDITION_NAME", "").strip()
        self._condition_seq = None
        self._condition_seq_resolved = False

    def load_positions(self) -> dict:
        if os.path.exists(POSITIONS_FILE):
            try:
                with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("positions", {})
            except (OSError, json.JSONDecodeError) as e:
                raise RuntimeError(f"단기형 포지션 파일을 읽을 수 없습니다: {POSITIONS_FILE}") from e
        return {}

    def save_positions(self):
        atomic_write_json(POSITIONS_FILE, {"positions": self.positions})

    @staticmethod
    def apply_live_quote(df: pd.DataFrame, quote: dict) -> pd.DataFrame:
        df = df.copy()
        today = pd.Timestamp(datetime.now().date())
        candle = {
            "시가": quote["open_price"],
            "고가": quote["high_price"],
            "저가": quote["low_price"],
            "종가": quote["current_price"],
            "거래량": quote["volume"],
        }
        if not df.empty and pd.Timestamp(df.index[-1]).normalize() == today:
            for column, value in candle.items():
                df.loc[df.index[-1], column] = value
        else:
            df = pd.concat([df, pd.DataFrame([candle], index=[today])])
        return df.sort_index()

    def get_latest_trading_date(self) -> str:
        today = datetime.now()
        for i in range(10):
            dt_str = (today - timedelta(days=i)).strftime("%Y%m%d")
            try:
                df = self.client.get_daily_ohlcv("005930", dt_str, dt_str)
                if not df.empty and df.iloc[0]["거래량"] > 0:
                    return dt_str
            except Exception:
                pass
        return today.strftime("%Y%m%d")

    def is_trading_day(self, date_str: str) -> bool:
        """휴장일이면 False, 거래일이면 True. 네트워크 오류 등으로 확인 자체에 실패하면
        "휴장"으로 단정하지 않도록 예외를 그대로 올린다 — 호출부가 "확인 실패(재시도 필요)"와
        "확인된 휴장"을 구분해서 처리해야 한다.

        (예전엔 pykrx/네이버로 조회했는데, KRX 로그인 세션을 네이버 조회에도 재사용하는 탓인지
        직전에 요청이 몰린 뒤 바로 이어지는 단발성 조회가 자주 타임아웃 났다. 키움 자체
        일봉 API(ka10081)로 바꿔서 그 문제 자체를 없앴다. 그래도 만일을 대비해 짧게 재시도한다.)"""
        last_error = None
        for attempt in range(3):
            try:
                df = self.client.get_daily_ohlcv("005930", date_str, date_str)
                return df is not None and not df.empty and int(df.iloc[0]["거래량"]) > 0
            except Exception as e:
                last_error = e
                if attempt < 2:
                    time.sleep(3)
        raise last_error

    def fetch_universe(self) -> list:
        dt = self.get_latest_trading_date()
        try:
            df_cap = stock.get_market_cap_by_ticker(dt, market="ALL")
            if df_cap is not None and not df_cap.empty and "시가총액" in df_cap.columns:
                df_cap = df_cap.sort_values(by="시가총액", ascending=False)
                return df_cap.index.tolist()[:self.universe_size]
        except Exception:
            pass
        return stock.get_market_ticker_list(dt, market="KOSPI")[:self.universe_size]

    def get_mid_positions_tickers(self) -> set:
        """중기형 포지션 파일(kiwoom_mid_positions.json)에서 보유 중인 티커 추출"""
        mid_file = "trader_mid_positions.json"
        if os.path.exists(mid_file):
            try:
                import json
                with open(mid_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return set(data.get("positions", {}).keys())
            except Exception:
                return set()
        return set()

    def _resolve_condition_seq(self):
        """SHORT_CONDITION_NAME에 해당하는 조건검색식 seq를 찾아 캐시한다.
        미설정이면 None(조건검색 미사용 모드). 설정했는데 목록 조회/이름 매칭에 실패해도
        None을 반환하지만, 이 경우 호출부는 fetch_universe() 전체 스캔으로 돌아가지 않고
        "후보 없음"으로 처리해야 한다 — 조건검색을 쓰기로 한 이상 실패했다고 대량 조회로
        되돌아가면 조건검색을 쓰는 의미(대량 호출 회피)가 없어진다."""
        if self._condition_seq_resolved:
            return self._condition_seq
        self._condition_seq_resolved = True
        if not self.condition_name:
            return None
        try:
            conditions = self.client.fetch_condition_list()
        except Exception as e:
            print(f"⚠️ [단기형 조건검색] 목록 조회 실패, 이번 회차는 후보 없음으로 처리합니다: {e}", flush=True)
            return None
        for seq, name in conditions:
            if name == self.condition_name:
                self._condition_seq = seq
                print(f"✅ [단기형 조건검색] '{self.condition_name}' 연결됨 (seq={seq})", flush=True)
                return seq
        print(f"⚠️ [단기형 조건검색] '{self.condition_name}' 조건식을 찾지 못했습니다. 이번 회차는 후보 없음으로 처리합니다.", flush=True)
        return None

    def prepare_candidate_history(self):
        today_str = datetime.now().strftime("%Y%m%d")
        if self._candidate_history_date == today_str:
            return

        if self.condition_name:
            # 조건검색식을 쓰기로 한 이상 후보 발견은 항상 조건검색 결과에만 의존하므로
            # (연결 성공/실패와 무관하게) 전체 유니버스를 미리 당겨둘 필요가 없다.
            seq = self._resolve_condition_seq()  # seq를 미리 캐시해서 스캔 시점 지연을 줄인다.
            self._candidate_histories = {}
            self._candidate_history_date = today_str
            print("[단기형 사전 적재] 조건검색 모드라 전체 유니버스 사전 적재를 건너뜁니다.", flush=True)
            # 실제 마감 루틴(15시대) 전에 조건검색이 잘 붙어 있는지, 지금 몇 종목이 잡히는지
            # 바로 확인할 수 있도록 한 번 미리 실행해서 매칭 건수만 보여준다(부트스트랩 시 1회뿐).
            # 마감 루틴 시점엔 scan_new_candidates()가 그때 시세로 다시 실행하므로 결과가 달라질 수 있다.
            if seq:
                try:
                    matches = self.client.run_condition_search(seq, stex_tp="K")
                    print(f"[단기형 조건검색 미리보기] '{self.condition_name}' 현재 매칭 {len(matches)}종목: "
                          f"{', '.join(m['ticker'] for m in matches) or '없음'}", flush=True)
                except Exception as e:
                    print(f"⚠️ [단기형 조건검색 미리보기] 실행 실패(마감 루틴 때 재시도됨): {e}", flush=True)
            return

        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        universe = self.fetch_universe()
        total = len(universe)
        print(f"[단기형 사전 적재] 후보 {total}종목 일봉 조회 시작", flush=True)
        started_at = time.monotonic()
        last_logged_at = started_at

        histories = {}
        for i, ticker in enumerate(universe, start=1):
            try:
                df = self.client.get_daily_ohlcv(ticker, start_date, today_str)
                if df is not None and len(df) >= 30:
                    histories[ticker] = df
            except Exception:
                continue
            now = time.monotonic()
            if now - last_logged_at >= 5.0 or i == total:
                print(f"[단기형 사전 적재] 진행 {i}/{total} ({now - started_at:.0f}초 경과)", flush=True)
                last_logged_at = now

        self._candidate_histories = histories
        self._candidate_history_date = today_str
        print(f"[단기형 사전 적재] 후보 지표용 일봉 {len(histories)}종목 준비 완료 ({time.monotonic() - started_at:.0f}초 소요)", flush=True)

    def scan_new_candidates(self, verbose: bool = False) -> list:
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")
        start_date = (now - timedelta(days=90)).strftime("%Y%m%d")
        candidates = []
        skip_counts = {"이미 보유": 0, "중기형 보유": 0, "이력 부족": 0, "조회 오류": 0}

        # 조건검색식을 설정한 이상 후보는 항상 조건검색 결과에만 의존한다. seq 연결이나
        # 실행 자체가 실패해도 fetch_universe() 전체 스캔으로 되돌아가지 않고 후보 없음으로
        # 처리한다 — 그렇지 않으면 조건검색을 쓰는 이유(대량 KRX/네이버 조회 회피)가 없어진다.
        condition_names = {}
        seq = self._resolve_condition_seq()
        if seq:
            try:
                matches = self.client.run_condition_search(seq, stex_tp="K")
            except Exception as e:
                print(f"⚠️ [단기형 조건검색] 실행 실패, 이번 회차는 후보 없음으로 처리합니다: {e}", flush=True)
                tickers = []
            else:
                tickers = [m["ticker"] for m in matches]
                condition_names = {m["ticker"]: m["name"] for m in matches if m.get("name")}
                print(f"[단기형 조건검색] '{self.condition_name}' 매칭 {len(tickers)}종목", flush=True)
        elif self.condition_name:
            tickers = []
        else:
            tickers = self.fetch_universe()

        # 중기형이 이미 보유 중인 종목 세트 조회
        mid_held_tickers = self.get_mid_positions_tickers()

        for ticker in tickers:
            # 1) 단기형 본인이 이미 보유 중이면 패스
            if ticker in self.positions:
                skip_counts["이미 보유"] += 1
                continue

            # 2) [신규] 중기형이 이미 보유 중인 종목이면 충돌 방지를 위해 패스
            if ticker in mid_held_tickers:
                skip_counts["중기형 보유"] += 1
                if verbose:
                    name = condition_names.get(ticker) or stock.get_market_ticker_name(ticker)
                    print(f"ℹ️ [단기형 스킵] [{ticker} {name}] 중기형 전략에서 이미 보유 중인 종목입니다.", flush=True)
                continue

            try:
                df = self._candidate_histories.get(ticker)
                if df is None:
                    df = self.client.get_daily_ohlcv(ticker, start_date, today_str)
                if df is None or len(df) < 30:
                    skip_counts["이력 부족"] += 1
                    continue

                quote = self.client.get_current_quote(ticker)
                df = self.apply_live_quote(df, quote)

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
                passed = is_oversold and (stoch_gc or tail_support)

                name = condition_names.get(ticker) or stock.get_market_ticker_name(ticker)
                if verbose:
                    print(
                        f"  [{ticker} {name}] RSI={curr['RSI']:.1f} 이격도={curr['이격도_20']:.1f} "
                        f"stoch_gc={stoch_gc} tail_support={tail_support} -> "
                        f"{'✅ 후보 채택' if passed else '❌ 미충족'}",
                        flush=True,
                    )

                if passed:
                    candidates.append({
                        "ticker": ticker,
                        "name": name,
                        "price": quote["current_price"],
                        "rsi": float(curr["RSI"]),
                        "disparity": float(curr["이격도_20"]),
                        "ma20": int(curr["20MA"])
                    })
                time.sleep(0.02)
            except Exception as e:
                skip_counts["조회 오류"] += 1
                if verbose:
                    print(f"  [{ticker}] 조회 오류: {e}", flush=True)
                continue

        if verbose:
            print(f"[단기형 스캔 요약] 대상 {len(tickers)}종목 | 스킵: {skip_counts} | 후보: {len(candidates)}개", flush=True)

        candidates.sort(key=lambda x: x["rsi"])
        return candidates

    def run_intraday_monitoring(self):
        """09:01 ~ 15:09 장중 실시간 감시"""
        if not self.positions:
            return

        # 잔고 조회가 실패해도(네트워크 오류 등) 익절/손절 판단까지 막히면 안 되므로,
        # 여기서 실패하더라도 추가매수 예산(slot_budget)만 이번 주기에 비워두고 계속 진행한다.
        try:
            bal = self.client.get_account_balance()
            total_equity = bal["total_equity"] * self.capital_ratio
            slot_budget = total_equity / self.max_slots if total_equity > 0 else 2_500_000
        except Exception as e:
            print(f"⚠️ [단기형 감시] 잔고 조회 실패로 이번 주기 추가매수는 건너뜁니다: {e}", flush=True)
            self.notifier.send_error("short_balance_check", f"⚠️ [단기형 감시] 잔고 조회 실패로 추가매수를 건너뛰고 있습니다: {e}")
            slot_budget = None

        closed_tickers = []
        for ticker, pos in list(self.positions.items()):
            try:
                quote = self.client.get_current_quote(ticker)
                curr_price = quote["current_price"]
                low_price = quote["low_price"]

                avg_price = float(pos["avg_price"])
                shares = int(pos["shares"])
                tranches = int(pos["tranches"])
                target_p = int(avg_price * (1 + self.target_profit_rate))

                # 목표가 익절
                if curr_price >= target_p:
                    success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, shares, price=0, is_buy=False, strategy=self.strategy_name)
                    if success:
                        real_exit = fill_p
                        proceeds = real_exit * fill_q * (1 - self.sell_fee_tax_rate)
                        cost_basis = avg_price * fill_q
                        pnl = proceeds - cost_basis
                        ret_pct = ((proceeds / cost_basis) - 1.0) * 100
                        msg = (
                            f"★ [단기형 익절 완료] {pos['name']} ({ticker})\n"
                            f"• 체결가: {real_exit:,}원 (목표가 {target_p:,}원 달성)\n"
                            f"• 매도수량: {fill_q}주\n"
                            f"• 실현수익률: {ret_pct:+.2f}% (손익금: {int(pnl):+,}원)"
                        )
                        print(msg, flush=True)
                        self.notifier.send(msg)
                        if fill_q >= shares:
                            closed_tickers.append(ticker)
                        else:
                            pos["shares"] -= fill_q
                            pos["invested"] = pos["shares"] * avg_price
                            self.save_positions()
                        continue

                    continue

                # 2차 추가 매수
                # low_price<=0은 실시간 체결에 저가 필드가 아직 없어 캐시가 비어있는 경우로,
                # 실제 급락이 아니라 데이터 공백이므로 트리거로 오인하면 안 된다.
                trigger_2nd = int(pos["p1"] * 0.95)
                if tranches == 1 and slot_budget is not None and low_price > 0 and low_price <= trigger_2nd:
                    add_budget = slot_budget * self.tranche_weights[1]
                    add_shares = int(add_budget // curr_price) if curr_price > 0 else 0
                    if add_shares > 0:
                        success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, add_shares, price=0, is_buy=True, strategy=self.strategy_name)
                        if success:
                            real_fill = fill_p
                            pos["shares"] += fill_q
                            pos["invested"] += fill_q * real_fill * (1 + self.buy_fee_rate)
                            pos["avg_price"] = pos["invested"] / pos["shares"]
                            pos["tranches"] = 2
                            self.save_positions()
                            msg = (
                                f"● [단기형 2차 추가매수] {pos['name']} ({ticker})\n"
                                f"• 매수가: {real_fill:,}원 (-5% 도달)\n"
                                f"• 추가수량: {fill_q}주 | 갱신 평단가: {int(pos['avg_price']):,}원"
                            )
                            print(msg, flush=True)
                            self.notifier.send(msg)

                # 3차 추가 매수 (low_price<=0 가드는 2차와 동일한 이유)
                trigger_3rd = int(pos["avg_price"] * 0.95)
                if tranches == 2 and slot_budget is not None and low_price > 0 and low_price <= trigger_3rd:
                    add_budget = slot_budget * self.tranche_weights[2]
                    add_shares = int(add_budget // curr_price) if curr_price > 0 else 0
                    if add_shares > 0:
                        success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, add_shares, price=0, is_buy=True, strategy=self.strategy_name)
                        if success:
                            real_fill = fill_p
                            pos["shares"] += fill_q
                            pos["invested"] += fill_q * real_fill * (1 + self.buy_fee_rate)
                            pos["avg_price"] = pos["invested"] / pos["shares"]
                            pos["tranches"] = 3
                            self.save_positions()
                            msg = (
                                f"● [단기형 3차 최종매수] {pos['name']} ({ticker})\n"
                                f"• 매수가: {real_fill:,}원 | 추가수량: {fill_q}주\n"
                                f"• 최종 평단가: {int(pos['avg_price']):,}원"
                            )
                            print(msg, flush=True)
                            self.notifier.send(msg)

            except Exception as e:
                print(f"❌ [단기형 감시 오류] {ticker}: {e}", flush=True)
                self.notifier.send_error(f"short_monitor_error:{ticker}", f"⚠️ [단기형 감시 오류] {ticker}: {e}")

        for t in closed_tickers:
            if t in self.positions:
                del self.positions[t]
        if closed_tickers:
            self.save_positions()
            self.client.update_quote_subscription(remove=closed_tickers)

    def run_closing_routine(self):
        """15:10 장마감 루틴"""
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")
        start_date = (now - timedelta(days=90)).strftime("%Y%m%d")

        print(f"\n[단기형 종가 근접 루틴 가동]", flush=True)
        closed_tickers = []

        # 단기형 포지션 종가 청산 루틴 시작
        for ticker, pos in list(self.positions.items()):
            try:
                df = self.client.get_daily_ohlcv(ticker, start_date, today_str)
                if df.empty or len(df) < 25:
                    continue

                df = self.apply_live_quote(df, self.client.get_current_quote(ticker))

                df["20MA"] = df["종가"].rolling(window=20).mean()
                curr_price = int(df.iloc[-1]["종가"])
                ma20_price = int(df.iloc[-1]["20MA"])
                avg_price = float(pos["avg_price"])
                shares = int(pos["shares"])
                tranches = int(pos["tranches"])
                hold_days = int(pos.get("hold_days", 1))

                hard_stop = int(avg_price * (1 + self.final_stop_loss_rate))
                exit_triggered = False
                reason = ""

                if curr_price >= ma20_price:
                    exit_triggered = True
                    reason = f"20일선 회복 ({curr_price:,}원 >= {ma20_price:,}원)"
                elif tranches == 3 and curr_price <= hard_stop:
                    exit_triggered = True
                    reason = f"3차 매수 후 종가 손절선 이탈 ({curr_price:,}원 <= {hard_stop:,}원)"
                elif hold_days >= self.max_holding_days:
                    exit_triggered = True
                    reason = f"30영업일 보유 기한 만료 ({hold_days}일 경과)"

                if exit_triggered:
                    success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, shares, price=0, is_buy=False, strategy=self.strategy_name)
                    if success:
                        real_exit = fill_p
                        proceeds = real_exit * fill_q * (1 - self.sell_fee_tax_rate)
                        cost_basis = avg_price * fill_q
                        pnl = proceeds - cost_basis
                        ret_pct = ((proceeds / cost_basis) - 1.0) * 100
                        msg = (
                            f"▲ [단기형 장마감 청산] {pos['name']} ({ticker})\n"
                            f"• 사유: {reason}\n"
                            f"• 매도가: {real_exit:,}원 | 실현수익률: {ret_pct:+.2f}% ({int(pnl):+,}원)"
                        )
                        print(msg, flush=True)
                        self.notifier.send(msg)
                        if fill_q >= shares:
                            closed_tickers.append(ticker)
                        else:
                            pos["shares"] -= fill_q
                            pos["invested"] = pos["shares"] * avg_price
                            self.save_positions()
                else:
                    pos["hold_days"] = hold_days + 1
            except Exception as e:
                print(f"❌ [단기형 청산 오류] {ticker}: {e}", flush=True)
                self.notifier.send_error(f"short_closing_error:{ticker}", f"🚨 [단기형 청산 오류] {ticker}: 마감 루틴에서 이 종목의 매도 판단을 건너뛰었습니다: {e}", cooldown_seconds=0)

        for t in closed_tickers:
            if t in self.positions:
                del self.positions[t]
        self.save_positions()
        if closed_tickers:
            self.client.update_quote_subscription(remove=closed_tickers)

        # 신규 매수
        bal = self.client.get_account_balance()
        short_budget = bal["total_equity"] * self.capital_ratio
        slot_budget = short_budget / self.max_slots if short_budget > 0 else 2_500_000

        open_slots = self.max_slots - len(self.positions)
        # verbose=True로 종목별 판단 근거(RSI/이격도/스토캐스틱 등)와 스캔 요약을 그대로 출력한다.
        # test_candidate_scan.py에서 쓰는 것과 동일한 메시지라 실전 로그에서도 왜 채택/탈락했는지 바로 보인다.
        chosen = self.scan_new_candidates(verbose=True)[:open_slots] if open_slots > 0 else []

        for c in chosen:
            ticker = c["ticker"]
            price = c["price"]
            print(f"신규 매수 후보: [{ticker}] {c['name']}, 가격: {price}")

        for c in chosen:
            ticker = c["ticker"]
            price = c["price"]

            b1_budget = slot_budget * self.tranche_weights[0]
            b1_shares = int(b1_budget // price) if price > 0 else 0

            if b1_shares > 0:
                success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, b1_shares, price=0, is_buy=True, strategy=self.strategy_name)
                if success:
                    real_price = fill_p if fill_p > 0 else price
                    real_qty = fill_q if fill_q > 0 else b1_shares
                    self.positions[ticker] = {
                        "name": c["name"],
                        "entry_date": today_str,
                        "hold_days": 0,
                        "p1": real_price,
                        "avg_price": float(real_price * (1 + self.buy_fee_rate)),
                        "shares": real_qty,
                        "invested": real_qty * real_price * (1 + self.buy_fee_rate),
                        "tranches": 1
                    }
                    self.save_positions()
                    self.client.update_quote_subscription(add=[ticker])
                    target_p = int(real_price * (1 + self.target_profit_rate))
                    msg = (
                        f"★ [단기형 1차 신규매수] {c['name']} ({ticker})\n"
                        f"• 체결가: {real_price:,}원 ({real_qty}주)\n"
                        f"• RSI: {c['rsi']:.1f} | 1차 목표가: {target_p:,}원"
                    )
                    print(msg, flush=True)
                    self.notifier.send(msg)

        # 텔레그램 일일 브리핑 발송
        summary = [f"📊 [단기형 일일 브리핑] {today_str}"]
        summary.append(f"• 운용 슬롯: {len(self.positions)}/{self.max_slots}개 (할당자산: {short_budget:,.0f}원)")
        for tk, p in self.positions.items():
            summary.append(f" - {p['name']}: {p.get('tranches', 1)}차 | {p.get('hold_days', 0)}일차 | 평단 {int(p['avg_price']):,}원")
        if not self.positions:
            summary.append("• 보유 종목 없음 (100% 현금 대기)")
        self.notifier.send("\n".join(summary))

    def warmup_krx_session(self):
        try:
            today_str = datetime.now().strftime("%Y%m%d")
            self.client.get_daily_ohlcv("005930", today_str, today_str)
            print("-> [15:08 웜업 완료] 키움 세션 최신화 성공", flush=True)
        except Exception:
            pass

if __name__ == "__main__":

    df = stock.get_market_ohlcv_by_date("20260914", "20260914", "005930")
    print(df.head())

