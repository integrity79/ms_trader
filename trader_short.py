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
                df = stock.get_market_ohlcv_by_date(dt_str, dt_str, "005930")
                if not df.empty and df.iloc[0]["거래량"] > 0:
                    return dt_str
            except Exception:
                pass
        return today.strftime("%Y%m%d")

    def is_trading_day(self, date_str: str) -> bool:
        try:
            df = stock.get_market_ohlcv_by_date(date_str, date_str, "005930")
            return df is not None and not df.empty and int(df.iloc[0]["거래량"]) > 0
        except Exception as e:
            print(f"⚠️ [거래일 확인 실패] {date_str}: {e}", flush=True)
            return False

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

    def prepare_candidate_history(self):
        today_str = datetime.now().strftime("%Y%m%d")
        if self._candidate_history_date == today_str:
            return

        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")
        histories = {}
        for ticker in self.fetch_universe():
            try:
                df = stock.get_market_ohlcv_by_date(start_date, today_str, ticker)
                if df is not None and len(df) >= 30:
                    histories[ticker] = df
            except Exception:
                continue

        self._candidate_histories = histories
        self._candidate_history_date = today_str
        print(f"[단기형 사전 적재] 후보 지표용 일봉 {len(histories)}종목 준비", flush=True)

    def scan_new_candidates(self, verbose: bool = False) -> list:
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")
        start_date = (now - timedelta(days=90)).strftime("%Y%m%d")
        tickers = self.fetch_universe()
        candidates = []
        skip_counts = {"이미 보유": 0, "중기형 보유": 0, "이력 부족": 0, "조회 오류": 0}

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
                    name = stock.get_market_ticker_name(ticker)
                    print(f"ℹ️ [단기형 스킵] [{ticker} {name}] 중기형 전략에서 이미 보유 중인 종목입니다.", flush=True)
                continue

            try:
                df = self._candidate_histories.get(ticker)
                if df is None:
                    df = stock.get_market_ohlcv_by_date(start_date, today_str, ticker)
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

                if verbose:
                    name = stock.get_market_ticker_name(ticker)
                    print(
                        f"  [{ticker} {name}] RSI={curr['RSI']:.1f} 이격도={curr['이격도_20']:.1f} "
                        f"stoch_gc={stoch_gc} tail_support={tail_support} -> "
                        f"{'✅ 후보 채택' if passed else '❌ 미충족'}",
                        flush=True,
                    )

                if passed:
                    candidates.append({
                        "ticker": ticker,
                        "name": stock.get_market_ticker_name(ticker),
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

        bal = self.client.get_account_balance()
        total_equity = bal["total_equity"] * self.capital_ratio
        slot_budget = total_equity / self.max_slots if total_equity > 0 else 2_500_000

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
                    success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, shares, price=0, is_buy=False)
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
                trigger_2nd = int(pos["p1"] * 0.95)
                if tranches == 1 and low_price <= trigger_2nd:
                    add_budget = slot_budget * self.tranche_weights[1]
                    add_shares = int(add_budget // curr_price) if curr_price > 0 else 0
                    if add_shares > 0:
                        success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, add_shares, price=0, is_buy=True)
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

                # 3차 추가 매수
                trigger_3rd = int(pos["avg_price"] * 0.95)
                if tranches == 2 and low_price <= trigger_3rd:
                    add_budget = slot_budget * self.tranche_weights[2]
                    add_shares = int(add_budget // curr_price) if curr_price > 0 else 0
                    if add_shares > 0:
                        success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, add_shares, price=0, is_buy=True)
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

        for t in closed_tickers:
            if t in self.positions:
                del self.positions[t]
        if closed_tickers:
            self.save_positions()

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
                df = stock.get_market_ohlcv_by_date(start_date, today_str, ticker)
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
                    success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, shares, price=0, is_buy=False)
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

        for t in closed_tickers:
            if t in self.positions:
                del self.positions[t]
        self.save_positions()

        # 신규 매수
        bal = self.client.get_account_balance()
        short_budget = bal["total_equity"] * self.capital_ratio
        slot_budget = short_budget / self.max_slots if short_budget > 0 else 2_500_000

        open_slots = self.max_slots - len(self.positions)
        chosen = self.scan_new_candidates()[:open_slots] if open_slots > 0 else []

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
                success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, b1_shares, price=0, is_buy=True)
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
            stock.get_market_ohlcv_by_date(today_str, today_str, "005930")
            print("-> [15:08 웜업 완료] KRX 세션 최신화 성공", flush=True)
        except Exception:
            pass