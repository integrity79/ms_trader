import os
import json
import time
from datetime import datetime
import pandas as pd
import numpy as np

from dotenv import load_dotenv
load_dotenv()

from pykrx import stock

UNIVERSE_FILE = "kospi50_universe.json"


def load_kospi50_map() -> dict:
    try:
        with open(UNIVERSE_FILE, "r", encoding="utf-8") as file:
            ticker_map = json.load(file).get("tickers", {})
        if not isinstance(ticker_map, dict) or not ticker_map:
            raise ValueError("tickers 항목이 비어 있거나 올바른 객체가 아닙니다.")
        return ticker_map
    except (OSError, json.JSONDecodeError, ValueError) as error:
        raise RuntimeError(f"KOSPI 유니버스 파일을 읽을 수 없습니다: {UNIVERSE_FILE}") from error

class AdaptiveReversalDCABacktester:
    def __init__(
        self,
        start_date="20210101",
        end_date="20260831",
        initial_capital=20_000_000,
        max_slots=7,
        max_tranches=5,
        oversold_drop_rate=0.10,
        tranche_drop_threshold=0.09,
        rebound_trigger_rate=0.03,
        # -------------------------------------------------------------
        # [외부 설정 파라미터] 보유 기간별 목표 수익률 & 리밸런싱 일수
        # -------------------------------------------------------------
        early_holding_days=60,         # 1단계 기본 구간 (영업일)
        early_target_rate=0.10,        # 1단계 목표가 (+10.0%)
        mid_holding_days=120,          # 2단계 중간 구간 (영업일)
        mid_target_rate=0.03,          # 2단계 목표가 (+3.0%)
        late_target_rate=0.005,        # 3단계 장기 구간 목표가 (+0.5% 본전 탈출)
        rebalance_days=120,            # 5차 풀매수 후 리밸런싱(50% 손절) 기준일
        cache_dir="./daily_market_cache"
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_slots = max_slots
        self.max_tranches = max_tranches
        self.oversold_drop_rate = oversold_drop_rate
        self.tranche_drop_threshold = tranche_drop_threshold
        self.rebound_trigger_rate = rebound_trigger_rate

        self.early_holding_days = early_holding_days
        self.early_target_rate = early_target_rate
        self.mid_holding_days = mid_holding_days
        self.mid_target_rate = mid_target_rate
        self.late_target_rate = late_target_rate
        self.rebalance_days = rebalance_days

        self.fee_tax_rate = 0.0023
        self.cache_dir = cache_dir
        self.ticker_name_map = load_kospi50_map()

    def get_kospi50_universe(self):
        return list(self.ticker_name_map)

    def get_trading_dates(self):
        df = stock.get_market_ohlcv_by_date(self.start_date, self.end_date, "005930")
        return [d.strftime("%Y%m%d") for d in df.index]

    def get_daily_market_df(self, date_str):
        cache_path = os.path.join(self.cache_dir, f"market_{date_str}.parquet")
        if os.path.exists(cache_path):
            try:
                return pd.read_parquet(cache_path)
            except Exception:
                pass
        return pd.DataFrame()

    def run_simulation(self):
        trading_dates = self.get_trading_dates()
        kospi50_set = set(self.get_kospi50_universe())
        print(f"[1/2] 적응형 탈출/리밸런싱 5단 DCA 시뮬레이션 시작... (총 {len(trading_dates)}영업일, {self.max_slots}슬롯)")

        cash = float(self.initial_capital)
        positions = {}
        trade_logs = []
        daily_equity_logs = []
        recent_close_history = {}

        for idx, curr_date in enumerate(trading_dates):
            df_day = self.get_daily_market_df(curr_date)
            if df_day.empty:
                continue

            for ticker in kospi50_set:
                if ticker in df_day.index:
                    if ticker not in recent_close_history:
                        recent_close_history[ticker] = []
                    recent_close_history[ticker].append(df_day.loc[ticker, "종가"])
                    if len(recent_close_history[ticker]) > 20:
                        recent_close_history[ticker].pop(0)

            # -------------------------------------------------------------
            # STEP 1: 보유 포지션 관리 (시간 경과형 목표가 & 리밸런싱)
            # -------------------------------------------------------------
            closed_tickers = []
            for ticker, pos in positions.items():
                if ticker not in df_day.index:
                    pos["hold_days"] += 1
                    continue

                candle = df_day.loc[ticker]
                pos["hold_days"] += 1
                avg_p = pos["avg_price"]
                slot_budget = pos["slot_budget"]

                # 1) 외부 세팅값을 기반으로 동적 목표 수익률 적용
                if pos["hold_days"] <= self.early_holding_days:
                    current_target_rate = self.early_target_rate
                elif pos["hold_days"] <= self.mid_holding_days:
                    current_target_rate = self.mid_target_rate
                else:
                    current_target_rate = self.late_target_rate

                target_p = avg_p * (1 + current_target_rate)

                # 목표가 도달 시 전량 청산
                if candle["고가"] >= target_p:
                    proceeds = (pos["shares"] * target_p) * (1.0 - self.fee_tax_rate)
                    pnl = proceeds - pos["invested"]
                    ret = (proceeds / pos["invested"]) - 1.0
                    cash += proceeds

                    exit_reason = "TARGET_PROFIT" if pos["hold_days"] <= self.early_holding_days else ("MID_EXIT" if pos["hold_days"] <= self.mid_holding_days else "TIME_BREAKEVEN")
                    trade_logs.append({
                        "ticker": ticker,
                        "name": self.ticker_name_map.get(ticker, ticker),
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": pos["tranches"],
                        "invested": pos["invested"],
                        "proceeds": proceeds,
                        "pnl": pnl,
                        "return": ret,
                        "reason": exit_reason
                    })
                    closed_tickers.append(ticker)
                    continue

                # 2) 5차 풀매수 상태에서 지정 일수(rebalance_days) 경과 시: 50% 강제 리밸런싱
                if pos["tranches"] >= self.max_tranches and pos["hold_days"] >= self.rebalance_days and not pos.get("rebalanced", False):
                    sell_shares = pos["shares"] * 0.5
                    sell_price = candle["종가"]
                    recovered_cash = (sell_shares * sell_price) * (1.0 - self.fee_tax_rate)
                    
                    realized_loss_portion = (pos["invested"] * 0.5) - recovered_cash
                    cash += recovered_cash
                    
                    pos["shares"] -= sell_shares
                    pos["invested"] = pos["invested"] * 0.5
                    pos["avg_price"] = pos["invested"] / pos["shares"]
                    pos["rebalanced"] = True
                    pos["rebalance_budget"] = recovered_cash
                    pos["local_low"] = sell_price
                    pos["tranches"] = 4  # 1회 추가 매수 슬롯 재확보

                    trade_logs.append({
                        "ticker": ticker,
                        "name": self.ticker_name_map.get(ticker, ticker),
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": 5,
                        "invested": pos["invested"] * 2.0,
                        "proceeds": recovered_cash,
                        "pnl": -realized_loss_portion,
                        "return": -realized_loss_portion / (pos["invested"] * 2.0),
                        "reason": "REBALANCE_HALF_CUT"
                    })

                # 3) 턴어라운드 추가 매수
                if pos["tranches"] < self.max_tranches:
                    curr_low = candle["저가"]
                    curr_close = candle["종가"]

                    if curr_low < pos["local_low"]:
                        pos["local_low"] = curr_low

                    is_deep_enough = (pos["local_low"] <= avg_p * (1 - self.tranche_drop_threshold))
                    rebound_price = pos["local_low"] * (1 + self.rebound_trigger_rate)
                    is_rebounding = (curr_close >= rebound_price) and (candle["종가"] > candle["시가"])

                    if is_deep_enough and is_rebounding:
                        tranche_budget = pos.get("rebalance_budget", slot_budget / self.max_tranches)
                        fill_p = curr_close
                        if cash >= tranche_budget and fill_p > 0:
                            add_shares = tranche_budget / fill_p
                            cash -= tranche_budget
                            pos["shares"] += add_shares
                            pos["invested"] += tranche_budget
                            pos["avg_price"] = pos["invested"] / pos["shares"]
                            pos["tranches"] += 1
                            pos["local_low"] = fill_p
                            if "rebalance_budget" in pos:
                                del pos["rebalance_budget"]

            for t in closed_tickers:
                del positions[t]

            # -------------------------------------------------------------
            # STEP 2: 신규 1차 매수 진입
            # -------------------------------------------------------------
            open_slots = self.max_slots - len(positions)
            if open_slots > 0:
                candidates = []
                for ticker in kospi50_set:
                    if ticker in positions or ticker not in df_day.index:
                        continue
                    history = recent_close_history.get(ticker, [])
                    if len(history) >= 20:
                        high_20 = max(history)
                        curr_close = df_day.loc[ticker, "종가"]
                        drop_rate = (curr_close - high_20) / high_20
                        if drop_rate <= -self.oversold_drop_rate and df_day.loc[ticker, "시가"] > 0:
                            is_bull = df_day.loc[ticker, "종가"] >= df_day.loc[ticker, "시가"]
                            candidates.append((ticker, curr_close, drop_rate, is_bull))

                candidates.sort(key=lambda x: (x[3], -x[2]), reverse=True)
                chosen = candidates[:open_slots]

                pos_val = sum(p["shares"] * df_day.loc[tk]["종가"] for tk, p in positions.items() if tk in df_day.index)
                current_equity = cash + pos_val
                slot_budget = current_equity / self.max_slots
                tranche_budget = slot_budget / self.max_tranches

                for ticker, close_p, _, _ in chosen:
                    if cash >= tranche_budget and close_p > 0:
                        shares = tranche_budget / close_p
                        cash -= tranche_budget
                        positions[ticker] = {
                            "entry_date": curr_date,
                            "hold_days": 0,
                            "shares": shares,
                            "invested": tranche_budget,
                            "avg_price": close_p,
                            "tranches": 1,
                            "slot_budget": slot_budget,
                            "local_low": close_p,
                            "rebalanced": False
                        }

            # -------------------------------------------------------------
            # STEP 3: 일자별 순자산(NAV) 기록
            # -------------------------------------------------------------
            cur_pos_val = sum(p["shares"] * df_day.loc[tk]["종가"] for tk, p in positions.items() if tk in df_day.index)
            daily_equity_logs.append({
                "date": curr_date,
                "equity": cash + cur_pos_val,
                "open_positions": len(positions)
            })

            if (idx + 1) % 300 == 0 or (idx + 1) == len(trading_dates):
                print(f" -> 진행도: {idx + 1}/{len(trading_dates)}영업일 완료 (현재 보유 종목: {len(positions)}개)")

        return pd.DataFrame(trade_logs), pd.DataFrame(daily_equity_logs), positions

    def print_performance(self, df_trades, df_daily, remaining_positions):
        if df_daily.empty:
            return

        df_daily["date_dt"] = pd.to_datetime(df_daily["date"])
        df_daily["year"] = df_daily["date_dt"].dt.year
        if not df_trades.empty:
            df_trades["exit_dt"] = pd.to_datetime(df_trades["exit_date"])
            df_trades["year"] = df_trades["exit_dt"].dt.year

        final_equity = df_daily["equity"].iloc[-1]
        total_account_return = ((final_equity / self.initial_capital) - 1.0) * 100
        total_days = len(df_daily)
        total_years = max(total_days / 245.0, 0.1)
        cagr = ((final_equity / self.initial_capital) ** (1.0 / total_years) - 1.0) * 100

        df_daily["peak"] = df_daily["equity"].cummax()
        df_daily["drawdown"] = (df_daily["equity"] - df_daily["peak"]) / df_daily["peak"] * 100
        real_mdd = df_daily["drawdown"].min()

        total_trades = len(df_trades)
        wins = df_trades[df_trades["pnl"] > 0]
        losses = df_trades[df_trades["pnl"] <= 0]
        win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0
        pf = abs(wins["pnl"].sum() / losses["pnl"].sum()) if not losses.empty and losses["pnl"].sum() != 0 else 99.9

        print("\n" + "="*70)
        print(f"  [{self.start_date[:4]}~{self.end_date[:4]}] 적응형 탈출/리밸런싱 5단 DCA 성과")
        print("="*70)
        print(f"테스트 기간           : {self.start_date} ~ {self.end_date} (약 {total_years:.1f}년)")
        print(f"초기 투자 원금        : {self.initial_capital:,.0f}원")
        print(f"최종 계좌 평가액      : {final_equity:,.0f}원")
        print(f"계좌 총 누적 수익률   : {total_account_return:+.2f}%")
        print(f"연평균 복리 수익률(CAGR): {cagr:+.2f}%")
        print(f"통합 최대 낙폭 (MDD)  : {real_mdd:.2f}%")
        print("-"*70)
        print(f"총 거래 건수          : {total_trades}회")
        print(f"실현 승률 (Win Rate)  : {win_rate:.2f}%")
        print(f"수익 팩터 (Profit Factor): {pf:.2f}")
        print(f"평균 보유 영업일       : {df_trades['hold_days'].mean():.1f}일")
        print(f"미청산 보유 종목 수   : {len(remaining_positions)}개 / {self.max_slots}슬롯")
        if remaining_positions:
            print(" • 백테스트 종료 시점 미청산 종목 현황:")
            for tk, pos in remaining_positions.items():
                name = self.ticker_name_map.get(tk, tk)
                print(f"   - [{tk} {name}] 진행 차수: {pos['tranches']}/5차 | 보유일: {pos['hold_days']}일 | 투자금: {pos['invested']:,.0f}원")
        print("-"*70)
        print("청산 사유별 분석:")
        for reason, count in df_trades["reason"].value_counts().items():
            print(f" • {reason:<22}: {count:>4}건 ({count/total_trades*100:5.1f}%)")
        print("="*70)

        years = sorted(df_daily["year"].unique())
        print("\n" + "="*70)
        print("                 [연도별 상세 성과 분석]")
        print("="*70)
        print(f"{'연도':<6} | {'기초자산':>12} | {'기말자산':>12} | {'수익률':>8} | {'MDD':>7} | {'매매수':>6} | {'승률':>6} | {'PF':>5}")
        print("-" * 70)

        for y in years:
            sub_daily = df_daily[df_daily["year"] == y]
            sub_trades = df_trades[df_trades["year"] == y] if not df_trades.empty else pd.DataFrame()

            idx_prev = df_daily[df_daily["year"] < y].index
            if len(idx_prev) > 0:
                start_eq = df_daily.loc[idx_prev[-1], "equity"]
            else:
                start_eq = self.initial_capital

            end_eq = sub_daily["equity"].iloc[-1]
            y_ret = ((end_eq / start_eq) - 1.0) * 100

            sub_peak = sub_daily["equity"].cummax()
            sub_dd = (sub_daily["equity"] - sub_peak) / sub_peak * 100
            y_mdd = sub_dd.min()

            total_y_trades = len(sub_trades)
            if total_y_trades > 0:
                y_wins = sub_trades[sub_trades["pnl"] > 0]
                y_losses = sub_trades[sub_trades["pnl"] <= 0]
                y_win_rate = (len(y_wins) / total_y_trades) * 100
                loss_sum = abs(y_losses["pnl"].sum())
                win_sum = y_wins["pnl"].sum()
                y_pf = (win_sum / loss_sum) if loss_sum > 0 else (99.9 if win_sum > 0 else 0.0)
            else:
                y_win_rate = 0.0
                y_pf = 0.0

            print(f"{y}년   | {start_eq:>10,.0f}원 | {end_eq:>10,.0f}원 | {y_ret:>7.2f}% | {y_mdd:>6.2f}% | {total_y_trades:>4}회 | {y_win_rate:>5.1f}% | {y_pf:>5.2f}")

        print("="*70 + "\n")

if __name__ == "__main__":
    # =========================================================================
    # [시뮬레이션 파라미터 제어 센터] - 여기서 값들을 자유롭게 변경하며 테스트하세요!
    # =========================================================================
    tester = AdaptiveReversalDCABacktester(
        start_date="20210101",
        end_date="20260831",
        initial_capital=20_000_000,
        max_slots=7,                   # 계좌 분할 슬롯 수 (7개)
        max_tranches=5,                # 최대 물타기 차수 (5차)
        oversold_drop_rate=0.10,       # 1차 진입: 20일 고점 대비 -10% 과매도
        tranche_drop_threshold=0.09,   # 추가 매수: 평단 대비 -9% 이상 하락 시
        rebound_trigger_rate=0.03,     # 추가 매수: 바닥 대비 +3% 양봉 턴어라운드 확인
        
        # --- 보유일수 및 익절/탈출 파라미터 ---
        early_holding_days=60,         # 1단계: 기본 목표 기간 (60영업일, 약 3개월)
        early_target_rate=0.10,        # 1단계 목표가 (+12.0% 익절)
        
        mid_holding_days=120,          # 2단계: 중간 탈출 기간 (120영업일, 약 6개월)
        mid_target_rate=0.03,          # 2단계 목표가 (+5.0% 조기 탈출)
        
        late_target_rate=0.01,         # 3단계: 장기 방치 탈출 목표가 (+0.5% 수수료 회수 탈출)
        rebalance_days=120             # 5차 풀매수 후 리밸런싱(50% 손절) 발동 기준일 (120영업일)
    )
    df_trades, df_daily, remaining = tester.run_simulation()
    tester.print_performance(df_trades, df_daily, remaining)