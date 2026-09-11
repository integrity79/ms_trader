import os
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from dotenv import load_dotenv
load_dotenv()

from pykrx import stock

class MAAlignmentDCABacktesterV2:
    def __init__(
        self,
        start_date="20210101",
        end_date="20260831",
        initial_capital=20_000_000,
        max_slots=5,                      # 슬롯 수 (5개 종목 분산)
        max_tranches=6,                  # 최대 6회 분할 매수
        disparity_240_threshold=-0.20,   # 1차 진입: 240일선 대비 -20% 이하 극단 과매도
        tranche_drop_threshold=0.08,     # 추가 매수 대기: 평단 대비 -8% 이상 하락
        rebound_trigger_rate=0.03,        # 추가 매수 집행: 최저점 대비 +3% 양봉 반등
        target_profit_rate=0.30,          # 대익절 목표가 (+30.0%)
        rebalance_days=180,               # 풀매수 후 리밸런싱(50% 손절) 기준일 (180영업일)
        universe_size=100,                # 시가총액 상위 100위
        cache_dir="./daily_market_cache"
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_slots = max_slots
        self.max_tranches = max_tranches
        self.disparity_240_threshold = disparity_240_threshold
        self.tranche_drop_threshold = tranche_drop_threshold
        self.rebound_trigger_rate = rebound_trigger_rate
        self.target_profit_rate = target_profit_rate
        self.rebalance_days = rebalance_days
        self.universe_size = universe_size
        self.cache_dir = cache_dir
        self.fee_tax_rate = 0.0023

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

    def get_top100_universe(self, ref_date):
        cache_file = os.path.join(self.cache_dir, f"top100_universe_{self.start_date[:4]}.json")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

        try:
            df_cap = stock.get_market_cap_by_ticker(ref_date, market="KOSPI")
            if df_cap is not None and not df_cap.empty and "시가총액" in df_cap.columns:
                top100 = df_cap.sort_values(by="시가총액", ascending=False).head(self.universe_size).index.tolist()
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump(top100, f, ensure_ascii=False, indent=2)
                return top100
        except Exception:
            pass
        return stock.get_market_ticker_list(ref_date, market="KOSPI")[:self.universe_size]

    def run_simulation(self):
        trading_dates = self.get_trading_dates()
        top100_list = self.get_top100_universe(trading_dates[0])
        top100_set = set(top100_list)

        print(f"[1/2] 개선형 역배열 매집/중기 정배열 추세 청산 DCA 백테스트 시작...")
        print(f" • 테스트 기간: {self.start_date} ~ {self.end_date} (총 {len(trading_dates)}영업일)")
        print(f" • 슬롯: {self.max_slots}개 | 최대 {self.max_tranches}회 턴어라운드 매수 | 180일 리밸런싱 탑재")

        cash = float(self.initial_capital)
        positions = {}
        trade_logs = []
        daily_equity_logs = []
        price_history = {}

        for idx, curr_date in enumerate(trading_dates):
            df_day = self.get_daily_market_df(curr_date)
            if df_day.empty:
                continue

            # 이평선 연산을 위한 종가 버퍼 관리
            for ticker in top100_set:
                if ticker in df_day.index:
                    if ticker not in price_history:
                        price_history[ticker] = []
                    price_history[ticker].append(df_day.loc[ticker, "종가"])
                    if len(price_history[ticker]) > 260:
                        price_history[ticker].pop(0)

            # -------------------------------------------------------------
            # STEP 1: 보유 포지션 관리 (대익절, 중기 정배열 추세청산, 리밸런싱, 추가매수)
            # -------------------------------------------------------------
            closed_tickers = []
            for ticker, pos in positions.items():
                if ticker not in df_day.index:
                    pos["hold_days"] += 1
                    continue

                candle = df_day.loc[ticker]
                pos["hold_days"] += 1
                close_p = candle["종가"]
                high_p = candle["고가"]
                avg_p = pos["avg_price"]
                slot_budget = pos["slot_budget"]

                hist = price_history.get(ticker, [])
                if len(hist) < 240:
                    continue

                ma5 = sum(hist[-5:]) / 5.0
                ma20 = sum(hist[-20:]) / 20.0
                ma60 = sum(hist[-60:]) / 60.0

                # 1) 대익절 청산: 평단가 대비 +30% 도달 시 선제적 전량 청산
                target_30 = avg_p * (1.0 + self.target_profit_rate)
                if high_p >= target_30:
                    proceeds = (pos["shares"] * target_30) * (1.0 - self.fee_tax_rate)
                    pnl = proceeds - pos["invested"]
                    ret = (proceeds / pos["invested"]) - 1.0
                    cash += proceeds

                    trade_logs.append({
                        "ticker": ticker,
                        "name": stock.get_market_ticker_name(ticker),
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": pos["tranches"],
                        "invested": pos["invested"],
                        "proceeds": proceeds,
                        "pnl": pnl,
                        "return": ret,
                        "reason": "TARGET_PROFIT_30%"
                    })
                    closed_tickers.append(ticker)
                    continue

                # 2) 중기 정배열 달성 여부 추적 (종가 > 5MA > 20MA > 60MA)
                is_mid_bull_aligned = (close_p > ma5 > ma20 > ma60)
                if is_mid_bull_aligned and not pos["bull_confirmed"]:
                    pos["bull_confirmed"] = True

                # 3) 추세 청산: 중기 정배열이 1회 이상 나온 후 첫 20일선 하향 이탈 시 매도
                if pos["bull_confirmed"] and (close_p < ma20):
                    proceeds = (pos["shares"] * close_p) * (1.0 - self.fee_tax_rate)
                    pnl = proceeds - pos["invested"]
                    ret = (proceeds / pos["invested"]) - 1.0
                    cash += proceeds

                    trade_logs.append({
                        "ticker": ticker,
                        "name": stock.get_market_ticker_name(ticker),
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": pos["tranches"],
                        "invested": pos["invested"],
                        "proceeds": proceeds,
                        "pnl": pnl,
                        "return": ret,
                        "reason": "MID_BULL_THEN_MA20_BREAK"
                    })
                    closed_tickers.append(ticker)
                    continue

                # 4) 풀매수 후 180영업일 경과 시 50% 리밸런싱 (LG생활건강 결박 방지)
                if pos["tranches"] >= self.max_tranches and pos["hold_days"] >= self.rebalance_days and not pos.get("rebalanced", False):
                    sell_shares = pos["shares"] * 0.5
                    sell_price = candle["종가"]
                    recovered_cash = (sell_shares * sell_price) * (1.0 - self.fee_tax_rate)
                    realized_loss = (pos["invested"] * 0.5) - recovered_cash
                    cash += recovered_cash

                    pos["shares"] -= sell_shares
                    pos["invested"] = pos["invested"] * 0.5
                    pos["avg_price"] = pos["invested"] / pos["shares"]
                    pos["rebalanced"] = True
                    pos["rebalance_budget"] = recovered_cash
                    pos["local_low"] = sell_price
                    pos["tranches"] = self.max_tranches - 1  # 1회 추가 매수 슬롯 확보

                    trade_logs.append({
                        "ticker": ticker,
                        "name": stock.get_market_ticker_name(ticker),
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": self.max_tranches,
                        "invested": pos["invested"] * 2.0,
                        "proceeds": recovered_cash,
                        "pnl": -realized_loss,
                        "return": -realized_loss / (pos["invested"] * 2.0),
                        "reason": "REBALANCE_HALF_CUT"
                    })

                # 5) 턴어라운드 추가 매수 (평단 대비 -8% 하락 후 +3% 양봉 반등 확인)
                if pos["tranches"] < self.max_tranches:
                    curr_low = candle["저가"]
                    curr_close = candle["종가"]

                    if curr_low < pos["local_low"]:
                        pos["local_low"] = curr_low

                    is_deep_enough = (pos["local_low"] <= avg_p * (1.0 - self.tranche_drop_threshold))
                    is_rebound = (curr_close >= pos["local_low"] * (1.0 + self.rebound_trigger_rate)) and (candle["종가"] > candle["시가"])

                    if is_deep_enough and is_rebound:
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
            # STEP 2: 신규 1차 매수 진입 (완전 역배열 + 240선 대비 -20% 과매도 + 양봉)
            # -------------------------------------------------------------
            open_slots = self.max_slots - len(positions)
            if open_slots > 0:
                candidates = []
                for ticker in top100_set:
                    if ticker in positions or ticker not in df_day.index:
                        continue

                    hist = price_history.get(ticker, [])
                    if len(hist) < 240:
                        continue

                    close_p = df_day.loc[ticker, "종가"]
                    open_p = df_day.loc[ticker, "시가"]

                    # 당일 양봉 체크
                    if close_p < open_p or close_p <= 0:
                        continue

                    ma5 = sum(hist[-5:]) / 5.0
                    ma20 = sum(hist[-20:]) / 20.0
                    ma60 = sum(hist[-60:]) / 60.0
                    ma120 = sum(hist[-120:]) / 120.0
                    ma240 = sum(hist[-240:]) / 240.0

                    # 1. 완전 역배열 확인: 종가 < 5 < 20 < 60 < 120 < 240
                    is_bear_aligned = (close_p < ma5 < ma20 < ma60 < ma120 < ma240)

                    # 2. 240일선 대비 괴리율이 기준(-20%) 이하로 극단적 투매가 나왔는가?
                    disparity_240 = (close_p - ma240) / ma240

                    if is_bear_aligned and (disparity_240 <= self.disparity_240_threshold):
                        candidates.append((ticker, close_p, disparity_240))

                # 괴리율이 가장 극심한(낙폭이 가장 깊은) 순서로 정렬
                candidates.sort(key=lambda x: x[2])
                chosen = candidates[:open_slots]

                pos_val = sum(p["shares"] * df_day.loc[tk]["종가"] for tk, p in positions.items() if tk in df_day.index)
                current_equity = cash + pos_val
                slot_budget = current_equity / self.max_slots
                tranche_budget = slot_budget / self.max_tranches

                for ticker, close_p, _ in chosen:
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
                            "bull_confirmed": False,
                            "rebalanced": False
                        }

            # -------------------------------------------------------------
            # STEP 3: 순자산(NAV) 일별 기록
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
        wins = df_trades[df_trades["pnl"] > 0] if total_trades > 0 else pd.DataFrame()
        losses = df_trades[df_trades["pnl"] <= 0] if total_trades > 0 else pd.DataFrame()
        win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0

        loss_sum = abs(losses["pnl"].sum()) if not losses.empty else 0.0
        win_sum = wins["pnl"].sum() if not wins.empty else 0.0
        pf = (win_sum / loss_sum) if loss_sum > 0 else (99.9 if win_sum > 0 else 0.0)

        print("\n" + "="*70)
        print(f"  [{self.start_date[:4]}~{self.end_date[:4]}] 개선형 이평선 매집/추세 청산 DCA Ver 2 성과")
        print("="*70)
        print(f"테스트 기간           : {self.start_date} ~ {self.end_date} (약 {total_years:.1f}년)")
        print(f"초기 투자 원금        : {self.initial_capital:,.0f}원")
        print(f"최종 계좌 평가액      : {final_equity:,.0f}원")
        print(f"계좌 총 누적 수익률   : {total_account_return:+.2f}%")
        print(f"연평균 복리 수익률(CAGR): {cagr:+.2f}%")
        print(f"통합 최대 낙폭 (MDD)  : {real_mdd:.2f}%")
        print("-"*70)
        print(f"총 청산 거래 건수     : {total_trades}회")
        print(f"실현 승률 (Win Rate)  : {win_rate:.2f}%")
        print(f"수익 팩터 (Profit Factor): {pf:.2f}")
        print(f"평균 보유 영업일       : {df_trades['hold_days'].mean():.1f}일 (약 {df_trades['hold_days'].mean()/20:.1f}개월)" if total_trades > 0 else "N/A")
        print(f"미청산 보유 종목 수   : {len(remaining_positions)}개 / {self.max_slots}슬롯")
        if remaining_positions:
            print(" • 백테스트 종료 시점 보유 종목 현황:")
            for tk, pos in remaining_positions.items():
                name = stock.get_market_ticker_name(tk)
                status_str = "중기 정배열 달성 후 추세 진행 중" if pos["bull_confirmed"] else "바닥 다지기 진행 중"
                print(f"   - [{tk} {name:<8}] {pos['tranches']}/{self.max_tranches}차 | 보유: {pos['hold_days']:>3}일 | 투자금: {pos['invested']:>10,.0f}원 | {status_str}")
        print("-"*70)
        if total_trades > 0:
            print("청산 사유별 상세 통계:")
            for reason, count in df_trades["reason"].value_counts().items():
                print(f" • {reason:<26}: {count:>3}건 ({count/total_trades*100:5.1f}%)")
            print("-"*70)
            print("청산 수익률 상위 Top 5 종목:")
            top_trades = df_trades.sort_values(by="return", ascending=False).head(5)
            for _, tr in top_trades.iterrows():
                print(f" • [{tr['ticker']} {tr['name']}] 수익률: {tr['return']*100:+.2f}% | 손익: {int(tr['pnl']):+,}원 | 보유: {tr['hold_days']}일 | {tr['reason']}")
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
            start_eq = df_daily.loc[idx_prev[-1], "equity"] if len(idx_prev) > 0 else self.initial_capital
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
                y_loss_sum = abs(y_losses["pnl"].sum())
                y_win_sum = y_wins["pnl"].sum()
                y_pf = (y_win_sum / y_loss_sum) if y_loss_sum > 0 else (99.9 if y_win_sum > 0 else 0.0)
            else:
                y_win_rate = 0.0
                y_pf = 0.0

            print(f"{y}년   | {start_eq:>10,.0f}원 | {end_eq:>10,.0f}원 | {y_ret:>7.2f}% | {y_mdd:>6.2f}% | {total_y_trades:>4}회 | {y_win_rate:>5.1f}% | {y_pf:>5.2f}")

        print("="*70 + "\n")

if __name__ == "__main__":
    # =========================================================================
    # [시뮬레이션 파라미터 제어 센터]
    # =========================================================================
    tester = MAAlignmentDCABacktesterV2(
        start_date="20210101",
        end_date="20260831",
        initial_capital=20_000_000,
        max_slots=5,                      # 슬롯 수 (5개 종목 분산)
        max_tranches=4,                  # 최대 6회 턴어라운드 분할 매수
        disparity_240_threshold=-0.20,   # 1차 진입: 240일선 대비 -20% 이하 극단 과매도 양봉
        tranche_drop_threshold=0.08,     # 추가 매수: 평단 대비 -8% 이상 하락 시 대기
        rebound_trigger_rate=0.03,        # 추가 매수: 바닥 대비 +3% 양봉 턴어라운드 확인 시 매수
        target_profit_rate=0.25,          # 대익절: 평단 대비 +30% 도달 시 전량 청산
        rebalance_days=180,               # 6차 풀매수 후 180영업일 경과 시 50% 리밸런싱
        universe_size=100
    )
    df_trades, df_daily, remaining = tester.run_simulation()
    tester.print_performance(df_trades, df_daily, remaining)