from dotenv import load_dotenv
load_dotenv()

import os
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pykrx import stock
import koreanize_matplotlib

class OptimalPortfolioDcaBacktester:
    def __init__(
        self, 
        start_date="20220101", 
        end_date="20251231", 
        initial_capital=20_000_000, 
        max_slots=4, 
        bear_slot_scale=1.00,
        cache_dir="./market_data_cache"
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.max_slots = max_slots
        self.bear_slot_scale = bear_slot_scale
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        # 전략 핵심 파라미터 (골든 모델)
        self.universe_size = 100                  # 시가총액 상위 100개 우량주
        self.disparity_threshold = 92.0           # 20일 이격도 92% 이하
        self.rsi_threshold = 32.0                 # RSI(14) 32 이하
        self.stoch_oversold = 25.0                # 스토캐스틱 %K 25 이하

        # 분할 매수 비중 (40% -> 30% -> 30%)
        self.tranche_weights = [0.40, 0.30, 0.30]
        
        # [평단가 기준 추가 매수 하락률]
        # 1차: 당일 진입
        # 2차: 1차 평단가 대비 -5% 하락 시
        # 3차: 2차 체결로 낮아진 평단가 대비 다시 -5% 추가 하락 시
        self.step_drop_rate = 0.05

        # 청산 룰 (회전율 극대화)
        self.target_profit_rate = 0.07            # 평단 대비 +7% 도달 시 전량 익절
        self.final_stop_loss_rate = -0.05         # 3차 체결 후 최종 평단 대비 -5% 이탈 시 손절
        self.max_holding_days = 30                # 30영업일 타임컷
        self.fee_tax_rate = 0.0023                # 거래세 + 수수료 (0.23%)

        # 시장 국면 사전 수집
        self.market_regime = self.fetch_market_regime()

    def fetch_market_regime(self):
        """코스피 60MA 기준 시장 국면 사전 판별"""
        print("[0/4] 코스피 지수 60MA 시장 국면 분석 중...")
        try:
            dt_start = datetime.strptime(self.start_date, "%Y%m%d") - timedelta(days=120)
            buffer_start = dt_start.strftime("%Y%m%d")
            
            df_kospi = stock.get_index_ohlcv_by_date(buffer_start, self.end_date, "1001")
            if df_kospi.empty:
                return {}

            df_kospi["60MA"] = df_kospi["종가"].rolling(window=60).mean()
            df_kospi["is_bear"] = df_kospi["종가"] < df_kospi["60MA"]

            regime_map = {}
            for dt_idx, val in df_kospi["is_bear"].items():
                is_bear_val = bool(val)
                regime_map[pd.to_datetime(dt_idx)] = is_bear_val
                regime_map[pd.to_datetime(dt_idx).strftime("%Y%m%d")] = is_bear_val

            bear_ratio = df_kospi.loc[self.start_date:self.end_date, "is_bear"].mean() * 100
            print(f"-> 코스피 약세 국면(60MA 하회) 비중: {bear_ratio:.1f}%")
            return regime_map
        except Exception as e:
            print(f"-> [경고] 시장 국면 조회 실패: {e}")
            return {}

    def get_top_market_cap_tickers(self, base_date="20250602"):
        """시총 상위 100개 대형 우량주 추출"""
        print("[1/4] 시가총액 상위 100개 우량주 유니버스 추출 중...")
        dt = datetime.strptime(base_date, "%Y%m%d")
        df_cap = None

        for _ in range(10):
            target_str = dt.strftime("%Y%m%d")
            try:
                df_temp = stock.get_market_cap_by_ticker(target_str, market="ALL")
                if df_temp is not None and not df_temp.empty and "시가총액" in df_temp.columns:
                    if (df_temp["시가총액"] > 0).any():
                        df_cap = df_temp
                        print(f"-> 기준 영업일 확인: {target_str}")
                        break
            except Exception:
                pass
            dt -= timedelta(days=1)

        if df_cap is None or df_cap.empty:
            print("-> [경고] 시총 조회 실패로 기본 코스피 리스트를 사용합니다.")
            kospi = stock.get_market_ticker_list("20250602", market="KOSPI")
            return kospi[:self.universe_size]

        df_cap = df_cap.sort_values(by="시가총액", ascending=False)
        tickers = df_cap.index.tolist()[:self.universe_size]
        print(f"-> 대상 종목 수: {len(tickers)}개")
        return tickers

    def fetch_and_prepare_all_data(self, tickers):
        """100종목 데이터 수집 및 지표 사전 연산"""
        print(f"[2/4] 유니버스 100종목 데이터 로드 및 지표 연산 중 ({self.start_date}~{self.end_date})...")
        market_data = {}

        for idx, ticker in enumerate(tickers):
            cache_path = os.path.join(self.cache_dir, f"{ticker}_{self.start_date}_{self.end_date}.parquet")
            df = None

            if os.path.exists(cache_path):
                try:
                    df = pd.read_parquet(cache_path)
                except Exception:
                    df = None

            if df is None or df.empty or len(df) < 50:
                try:
                    df = stock.get_market_ohlcv_by_date(self.start_date, self.end_date, ticker)
                    if df.empty or len(df) < 50:
                        continue
                    df.to_parquet(cache_path)
                    time.sleep(0.04)
                except Exception:
                    continue

            df.index = pd.to_datetime(df.index)
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

            is_oversold = (df["이격도_20"] <= self.disparity_threshold) | (df["RSI"] <= self.rsi_threshold)
            stoch_gc = (df["SLOW_K"].shift(1) <= df["SLOW_D"].shift(1)) & (df["SLOW_K"] > df["SLOW_D"]) & (df["SLOW_K"] <= self.stoch_oversold)
            body = (df["종가"] - df["시가"]).abs()
            lower_tail = df[["시가", "종가"]].min(axis=1) - df["저가"]
            tail_support = lower_tail >= body
            df["buy_signal"] = is_oversold & (stoch_gc | tail_support)

            market_data[ticker] = df

            if (idx + 1) % 25 == 0 or (idx + 1) == len(tickers):
                print(f" -> 로드 진행도: {idx + 1}/{len(tickers)} 완료")

        return market_data

    def run_simulation(self, tickers):
        """평단가 기준 DCA 포트폴리오 백테스팅 시뮬레이션"""
        market_data = self.fetch_and_prepare_all_data(tickers)
        
        all_dates = set()
        for df in market_data.values():
            all_dates.update(df.index)
        trading_dates = sorted(list(all_dates))

        print(f"[3/4] 포트폴리오 일자별 시뮬레이션 시작... (총 {len(trading_dates)}영업일)")

        cash = float(self.initial_capital)
        positions = {}
        trade_logs = []
        daily_equity_logs = []

        for curr_date in trading_dates:
            curr_date_str = pd.to_datetime(curr_date).strftime("%Y%m%d")
            is_bear = self.market_regime.get(curr_date, self.market_regime.get(curr_date_str, False))

            # -------------------------------------------------------------
            # STEP 1: 보유 포지션 관리 (추가 매수 & 청산)
            # -------------------------------------------------------------
            closed_tickers = []

            for ticker, pos in positions.items():
                df = market_data[ticker]
                if curr_date not in df.index:
                    pos["hold_days"] += 1
                    continue

                candle = df.loc[curr_date]
                pos["hold_days"] += 1
                slot_budget = pos["slot_budget"]

                # ---------------------------------------------------------
                # [수정] 1) 2차 추가 매수: '1차 평단가' 대비 -5% 도달 시
                # ---------------------------------------------------------
                if pos["tranches"] == 1:
                    target_p2 = pos["avg_price"] * (1.0 - self.step_drop_rate)
                    if candle["저가"] <= target_p2:
                        fill_price = min(candle["시가"], target_p2)
                        add_budget = slot_budget * self.tranche_weights[1]
                        if cash >= add_budget:
                            add_shares = add_budget / fill_price
                            cash -= add_budget
                            pos["shares"] += add_shares
                            pos["invested"] += add_budget
                            pos["tranches"] = 2
                            # [핵심] 2차 매수 직후 낮아진 평단가를 즉시 재계산
                            pos["avg_price"] = pos["invested"] / pos["shares"]

                # ---------------------------------------------------------
                # [수정] 2) 3차 추가 매수: '2차 체결 후 낮아진 평단가' 대비 다시 -5% 도달 시
                # ---------------------------------------------------------
                if pos["tranches"] == 2:
                    target_p3 = pos["avg_price"] * (1.0 - self.step_drop_rate)
                    if candle["저가"] <= target_p3:
                        fill_price = min(candle["시가"], target_p3)
                        add_budget = slot_budget * self.tranche_weights[2]
                        if cash >= add_budget:
                            add_shares = add_budget / fill_price
                            cash -= add_budget
                            pos["shares"] += add_shares
                            pos["invested"] += add_budget
                            pos["tranches"] = 3
                            # [핵심] 3차 매수 직후 최종 평단가 재계산
                            pos["avg_price"] = pos["invested"] / pos["shares"]

                avg_price = pos["avg_price"]

                # 3) 청산 조건 검증
                exit_triggered = False
                exit_price = 0.0
                exit_type = ""

                target_price = avg_price * (1 + self.target_profit_rate)
                hard_stop = avg_price * (1 + self.final_stop_loss_rate)

                if candle["고가"] >= target_price:
                    exit_triggered = True
                    exit_price = target_price
                    exit_type = "TARGET_PROFIT"
                elif candle["종가"] >= candle["20MA"]:
                    exit_triggered = True
                    exit_price = candle["종가"]
                    exit_type = "20MA_RECOVER"
                elif pos["tranches"] == 3 and candle["종가"] <= hard_stop:
                    exit_triggered = True
                    exit_price = hard_stop
                    exit_type = "STOP_LOSS"
                elif pos["hold_days"] >= self.max_holding_days:
                    exit_triggered = True
                    exit_price = candle["종가"]
                    exit_type = "TIME_CUT"

                if exit_triggered:
                    proceeds = (pos["shares"] * exit_price) * (1.0 - self.fee_tax_rate)
                    pnl = proceeds - pos["invested"]
                    ret = (proceeds / pos["invested"]) - 1.0
                    cash += proceeds

                    trade_logs.append({
                        "ticker": ticker,
                        "entry_date": pos["entry_date"],
                        "exit_date": curr_date,
                        "hold_days": pos["hold_days"],
                        "tranches": pos["tranches"],
                        "invested": pos["invested"],
                        "proceeds": proceeds,
                        "pnl": pnl,
                        "return": ret,
                        "result": "WIN" if pnl > 0 else "LOSS",
                        "exit_type": exit_type
                    })
                    closed_tickers.append(ticker)

            for t in closed_tickers:
                del positions[t]

            # -------------------------------------------------------------
            # STEP 2: 신규 종목 진입
            # -------------------------------------------------------------
            open_slots = self.max_slots - len(positions)
            
            if open_slots > 0:
                candidates = []
                for ticker in tickers:
                    if ticker in positions or ticker not in market_data:
                        continue
                    df = market_data[ticker]
                    if curr_date not in df.index:
                        continue
                    candle = df.loc[curr_date]
                    if candle.get("buy_signal", False):
                        candidates.append((ticker, candle["RSI"], candle["종가"]))

                candidates.sort(key=lambda x: x[1])
                chosen = candidates[:open_slots]

                current_positions_val = sum(
                    pos["shares"] * market_data[t].loc[curr_date]["종가"] 
                    for t, pos in positions.items() if curr_date in market_data[t].index
                )
                current_equity = cash + current_positions_val
                
                scale = self.bear_slot_scale if is_bear else 1.0
                slot_budget = (current_equity / self.max_slots) * scale

                for t, _, close_p in chosen:
                    init_budget = slot_budget * self.tranche_weights[0]
                    if cash >= init_budget:
                        shares = init_budget / close_p
                        cash -= init_budget
                        positions[t] = {
                            "entry_date": curr_date,
                            "hold_days": 0,
                            "slot_budget": slot_budget,
                            "shares": shares,
                            "invested": init_budget,
                            "p1": close_p,
                            "avg_price": close_p,  # 초기 평단가 = 1차 매수가
                            "tranches": 1
                        }

            # -------------------------------------------------------------
            # STEP 3: 당일 순자산(NAV) 기록
            # -------------------------------------------------------------
            pos_val = sum(
                pos["shares"] * market_data[t].loc[curr_date]["종가"] 
                for t, pos in positions.items() if curr_date in market_data[t].index
            )
            daily_total_equity = cash + pos_val
            daily_equity_logs.append({
                "date": curr_date,
                "cash": cash,
                "stock_val": pos_val,
                "equity": daily_total_equity,
                "active_slots": len(positions)
            })

        df_trades = pd.DataFrame(trade_logs)
        df_daily = pd.DataFrame(daily_equity_logs)
        return df_trades, df_daily

    def print_annual_breakdown(self, df_daily, df_trades):
        """연도별 개별 수익률, MDD, 승률 자동 분해 출력"""
        if df_daily.empty or df_trades.empty:
            return

        df_d = df_daily.copy()
        df_d["year"] = df_d["date"].dt.year
        
        df_t = df_trades.copy()
        df_t["exit_year"] = pd.to_datetime(df_t["exit_date"]).dt.year

        print("\n" + "="*60)
        print("                 연도별 개별 실적 분해 리포트")
        print("="*60)
        print(f"{'연도':<6} | {'체결건수':<8} | {'승률':<8} | {'연간 수익률':<12} | {'연간 MDD':<10}")
        print("-" * 60)

        years = sorted(df_d["year"].unique())
        for yr in years:
            sub_d = df_d[df_d["year"] == yr].copy().reset_index(drop=True)
            sub_t = df_t[df_t["exit_year"] == yr].copy()

            yr_start_eq = sub_d["equity"].iloc[0]
            yr_end_eq = sub_d["equity"].iloc[-1]
            yr_return = ((yr_end_eq / yr_start_eq) - 1.0) * 100

            sub_d["yr_peak"] = sub_d["equity"].cummax()
            sub_d["yr_dd"] = (sub_d["equity"] - sub_d["yr_peak"]) / sub_d["yr_peak"] * 100
            yr_mdd = sub_d["yr_dd"].min()

            t_cnt = len(sub_t)
            w_cnt = len(sub_t[sub_t["result"] == "WIN"])
            win_rate = (w_cnt / t_cnt * 100) if t_cnt > 0 else 0.0

            print(f"{yr}년  | {t_cnt:>4}회    | {win_rate:>6.2f}% | {yr_return:>+10.2f}% | {yr_mdd:>8.2f}%")

        print("="*60)

    def print_performance(self, df_trades, df_daily):
        """가변 기간 연동 상세 성과 보고서 출력"""
        if df_trades.empty or df_daily.empty:
            print("시뮬레이션 거래 결과가 없습니다.")
            return

        total_trades = len(df_trades)
        wins = df_trades[df_trades["result"] == "WIN"]
        losses = df_trades[df_trades["result"] == "LOSS"]

        win_rate = (len(wins) / total_trades) * 100
        avg_ret = df_trades["return"].mean() * 100
        avg_win = wins["return"].mean() * 100 if not wins.empty else 0
        avg_loss = losses["return"].mean() * 100 if not losses.empty else 0
        profit_factor = abs(wins["pnl"].sum() / losses["pnl"].sum()) if not losses.empty and losses["pnl"].sum() != 0 else np.nan

        final_equity = df_daily["equity"].iloc[-1]
        total_account_return = ((final_equity / self.initial_capital) - 1.0) * 100
        
        total_days = (df_daily["date"].iloc[-1] - df_daily["date"].iloc[0]).days
        total_years = max(total_days / 365.25, 0.1)
        cagr = ((final_equity / self.initial_capital) ** (1.0 / total_years) - 1.0) * 100
        
        df_daily["peak"] = df_daily["equity"].cummax()
        df_daily["drawdown"] = (df_daily["equity"] - df_daily["peak"]) / df_daily["peak"] * 100
        real_mdd = df_daily["drawdown"].min()
        avg_slots = df_daily["active_slots"].mean()

        t1_count = len(df_trades[df_trades["tranches"] == 1])
        t2_count = len(df_trades[df_trades["tranches"] == 2])
        t3_count = len(df_trades[df_trades["tranches"] == 3])

        print("\n" + "="*52)
        print(f"  [{self.start_date[:4]}~{self.end_date[:4]}] {self.max_slots}슬롯 DCA 평단가 기준 성과 보고서")
        print("="*52)
        print(f"테스트 기간           : {self.start_date} ~ {self.end_date} (약 {total_years:.1f}년)")
        print(f"초기 투자 원금        : {self.initial_capital:,.0f}원")
        print(f"최종 계좌 평가액      : {final_equity:,.0f}원")
        print(f"계좌 총 누적 수익률   : {total_account_return:+.2f}%")
        print(f"연평균 복리 수익률(CAGR): {cagr:+.2f}%")
        print(f"통합 최대 낙폭 (MDD)  : {real_mdd:.2f}%")
        print(f"평균 슬롯 가동 개수   : {avg_slots:.1f}개 / {self.max_slots}.0개 (가동률: {avg_slots/self.max_slots*100:.1f}%)")
        print("-"*52)
        print(f"실제 총 체결 건수     : {total_trades}회 (연평균 약 {total_trades/total_years:.0f}회)")
        print(f"승률 (Win Rate)       : {win_rate:.2f}%")
        print(f"평균 손익비 (Win/Loss): {abs(avg_win / avg_loss):.2f}" if avg_loss != 0 else "N/A")
        print(f"수익 팩터 (Profit Factor): {profit_factor:.2f}")
        print(f"건당 평균 수익률       : {avg_ret:+.2f}%")
        print(f"평균 보유 영업일       : {df_trades['hold_days'].mean():.1f}일")
        print("-"*52)
        print("분할 매수 체결 차수별 비중 (DCA Tranche Ratio):")
        print(f" • 1차만 체결 후 청산 (40% 투입): {t1_count:>3}건 ({t1_count/total_trades*100:5.1f}%)")
        print(f" • 2차까지 체결 후 청산(70% 투입): {t2_count:>3}건 ({t2_count/total_trades*100:5.1f}%)")
        print(f" • 3차 전량 체결 후 청산(100%투입): {t3_count:>3}건 ({t3_count/total_trades*100:5.1f}%)")
        print("-"*52)
        print("청산 사유별 체결 비중:")
        for exit_type, count in df_trades["exit_type"].value_counts().items():
            print(f" • {exit_type:<15}: {count:>3}건 ({count/total_trades*100:5.1f}%)")
        print("="*52)

def plot_portfolio_results(df_daily, initial_capital=20_000_000, save_path=None):
    """자산 평가액 및 Drawdown 곡선 시각화"""
    if df_daily.empty:
        return

    df = df_daily.copy()
    total_ret = ((df["equity"].iloc[-1] / initial_capital) - 1.0) * 100
    mdd = df["drawdown"].min()
    start_str = df["date"].iloc[0].strftime("%Y%m%d")
    end_str = df["date"].iloc[-1].strftime("%Y%m%d")

    fig, (ax1, ax2) = plt.subplots(
        nrows=2, 
        ncols=1, 
        figsize=(12, 8), 
        sharex=True, 
        gridspec_kw={"height_ratios": [2.5, 1]}
    )
    fig.suptitle(
        f"포트폴리오 백테스트 ({start_str}~{end_str} | 총 수익: {total_ret:+.2f}% | MDD: {mdd:.2f}%)", 
        fontsize=14, 
        fontweight="bold"
    )

    ax1.plot(df["date"], df["equity"] / 10_000, color="#2ca02c", linewidth=2, label="계좌 순자산 (만원)")
    ax1.plot(df["date"], df["peak"] / 10_000, color="#7f7f7f", linestyle="--", linewidth=1, alpha=0.7, label="고점 (HWM)")
    ax1.axhline(initial_capital / 10_000, color="red", linestyle=":", alpha=0.6, label="초기 원금")
    
    ax1.set_ylabel("계좌 평가액 (만원)", fontsize=11)
    ax1.legend(loc="upper left", frameon=True)
    ax1.grid(True, linestyle="--", alpha=0.4)
    ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))

    ax2.plot(df["date"], df["drawdown"], color="#d62728", linewidth=1.2, label="Drawdown (%)")
    ax2.fill_between(df["date"], df["drawdown"], 0, color="#d62728", alpha=0.25)
    
    mdd_idx = df["drawdown"].idxmin()
    ax2.scatter(df.loc[mdd_idx, "date"], mdd, color="black", s=30, zorder=5)
    ax2.annotate(
        f"MDD: {mdd:.2f}%",
        xy=(df.loc[mdd_idx, "date"], mdd),
        xytext=(10, -10),
        textcoords="offset points",
        fontsize=9,
        fontweight="bold",
        color="#d62728"
    )

    ax2.set_ylabel("낙폭 (%)", fontsize=11)
    ax2.set_xlabel("일자", fontsize=11)
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.set_ylim(min(mdd * 1.2, -5), 1)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches="tight")
        print(f"[저장 완료] 결과 차트 저장: {save_path}")

    plt.show()

if __name__ == "__main__":
    tester = OptimalPortfolioDcaBacktester(
        start_date="20210101", 
        end_date="20251231", 
        initial_capital=20_000_000, 
        max_slots=4,
        bear_slot_scale=1.00
    )
    
    top_tickers = tester.get_top_market_cap_tickers()
    df_trades, df_daily = tester.run_simulation(top_tickers)
    tester.print_performance(df_trades, df_daily)
    tester.print_annual_breakdown(df_daily, df_trades)
    
    if not df_daily.empty:
        plot_portfolio_results(df_daily, initial_capital=20_000_000, save_path="final_dca_portfolio_equity.png")