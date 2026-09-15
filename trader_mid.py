# trader_mid.py
import os
import json
import math
import time
from datetime import datetime, timedelta
import pandas as pd

#from dotenv import load_dotenv
#load_dotenv()

from pykrx import stock
from state_store import atomic_write_json

POSITIONS_FILE = "trader_mid_positions.json"
UNIVERSE_FILE = "kospi50_universe.json"

class MidTermTrader:
    strategy_name = "mid"

    def __init__(self, client, notifier):
        self.client = client
        self.notifier = notifier

        self.capital_ratio = float(os.getenv("MID_CAPITAL_RATIO", "0.50"))
        self.max_slots = int(os.getenv("MID_MAX_SLOTS", "7"))
        self.max_tranches = int(os.getenv("MID_MAX_TRANCHES", "5"))
        self.oversold_drop_rate = float(os.getenv("MID_OVERSOLD_DROP", "0.10"))
        self.tranche_drop_threshold = float(os.getenv("MID_TRANCHE_DROP", "0.09"))
        self.rebound_trigger_rate = float(os.getenv("MID_REBOUND_RATE", "0.03"))

        self.early_holding_days = int(os.getenv("MID_EARLY_DAYS", "60"))
        self.early_target_rate = float(os.getenv("MID_EARLY_TARGET", "0.10"))
        self.mid_holding_days = int(os.getenv("MID_MID_DAYS", "120"))
        self.mid_target_rate = float(os.getenv("MID_MID_TARGET", "0.03"))
        self.late_target_rate = float(os.getenv("MID_LATE_TARGET", "0.01"))
        self.rebalance_days = int(os.getenv("MID_REBALANCE_DAYS", "120"))
        self.buy_fee_rate = float(os.getenv("BUY_FEE_RATE", "0.0"))
        self.sell_fee_tax_rate = float(os.getenv("SELL_FEE_TAX_RATE", "0.0023"))
        self.universe_cache_days = int(os.getenv("MID_UNIVERSE_CACHE_DAYS", "30"))
        rate_values = (
            self.oversold_drop_rate,
            self.tranche_drop_threshold,
            self.rebound_trigger_rate,
            self.early_target_rate,
            self.mid_target_rate,
            self.late_target_rate,
            self.buy_fee_rate,
            self.sell_fee_tax_rate,
        )
        if not 0 < self.capital_ratio <= 1 or self.max_slots <= 0 or self.max_tranches <= 0 or self.early_holding_days <= 0 or self.mid_holding_days < self.early_holding_days or self.rebalance_days <= 0 or any(not math.isfinite(rate) or rate < 0 for rate in rate_values):
            raise ValueError("중기형 자본비율, 보유일, 슬롯 및 수익률 설정이 유효하지 않습니다.")

        self.positions = self.load_positions()
        self.universe_map = self.get_or_update_kospi50()
        self._candidate_histories = {}
        self._candidate_history_date = ""
        # 키움 조건검색식으로 신규 후보를 좁혀서(전체 유니버스 스캔 대신) 대량 호출을 줄인다.
        # 이름으로 설정하면 CNSRLST 목록에서 seq를 찾아 캐시해둔다. 미설정/조회 실패 시에는
        # 기존 방식(universe_map 전체 스캔)으로 자동 폴백한다.
        self.condition_name = os.getenv("MID_CONDITION_NAME", "").strip()
        self._condition_seq = None
        self._condition_seq_resolved = False

    def load_positions(self) -> dict:
        if os.path.exists(POSITIONS_FILE):
            try:
                with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
                    return json.load(f).get("positions", {})
            except (OSError, json.JSONDecodeError) as e:
                raise RuntimeError(f"중기형 포지션 파일을 읽을 수 없습니다: {POSITIONS_FILE}") from e
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

    def get_or_update_kospi50(self) -> dict:
        now = datetime.now()
        if os.path.exists(UNIVERSE_FILE):
            try:
                with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    cached_dt_str = data.get("updated_at", "")
                    cached_map = data.get("tickers", {})
                    if cached_dt_str and cached_map:
                        cached_dt = datetime.strptime(cached_dt_str, "%Y-%m-%d %H:%M:%S")
                        if (now - cached_dt).days < self.universe_cache_days:
                            return cached_map
            except Exception:
                pass

        print(f"🔄 [중기형 KOSPI Top 50 갱신 중...]", flush=True)
        for i in range(10):
            dt_str = (now - timedelta(days=i)).strftime("%Y%m%d")
            try:
                df_cap = stock.get_market_cap_by_ticker(dt_str, market="KOSPI")
                if df_cap is not None and not df_cap.empty and "시가총액" in df_cap.columns:
                    top50 = df_cap.sort_values(by="시가총액", ascending=False).head(50)
                    new_map = {tk: stock.get_market_ticker_name(tk) for tk in top50.index}
                    atomic_write_json(UNIVERSE_FILE, {"updated_at": now.strftime("%Y-%m-%d %H:%M:%S"), "tickers": new_map})
                    return new_map
            except Exception:
                time.sleep(0.3)

        if os.path.exists(UNIVERSE_FILE):
            with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
                return json.load(f).get("tickers", {})
        return {}

    def get_short_positions_tickers(self) -> set:
        """단기형 포지션 파일(trader_short_positions.json)에서 보유 중인 티커 추출"""
        short_file = "trader_short_positions.json"
        if os.path.exists(short_file):
            try:
                with open(short_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return set(data.get("positions", {}).keys())
            except Exception:
                return set()
        return set()

    def _resolve_condition_seq(self):
        """MID_CONDITION_NAME에 해당하는 조건검색식 seq를 찾아 캐시한다.
        미설정이면 None(조건검색 미사용 모드). 설정했는데 목록 조회/이름 매칭에 실패해도
        None을 반환하지만, 이 경우 호출부는 universe_map 전체 스캔으로 돌아가지 않고
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
            print(f"⚠️ [중기형 조건검색] 목록 조회 실패, 이번 회차는 후보 없음으로 처리합니다: {e}", flush=True)
            return None
        for seq, name in conditions:
            if name == self.condition_name:
                self._condition_seq = seq
                print(f"✅ [중기형 조건검색] '{self.condition_name}' 연결됨 (seq={seq})", flush=True)
                return seq
        print(f"⚠️ [중기형 조건검색] '{self.condition_name}' 조건식을 찾지 못했습니다. 이번 회차는 후보 없음으로 처리합니다.", flush=True)
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
            print("[중기형 사전 적재] 조건검색 모드라 전체 유니버스 사전 적재를 건너뜁니다.", flush=True)
            # 실제 마감 루틴(15시대) 전에 조건검색이 잘 붙어 있는지, 지금 몇 종목이 잡히는지
            # 바로 확인할 수 있도록 한 번 미리 실행해서 매칭 건수만 보여준다(부트스트랩 시 1회뿐).
            # 마감 루틴 시점엔 scan_new_candidates()가 그때 시세로 다시 실행하므로 결과가 달라질 수 있다.
            if seq:
                try:
                    matches = self.client.run_condition_search(seq, stex_tp="K")
                    print(f"[중기형 조건검색 미리보기] '{self.condition_name}' 현재 매칭 {len(matches)}종목: "
                          f"{', '.join(m['ticker'] for m in matches) or '없음'}", flush=True)
                except Exception as e:
                    print(f"⚠️ [중기형 조건검색 미리보기] 실행 실패(마감 루틴 때 재시도됨): {e}", flush=True)
            return

        start_date = (datetime.now() - timedelta(days=45)).strftime("%Y%m%d")
        total = len(self.universe_map)
        print(f"[중기형 사전 적재] 후보 {total}종목 일봉 조회 시작", flush=True)
        started_at = time.monotonic()
        last_logged_at = started_at

        histories = {}
        for i, ticker in enumerate(self.universe_map, start=1):
            try:
                df = self.client.get_daily_ohlcv(ticker, start_date, today_str)
                if df is not None and len(df) >= 20:
                    histories[ticker] = df
            except Exception:
                continue
            now = time.monotonic()
            if now - last_logged_at >= 5.0 or i == total:
                print(f"[중기형 사전 적재] 진행 {i}/{total} ({now - started_at:.0f}초 경과)", flush=True)
                last_logged_at = now

        self._candidate_histories = histories
        self._candidate_history_date = today_str
        print(f"[중기형 사전 적재] 후보 지표용 일봉 {len(histories)}종목 준비 완료 ({time.monotonic() - started_at:.0f}초 소요)", flush=True)

    def scan_new_candidates(self, verbose: bool = False) -> list:
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")
        dt_20 = (now - timedelta(days=45)).strftime("%Y%m%d")
        candidates = []
        skip_counts = {"이미 보유": 0, "단기형 보유": 0, "상승 미달": 0, "이력 부족": 0, "조회 오류": 0}

        # 조건검색식을 설정한 이상 후보는 항상 조건검색 결과에만 의존한다. seq 연결이나
        # 실행 자체가 실패해도 universe_map 전체 스캔으로 되돌아가지 않고 후보 없음으로
        # 처리한다 — 그렇지 않으면 조건검색을 쓰는 이유(대량 KRX/네이버 조회 회피)가 없어진다.
        scan_targets = self.universe_map
        seq = self._resolve_condition_seq()
        if seq:
            try:
                matches = self.client.run_condition_search(seq, stex_tp="K")
            except Exception as e:
                print(f"⚠️ [중기형 조건검색] 실행 실패, 이번 회차는 후보 없음으로 처리합니다: {e}", flush=True)
                scan_targets = {}
            else:
                scan_targets = {m["ticker"]: (m["name"] or self.universe_map.get(m["ticker"], m["ticker"])) for m in matches}
                print(f"[중기형 조건검색] '{self.condition_name}' 매칭 {len(scan_targets)}종목", flush=True)
        elif self.condition_name:
            scan_targets = {}

        # 단기형에서 이미 보유 중인 종목 세트 조회
        short_held_tickers = self.get_short_positions_tickers()

        for ticker, name in scan_targets.items():
            # 1) 중기형 본인이 이미 들고 있으면 패스
            if ticker in self.positions:
                skip_counts["이미 보유"] += 1
                continue

            # 2) [신규] 단기형이 15:10에 먼저 샀거나 보유 중인 종목이면 패스
            if ticker in short_held_tickers:
                skip_counts["단기형 보유"] += 1
                if verbose:
                    print(f"ℹ️ [중기형 스킵] [{ticker} {name}] 단기형 전략에서 이미 보유 중인 종목입니다.", flush=True)
                continue

            try:
                quote = self.client.get_current_quote(ticker)
                close_p = quote["current_price"]
                open_p = quote["open_price"]
                if close_p < open_p:
                    skip_counts["상승 미달"] += 1
                    if verbose:
                        print(f"  [{ticker} {name}] 종가({close_p:,}) < 시가({open_p:,}) -> ❌ 미충족", flush=True)
                    continue

                df_hist = self._candidate_histories.get(ticker)
                if df_hist is None:
                    df_hist = self.client.get_daily_ohlcv(ticker, dt_20, today_str)
                if len(df_hist) < 20:
                    skip_counts["이력 부족"] += 1
                    continue

                df_hist = self.apply_live_quote(df_hist, quote)
                high_20 = df_hist["종가"].tail(20).max()
                drop_rate = (close_p - high_20) / high_20
                passed = drop_rate <= -self.oversold_drop_rate

                if verbose:
                    print(
                        f"  [{ticker} {name}] 현재가={close_p:,} 20일최고={int(high_20):,} "
                        f"낙폭={drop_rate*100:.1f}% (기준 -{self.oversold_drop_rate*100:.0f}%) -> "
                        f"{'✅ 후보 채택' if passed else '❌ 미충족'}",
                        flush=True,
                    )

                if passed:
                    candidates.append({"ticker": ticker, "name": name, "price": close_p, "drop_rate": drop_rate})
            except Exception as e:
                skip_counts["조회 오류"] += 1
                if verbose:
                    print(f"  [{ticker} {name}] 조회 오류: {e}", flush=True)
                continue

        if verbose:
            print(f"[중기형 스캔 요약] 대상 {len(self.universe_map)}종목 | 스킵: {skip_counts} | 후보: {len(candidates)}개", flush=True)

        candidates.sort(key=lambda x: x["drop_rate"])
        return candidates

    def run_daily_routine(self):
        """15시 이후 1회 호출되는 중기형 장마감 루틴"""
        now = datetime.now()
        today_str = now.strftime("%Y%m%d")

        print(f"\n[중기형 Ver2 종가 근접 루틴 가동]", flush=True)
        # 잔고 조회가 실패해도(네트워크 오류 등) 보유 종목 익절/리밸런싱 판단까지 막히면 안 되므로,
        # 여기서 실패하면 신규/추가매수 예산만 이번 회차에 비워두고 나머지는 계속 진행한다.
        try:
            bal = self.client.get_account_balance()
            total_equity = bal["total_equity"]
            mid_budget = (total_equity * self.capital_ratio) if total_equity > 0 else 10_000_000.0
            slot_budget = mid_budget / self.max_slots
            tranche_budget = slot_budget / self.max_tranches
        except Exception as e:
            print(f"⚠️ [중기형 마감] 잔고 조회 실패로 이번 회차 신규/추가매수는 건너뜁니다: {e}", flush=True)
            self.notifier.send_error("mid_balance_check", f"⚠️ [중기형 마감] 잔고 조회 실패로 신규/추가매수를 건너뛰었습니다: {e}")
            mid_budget = None
            tranche_budget = None

        # 1) 보유 종목 확인 및 익절/리밸런싱 처리
        closed_tickers = []
        for ticker, pos in list(self.positions.items()):
            try:
                quote = self.client.get_current_quote(ticker)
            except RuntimeError as e:
                print(f"❌ [중기형 청산 오류] {ticker}: {e}", flush=True)
                self.notifier.send_error(f"mid_closing_error:{ticker}", f"🚨 [중기형 청산 오류] {ticker}: 마감 루틴에서 이 종목의 매도 판단을 건너뛰었습니다: {e}", cooldown_seconds=0)
                continue

            pos["hold_days"] = int(pos.get("hold_days", 1)) + 1
            avg_p = float(pos["avg_price"])
            shares = int(pos["shares"])
            hold_d = pos["hold_days"]
            close_p = quote["current_price"]

            if hold_d <= self.early_holding_days:
                target_rate = self.early_target_rate
                exit_label = f"1단계 목표 익절(+{self.early_target_rate*100:.0f}%)"
            elif hold_d <= self.mid_holding_days:
                target_rate = self.mid_target_rate
                exit_label = f"2단계 조기 탈출(+{self.mid_target_rate*100:.0f}%)"
            else:
                target_rate = self.late_target_rate
                exit_label = f"3단계 장기 탈출(+{self.late_target_rate*100:.1f}%)"

            target_p = int(avg_p * (1 + target_rate))

            # 익절
            if close_p >= target_p:
                success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, shares, price=0, is_buy=False, strategy=self.strategy_name)
                if success:
                    real_exit = fill_p
                    proceeds = real_exit * fill_q * (1 - self.sell_fee_tax_rate)
                    cost_basis = avg_p * fill_q
                    pnl = proceeds - cost_basis
                    ret_pct = ((proceeds / cost_basis) - 1.0) * 100
                    msg = (
                        f"🎯 [중기형 익절 완료] {pos['name']} ({ticker})\n"
                        f"• {exit_label} 달성: {real_exit:,}원\n"
                        f"• 매도수량: {fill_q}주 | 실현수익률: {ret_pct:+.2f}% ({int(pnl):+,}원)"
                    )
                    print(msg, flush=True)
                    self.notifier.send(msg)
                    if fill_q >= shares:
                        closed_tickers.append(ticker)
                    else:
                        pos["shares"] -= fill_q
                        pos["invested"] = pos["shares"] * avg_p
                        self.save_positions()
                continue

            # 50% 리밸런싱
            if pos["tranches"] >= self.max_tranches and hold_d >= self.rebalance_days and not pos.get("rebalanced", False):
                sell_qty = int(shares * 0.5)
                if sell_qty > 0:
                    success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, sell_qty, price=0, is_buy=False, strategy=self.strategy_name)
                    if success:
                        pos["shares"] -= fill_q
                        pos["invested"] = pos["shares"] * avg_p
                        pos["avg_price"] = pos["invested"] / pos["shares"]
                        pos["rebalanced"] = True
                        pos["local_low"] = close_p
                        pos["tranches"] = 4
                        self.save_positions()
                        msg = f"⚖️ [중기형 50% 리밸런싱] {pos['name']} ({ticker}) 120일 경과 {fill_q}주 매도"
                        print(msg, flush=True)
                        self.notifier.send(msg)
                    continue

            # 턴어라운드 추가 매수
            if pos["tranches"] < self.max_tranches:
                curr_low = quote["low_price"]
                # curr_low<=0은 실시간 체결에 저가 필드가 아직 없어 캐시가 비어있는 경우다.
                # 이 값을 그대로 반영하면 local_low가 0으로 영구 저장되어(JSON에 저장됨) 이후
                # is_deep이 항상 참이 되는 상태로 포지션이 고착되므로 반드시 걸러야 한다.
                if curr_low > 0 and curr_low < pos.get("local_low", avg_p):
                    pos["local_low"] = curr_low

                is_deep = (pos["local_low"] <= avg_p * (1 - self.tranche_drop_threshold))
                # open_price도 같은 이유로 0이면 "오늘 양봉" 조건이 무의미하게 항상 참이 되므로 같이 걸러준다.
                is_rebound = (
                    quote["open_price"] > 0
                    and close_p >= pos["local_low"] * (1 + self.rebound_trigger_rate)
                    and close_p > quote["open_price"]
                )

                if is_deep and is_rebound and tranche_budget is not None:
                    next_tr = pos["tranches"] + 1
                    add_shares = int(tranche_budget // close_p) if close_p > 0 else 0
                    if add_shares > 0:
                        success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, add_shares, price=0, is_buy=True, strategy=self.strategy_name)
                        if success:
                            real_fill = fill_p if fill_p > 0 else close_p
                            real_q = fill_q if fill_q > 0 else add_shares
                            pos["shares"] += real_q
                            pos["invested"] += real_q * real_fill * (1 + self.buy_fee_rate)
                            pos["avg_price"] = pos["invested"] / pos["shares"]
                            pos["tranches"] = next_tr
                            pos["local_low"] = real_fill
                            self.save_positions()
                            msg = f"🌊 [중기형 {next_tr}차 매수] {pos['name']} ({ticker}) @ {real_fill:,}원"
                            print(msg, flush=True)
                            self.notifier.send(msg)

        for t in closed_tickers:
            if t in self.positions:
                del self.positions[t]
        if closed_tickers:
            self.save_positions()
            self.client.update_quote_subscription(remove=closed_tickers)

        # STEP 2: 신규 매수 대상 탐색
        open_slots = self.max_slots - len(self.positions)
        # verbose=True로 종목별 판단 근거(낙폭/기준 등)와 스캔 요약을 그대로 출력한다.
        # test_candidate_scan.py에서 쓰는 것과 동일한 메시지라 실전 로그에서도 왜 채택/탈락했는지 바로 보인다.
        chosen = self.scan_new_candidates(verbose=True)[:open_slots] if open_slots > 0 and tranche_budget is not None else []

        for c in chosen:
            ticker = c["ticker"]
            price = c["price"]
            buy_shares = int(tranche_budget // price) if price > 0 else 0
            if buy_shares > 0:
                success, fill_p, fill_q = self.client.execute_and_confirm_order(ticker, buy_shares, price=0, is_buy=True, strategy=self.strategy_name)
                if success:
                    real_p = fill_p if fill_p > 0 else price
                    real_q = fill_q if fill_q > 0 else buy_shares
                    self.positions[ticker] = {
                        "name": c["name"],
                        "entry_date": today_str,
                        "hold_days": 0,
                        "shares": real_q,
                        "invested": real_q * real_p * (1 + self.buy_fee_rate),
                        "avg_price": float(real_p * (1 + self.buy_fee_rate)),
                        "tranches": 1,
                        "local_low": real_p,
                        "rebalanced": False
                    }
                    self.save_positions()
                    self.client.update_quote_subscription(add=[ticker])
                    target_10 = int(real_p * (1 + self.early_target_rate))
                    msg = f"✨ [중기형 1차 신규매수] {c['name']} ({ticker}) @ {real_p:,}원 | 목표: {target_10:,}원"
                    print(msg, flush=True)
                    self.notifier.send(msg)

        # 텔레그램 일일 브리핑 발송
        summary = [f"📊 [중기형 Ver2 일일 브리핑] {today_str}"]
        budget_text = f"{mid_budget:,.0f}원" if mid_budget is not None else "조회 실패"
        summary.append(f"• 운용 슬롯: {len(self.positions)}/{self.max_slots}개 (할당자산: {budget_text})")
        for tk, p in self.positions.items():
            summary.append(f" - {p['name']}: {p['tranches']}/5차 | {p['hold_days']}일차 | 평단 {int(p['avg_price']):,}원")
        if not self.positions:
            summary.append("• 보유 종목 없음 (100% 현금 대기)")
        self.notifier.send("\n".join(summary))