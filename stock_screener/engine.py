from __future__ import annotations

import math
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import baostock as bs
import duckdb
import numpy as np
import pandas as pd

from .runtime import is_frozen_app, resolve_data_dir


A_SHARE_PREFIXES = (
    "sh.600",
    "sh.601",
    "sh.603",
    "sh.605",
    "sh.688",
    "sz.000",
    "sz.001",
    "sz.002",
    "sz.003",
    "sz.300",
    "sz.301",
)

UNIVERSE_LABELS = {
    "all_a": "沪深所有A股",
    "hs300": "沪深300",
    "zz500": "中证500",
    "sz50": "上证50",
}

UNIVERSE_FILES = {
    "hs300": "沪深300成分股_股票列表.csv",
    "zz500": "中证500成分股_股票列表.csv",
    "sz50": "上证50成分股_股票列表.csv",
    "all_a": "全部股票_股票列表.csv",
}

POSITIVE_FORECAST_TYPES = {
    "预增",
    "略增",
    "扭亏",
    "续盈",
    "预盈",
    "大幅上升",
    "大幅增长",
    "增长",
    "上升",
}


@dataclass
class StockInfo:
    code: str
    name: str


@dataclass
class ReviewParams:
    signal_date: str
    universe: str = "all_a"
    ema_period: int = 21
    enable_ema_breakout: bool = False
    volume_ma_window: int = 20
    enable_volume_ratio: bool = True
    volume_ratio_min: float = 3.0
    daily_k_angle_window: int = 5
    enable_daily_k_angle: bool = True
    daily_k_angle_min: float = 40.0
    relative_low_window: int = 120
    enable_relative_low: bool = True
    relative_low_position_max: float = 0.30
    enable_earnings_filter: bool = True
    earnings_forecast_change_min: float = 20.0
    earnings_yoy_min: float = 10.0
    enable_price_max: bool = True
    price_max: float = 50.0
    enable_turnover: bool = True
    turnover_min: float = 10.0
    hold_days: int = 0
    lookback_days: int = 160
    adjustflag: str = "2"
    exclude_st: bool = True


@dataclass
class SyncParams:
    universe: str = "all_a"
    start_date: str = "2024-01-01"
    end_date: str = date.today().isoformat()
    sync_history: bool = True
    sync_earnings: bool = False
    growth_quarters: int = 6
    adjustflag: str = "2"
    exclude_st: bool = True
    force_refresh: bool = False


@dataclass
class ReviewResult:
    code: str
    name: str
    universe_label: str
    signal_date: str
    signal_open: float
    signal_high: float
    signal_low: float
    signal_close: float
    signal_change_pct: float
    ema_value: float
    breakout_pct: float
    daily_k_slope_pct: float
    daily_k_angle: float
    relative_low_position: float
    earnings_pub_date: str
    earnings_signal: str
    volume_ratio: float
    turnover_rate: float
    volume: float
    amount: float
    buy_date: str
    buy_open: float
    sell_date: str
    sell_close: float
    hold_days: int
    hold_return_pct: float
    rule_label: str

    def to_dict(self) -> Dict:
        return asdict(self)


def normalize_date(raw_value: str) -> str:
    if len(raw_value) == 8 and raw_value.isdigit():
        return f"{raw_value[:4]}-{raw_value[4:6]}-{raw_value[6:]}"
    return raw_value


def to_bs_code(code: str) -> str:
    code = code.strip()
    if not code:
        return code
    lower_code = code.lower()
    if lower_code.startswith(("sh.", "sz.", "bj.")):
        return lower_code
    upper_code = code.upper()
    if upper_code.endswith((".SH", ".SZ", ".BJ")) and len(upper_code) >= 9:
        return f"{upper_code[-2:].lower()}.{upper_code[:6]}"
    return lower_code


def to_display_code(code: str) -> str:
    lower_code = code.lower()
    if lower_code.startswith(("sh.", "sz.", "bj.")) and len(lower_code) >= 9:
        return f"{lower_code[3:9]}.{lower_code[:2].upper()}"
    return code.upper()


def is_a_share(code: str) -> bool:
    return code.startswith(A_SHARE_PREFIXES)


def shift_day(raw_date: str, days: int) -> str:
    dt = datetime.strptime(normalize_date(raw_date), "%Y-%m-%d").date() + timedelta(days=days)
    return dt.isoformat()


def safe_mean(values: pd.Series) -> float:
    if values.empty:
        return 0.0
    return float(values.mean())


def safe_median(values: pd.Series) -> float:
    if values.empty:
        return 0.0
    return float(values.median())


def calculate_daily_k_slope_pct(values) -> float:
    window = np.asarray(values, dtype=float)
    if window.size == 0 or np.isnan(window).any():
        return float("nan")

    base_price = window[0]
    if not np.isfinite(base_price) or abs(base_price) < 1e-12:
        return float("nan")

    normalized = window / base_price
    x_axis = np.arange(window.size, dtype=float)
    slope = float(np.polyfit(x_axis, normalized, deg=1)[0])
    return slope * 100.0


def slope_pct_to_angle(slope_pct: float) -> float:
    if pd.isna(slope_pct):
        return float("nan")
    return float(math.degrees(math.atan(float(slope_pct))))


def iter_quarters_between(start_date: str, end_date: str) -> List[Tuple[int, int]]:
    start_dt = datetime.strptime(normalize_date(start_date), "%Y-%m-%d").date()
    end_dt = datetime.strptime(normalize_date(end_date), "%Y-%m-%d").date()
    current_year = start_dt.year
    current_quarter = (start_dt.month - 1) // 3 + 1
    end_year = end_dt.year
    end_quarter = (end_dt.month - 1) // 3 + 1

    quarters: List[Tuple[int, int]] = []
    while (current_year, current_quarter) <= (end_year, end_quarter):
        quarters.append((current_year, current_quarter))
        if current_quarter == 4:
            current_year += 1
            current_quarter = 1
        else:
            current_quarter += 1
    return quarters


def iter_recent_quarters(end_date: str, count: int) -> List[Tuple[int, int]]:
    count = max(1, int(count))
    end_dt = datetime.strptime(normalize_date(end_date), "%Y-%m-%d").date()
    year = end_dt.year
    quarter = (end_dt.month - 1) // 3 + 1

    quarters: List[Tuple[int, int]] = []
    for _ in range(count):
        quarters.append((year, quarter))
        if quarter == 1:
            year -= 1
            quarter = 4
        else:
            quarter -= 1
    quarters.reverse()
    return quarters


def quarter_bounds(year: int, quarter: int) -> Tuple[date, date]:
    month = (quarter - 1) * 3 + 1
    start_dt = date(year, month, 1)
    if quarter == 4:
        end_dt = date(year, 12, 31)
    else:
        next_month = month + 3
        end_dt = date(year, next_month, 1) - timedelta(days=1)
    return start_dt, end_dt


def format_percent_value(value: float, scale: float = 100.0) -> str:
    if pd.isna(value):
        return "--"
    return f"{float(value) * scale:.2f}%"


def sell_shift_from_hold_days(hold_days: int) -> int:
    return max(1, int(hold_days))


def build_rule_label(params: ReviewParams) -> str:
    rule_parts: List[str] = []
    if params.enable_ema_breakout:
        rule_parts.append(f"收盘上穿 EMA{params.ema_period}")
    if params.enable_volume_ratio:
        rule_parts.append(f"量比 >= {params.volume_ratio_min:.2f}")
    if params.enable_daily_k_angle:
        rule_parts.append(f"日K角度({params.daily_k_angle_window}日) >= {params.daily_k_angle_min:.2f}°")
    if params.enable_relative_low:
        rule_parts.append(f"近{params.relative_low_window}日区间位置 <= {params.relative_low_position_max:.2%}")
    if params.enable_earnings_filter:
        rule_parts.append(
            f"业绩预告 >= {params.earnings_forecast_change_min:.2f}% 或 快报/净利同比 >= {params.earnings_yoy_min:.2f}%"
        )
    if params.enable_price_max:
        rule_parts.append(f"股价 < {params.price_max:.2f}")
    if params.enable_turnover:
        rule_parts.append(f"换手率 >= {params.turnover_min:.2f}%")
    if not rule_parts:
        rule_parts.append("无额外过滤条件")
    return " 且 ".join(rule_parts)


class BaoStockSession:
    def __init__(self):
        self._logged_in = False

    def login(self, force: bool = False):
        if force:
            try:
                bs.logout()
            except Exception:
                pass
            self._logged_in = False

        if self._logged_in:
            return
        result = bs.login()
        if result.error_code != "0":
            raise RuntimeError(f"BaoStock 登录失败: {result.error_msg}")
        self._logged_in = True

    def logout(self):
        if self._logged_in:
            try:
                bs.logout()
            finally:
                self._logged_in = False

    def _should_retry_login(self, error_msg: str) -> bool:
        reconnect_tokens = (
            "未登录",
            "请先登录",
            "用户未登录",
            "网络接收错误",
            "接收数据异常",
            "连接断开",
        )
        return any(token in error_msg for token in reconnect_tokens)

    def _run_query(self, query_func, error_prefix: str):
        self.login()
        result = query_func()
        if result.error_code == "0":
            return result

        error_msg = result.error_msg or "未知错误"
        if self._should_retry_login(error_msg):
            self.login(force=True)
            result = query_func()
            if result.error_code == "0":
                return result
            error_msg = result.error_msg or error_msg

        raise RuntimeError(f"{error_prefix}: {error_msg}")

    def query_history(
        self,
        code: str,
        start_date: str,
        end_date: str,
        adjustflag: str = "2",
        frequency: str = "d",
    ) -> pd.DataFrame:
        fields = "date,code,open,high,low,close,volume,amount,turn,pctChg"
        result = self._run_query(
            lambda: bs.query_history_k_data_plus(
                code,
                fields,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency,
                adjustflag=adjustflag,
            ),
            f"{code} 拉取历史行情失败",
        )

        rows: List[List[str]] = []
        while result.next():
            rows.append(result.get_row_data())

        if not rows:
            return pd.DataFrame(columns=fields.split(","))

        df = pd.DataFrame(rows, columns=fields.split(","))
        numeric_cols = ["open", "high", "low", "close", "volume", "amount", "turn", "pctChg"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["date"] = pd.to_datetime(df["date"])
        df["code"] = df["code"].astype(str).str.lower()
        return df.sort_values("date").reset_index(drop=True)

    def query_all_stocks(self, day: str) -> List[StockInfo]:
        result = self._run_query(lambda: bs.query_all_stock(day=day), "获取股票列表失败")

        rows: List[StockInfo] = []
        while result.next():
            record = result.get_row_data()
            if len(record) < 3:
                continue
            rows.append(StockInfo(code=record[0].lower(), name=record[2]))
        return rows

    def query_component_stocks(self, universe: str) -> List[StockInfo]:
        method_name = {
            "hs300": "query_hs300_stocks",
            "zz500": "query_zz500_stocks",
            "sz50": "query_sz50_stocks",
        }.get(universe)
        if not method_name or not hasattr(bs, method_name):
            return []

        result = self._run_query(lambda: getattr(bs, method_name)(), f"获取 {universe} 成分股失败")

        rows: List[StockInfo] = []
        while result.next():
            record = result.get_row_data()
            if len(record) < 3:
                continue
            rows.append(StockInfo(code=record[1].lower(), name=record[2]))
        return rows

    def query_forecast_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        fields = [
            "code",
            "profitForcastExpPubDate",
            "profitForcastExpStatDate",
            "profitForcastType",
            "profitForcastAbstract",
            "profitForcastChgPctUp",
            "profitForcastChgPctDwn",
        ]
        result = self._run_query(
            lambda: bs.query_forecast_report(code, start_date=start_date, end_date=end_date),
            f"{code} 拉取业绩预告失败",
        )

        rows: List[List[str]] = []
        while result.next():
            rows.append(result.get_row_data())
        if not rows:
            return pd.DataFrame(columns=fields)

        frame = pd.DataFrame(rows, columns=fields)
        frame["code"] = frame["code"].astype(str).str.lower()
        frame["profitForcastExpPubDate"] = pd.to_datetime(frame["profitForcastExpPubDate"], errors="coerce")
        frame["profitForcastExpStatDate"] = pd.to_datetime(frame["profitForcastExpStatDate"], errors="coerce")
        frame["profitForcastChgPctUp"] = pd.to_numeric(frame["profitForcastChgPctUp"], errors="coerce")
        frame["profitForcastChgPctDwn"] = pd.to_numeric(frame["profitForcastChgPctDwn"], errors="coerce")
        return frame.sort_values(["profitForcastExpPubDate", "profitForcastExpStatDate"]).reset_index(drop=True)

    def query_performance_express_report(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        fields = [
            "code",
            "performanceExpPubDate",
            "performanceExpStatDate",
            "performanceExpUpdateDate",
            "performanceExpressTotalAsset",
            "performanceExpressNetAsset",
            "performanceExpressEPSChgPct",
            "performanceExpressROEWa",
            "performanceExpressEPSDiluted",
            "performanceExpressGRYOY",
            "performanceExpressOPYOY",
        ]
        result = self._run_query(
            lambda: bs.query_performance_express_report(code, start_date=start_date, end_date=end_date),
            f"{code} 拉取业绩快报失败",
        )

        rows: List[List[str]] = []
        while result.next():
            rows.append(result.get_row_data())
        if not rows:
            return pd.DataFrame(columns=fields)

        frame = pd.DataFrame(rows, columns=fields)
        frame["code"] = frame["code"].astype(str).str.lower()
        for col in ("performanceExpPubDate", "performanceExpStatDate", "performanceExpUpdateDate"):
            frame[col] = pd.to_datetime(frame[col], errors="coerce")
        numeric_cols = [
            "performanceExpressTotalAsset",
            "performanceExpressNetAsset",
            "performanceExpressEPSChgPct",
            "performanceExpressROEWa",
            "performanceExpressEPSDiluted",
            "performanceExpressGRYOY",
            "performanceExpressOPYOY",
        ]
        for col in numeric_cols:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame.sort_values(["performanceExpPubDate", "performanceExpStatDate"]).reset_index(drop=True)

    def query_growth_data(self, code: str, year: int, quarter: int) -> pd.DataFrame:
        fields = ["code", "pubDate", "statDate", "YOYEquity", "YOYAsset", "YOYNI", "YOYEPSBasic", "YOYPNI"]
        result = self._run_query(
            lambda: bs.query_growth_data(code=code, year=year, quarter=quarter),
            f"{code} 拉取成长数据失败",
        )

        rows: List[List[str]] = []
        while result.next():
            rows.append(result.get_row_data())
        if not rows:
            return pd.DataFrame(columns=fields)

        frame = pd.DataFrame(rows, columns=fields)
        frame["code"] = frame["code"].astype(str).str.lower()
        frame["pubDate"] = pd.to_datetime(frame["pubDate"], errors="coerce")
        frame["statDate"] = pd.to_datetime(frame["statDate"], errors="coerce")
        for col in ("YOYEquity", "YOYAsset", "YOYNI", "YOYEPSBasic", "YOYPNI"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame.sort_values(["pubDate", "statDate"]).reset_index(drop=True)


class ReviewEngine:
    DB_FILENAME = "market_data.duckdb"

    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        if is_frozen_app():
            self.package_dir = self.project_root
            self.cache_dir = self.project_root / "cache"
            self.report_dir = self.project_root / "reports"
        else:
            self.package_dir = self.project_root / "stock_screener"
            self.cache_dir = self.package_dir / "cache"
            self.report_dir = self.package_dir / "reports"
        self.legacy_cache_dir = self.cache_dir / "daily"
        self.data_dir = resolve_data_dir(self.project_root)
        self.db_path = self.cache_dir / self.DB_FILENAME

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.legacy_cache_dir.mkdir(parents=True, exist_ok=True)
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def screen(
        self,
        params: ReviewParams,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        is_cancelled: Optional[Callable[[], bool]] = None,
    ) -> Dict:
        params = self._normalize_params(params)
        total_steps = 4 if params.enable_earnings_filter else 3

        with self._connect() as conn:
            self._ensure_database_ready(conn, progress_callback=progress_callback, is_cancelled=is_cancelled)

            if is_cancelled and is_cancelled():
                raise RuntimeError("筛选已取消。")

            if progress_callback:
                progress_callback(1, total_steps, "正在准备股票池...")
            universe_df = self._load_universe_frame(conn, params.universe, params.signal_date, params.exclude_st)
            if universe_df.empty:
                raise RuntimeError("股票池为空，无法筛选。请先同步数据。")

            if is_cancelled and is_cancelled():
                raise RuntimeError("筛选已取消。")

            if progress_callback:
                progress_callback(2, total_steps, "正在从本地数据库读取日线数据...")
            history = self._load_history_frame(conn, params, universe_df)
            if history.empty:
                raise RuntimeError("本地数据库没有命中范围内的历史数据，请先同步对应日期区间。")

            if is_cancelled and is_cancelled():
                raise RuntimeError("筛选已取消。")

            earnings_df = pd.DataFrame()
            if params.enable_earnings_filter:
                if progress_callback:
                    progress_callback(3, total_steps, "正在准备业绩预告、快报和成长数据...")
                earnings_df = self._load_latest_earnings_frame(conn, params.signal_date, universe_df)
                if earnings_df.empty or earnings_df[["forecast_pub_date", "express_pub_date", "growth_pub_date"]].isna().all().all():
                    raise RuntimeError("当前本地数据库没有可用的业绩数据，请先点击“同步数据”更新。")

            if is_cancelled and is_cancelled():
                raise RuntimeError("筛选已取消。")

            if progress_callback:
                progress_callback(total_steps, total_steps, "正在批量计算条件并生成结果...")
            result_df = self._evaluate_universe(history, params, earnings_df)

        if result_df.empty:
            return {
                "params": asdict(params),
                "rows": [],
                "summary": self._build_summary(result_df, params),
                "report_path": "",
            }

        result_df = result_df.sort_values(
            by=["hold_return_pct", "turnover_rate", "volume_ratio", "daily_k_angle", "breakout_pct"],
            ascending=[False, False, False, False, False],
            na_position="last",
        ).reset_index(drop=True)

        report_path = self._save_report(result_df, params)
        return {
            "params": asdict(params),
            "rows": result_df.to_dict(orient="records"),
            "summary": self._build_summary(result_df, params),
            "report_path": str(report_path),
        }

    def sync_data(
        self,
        params: SyncParams,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        is_cancelled: Optional[Callable[[], bool]] = None,
    ) -> Dict:
        params = self._normalize_sync_params(params)
        if not params.sync_history and not params.sync_earnings:
            raise RuntimeError("请至少勾选一种同步内容。")
        created_count = 0
        updated_count = 0
        reused_count = 0
        processed_count = 0
        failed_rows: List[Dict[str, str]] = []
        financial_start_date = shift_day(params.start_date, -370)
        sync_parts: List[str] = []
        if params.sync_history:
            sync_parts.append("日线")
        if params.sync_earnings:
            sync_parts.append(f"业绩(近{params.growth_quarters}季成长)")
        sync_label = "+".join(sync_parts)

        with self._connect() as conn:
            self._ensure_database_ready(conn, progress_callback=progress_callback, is_cancelled=is_cancelled)

            with self._session() as client:
                universe = self._load_universe_from_source(client, params)
                if not universe:
                    raise RuntimeError("股票池为空，无法同步。")

                self._upsert_stock_master(
                    conn,
                    universe,
                    last_seen=params.end_date,
                    prefer_existing=False,
                )
                self._replace_universe_snapshot(conn, params.universe, params.end_date, universe)

                total = len(universe)
                for index, item in enumerate(universe, start=1):
                    if is_cancelled and is_cancelled():
                        raise RuntimeError("数据同步已取消。")

                    if progress_callback:
                        progress_callback(index, total, f"[{index}/{total}] 同步 {item.code} {item.name}（{sync_label}）")

                    try:
                        sync_status = "skipped"
                        if params.sync_history:
                            sync_status = self._sync_symbol_history(
                                conn,
                                client,
                                item.code,
                                params.start_date,
                                params.end_date,
                                params.adjustflag,
                                params.force_refresh,
                            )
                        if params.sync_earnings:
                            self._sync_symbol_earnings(
                                conn,
                                client,
                                item.code,
                                financial_start_date,
                                params.end_date,
                                params.force_refresh,
                                params.growth_quarters,
                            )
                        processed_count += 1
                        if sync_status == "created":
                            created_count += 1
                        elif sync_status == "updated":
                            updated_count += 1
                        elif sync_status == "reused":
                            reused_count += 1
                    except Exception as exc:
                        failed_rows.append(
                            {
                                "code": to_display_code(item.code),
                                "name": item.name,
                                "error": str(exc),
                            }
                        )

            cache_status = self._get_cache_status_from_conn(conn)

        return {
            "params": asdict(params),
            "summary": {
                "total": len(universe),
                "created": created_count,
                "updated": updated_count,
                "reused": reused_count,
                "processed": processed_count,
                "failed": len(failed_rows),
                "cache_count": cache_status["cache_count"],
                "latest_date": cache_status["latest_date"],
                "range": f"{params.start_date} ~ {params.end_date}",
                "universe_label": UNIVERSE_LABELS.get(params.universe, params.universe),
                "sync_history": params.sync_history,
                "sync_earnings": params.sync_earnings,
                "growth_quarters": params.growth_quarters,
                "sync_label": sync_label,
                "force_refresh": params.force_refresh,
            },
            "failed_rows": failed_rows,
        }

    def get_cache_status(self) -> Dict[str, str | int]:
        try:
            with self._connect() as conn:
                if self._database_has_bars(conn):
                    return self._get_cache_status_from_conn(conn)
        except RuntimeError:
            pass

        latest_date = ""
        cache_count = 0
        for file_path in self.legacy_cache_dir.glob("*.csv"):
            cache_count += 1
            tail_date = self._read_last_cached_date(file_path)
            if tail_date and tail_date > latest_date:
                latest_date = tail_date
        return {"cache_count": cache_count, "latest_date": latest_date}

    def load_chart_payload(self, code: str, params: ReviewParams) -> Dict:
        params = self._normalize_params(params)
        bs_code = to_bs_code(code)
        start_date, end_date = self._build_history_range(params)

        with self._connect() as conn:
            self._ensure_database_ready(conn)
            history = conn.execute(
                """
                SELECT
                    trade_date AS date,
                    code,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    amount,
                    turn,
                    pct_chg AS pctChg
                FROM daily_bars
                WHERE code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date
                """,
                [bs_code, start_date, end_date],
            ).fetchdf()

        if history.empty:
            raise RuntimeError(f"未找到 {to_display_code(bs_code)} 的本地数据库数据，请先同步。")

        history["date"] = pd.to_datetime(history["date"])
        enriched = self._enrich_history(history, params)
        signal_ts = pd.Timestamp(params.signal_date)
        matches = enriched.index[enriched["date"] == signal_ts]
        if len(matches) == 0:
            raise RuntimeError(f"{to_display_code(bs_code)} 在 {params.signal_date} 没有交易数据。")

        signal_index = int(matches[0])
        buy_index = signal_index + 1
        sell_index = buy_index + sell_shift_from_hold_days(params.hold_days) - 1
        if sell_index >= len(enriched):
            raise RuntimeError(f"{to_display_code(bs_code)} 的未来数据不足，无法绘制买卖点。")

        start_index = max(0, signal_index - 45)
        end_index = min(len(enriched), sell_index + 15)
        frame = enriched.iloc[start_index:end_index].copy().reset_index(drop=True)

        return {
            "frame": frame,
            "signal_date": params.signal_date,
            "buy_date": enriched.iloc[buy_index]["date"].date().isoformat(),
            "sell_date": enriched.iloc[sell_index]["date"].date().isoformat(),
            "ema_period": params.ema_period,
            "volume_ma_window": params.volume_ma_window,
            "daily_k_angle_window": params.daily_k_angle_window,
            "relative_low_window": params.relative_low_window,
            "hold_days": params.hold_days,
            "code": to_display_code(bs_code),
        }

    def _normalize_params(self, params: ReviewParams) -> ReviewParams:
        params.signal_date = normalize_date(params.signal_date)
        params.universe = params.universe if params.universe in UNIVERSE_LABELS else "all_a"
        params.ema_period = max(2, int(params.ema_period))
        params.volume_ma_window = max(2, int(params.volume_ma_window))
        params.daily_k_angle_window = max(2, int(params.daily_k_angle_window))
        params.relative_low_window = max(2, int(params.relative_low_window))
        params.hold_days = max(0, int(params.hold_days))
        params.lookback_days = max(60, int(params.lookback_days))
        params.enable_ema_breakout = bool(params.enable_ema_breakout)
        params.enable_volume_ratio = bool(params.enable_volume_ratio)
        params.enable_daily_k_angle = bool(params.enable_daily_k_angle)
        params.enable_relative_low = bool(params.enable_relative_low)
        params.enable_earnings_filter = bool(params.enable_earnings_filter)
        params.enable_price_max = bool(params.enable_price_max)
        params.enable_turnover = bool(params.enable_turnover)
        params.volume_ratio_min = float(params.volume_ratio_min)
        params.daily_k_angle_min = float(params.daily_k_angle_min)
        params.relative_low_position_max = min(1.0, max(0.0, float(params.relative_low_position_max)))
        params.earnings_forecast_change_min = max(0.0, float(params.earnings_forecast_change_min))
        params.earnings_yoy_min = max(0.0, float(params.earnings_yoy_min))
        params.price_max = max(0.0, float(params.price_max))
        params.turnover_min = max(0.0, float(params.turnover_min))
        params.exclude_st = bool(params.exclude_st)
        return params

    def _normalize_sync_params(self, params: SyncParams) -> SyncParams:
        params.universe = params.universe if params.universe in UNIVERSE_LABELS else "all_a"
        params.start_date = normalize_date(params.start_date)
        params.end_date = normalize_date(params.end_date)
        params.sync_history = bool(params.sync_history)
        params.sync_earnings = bool(params.sync_earnings)
        params.growth_quarters = max(1, min(24, int(params.growth_quarters)))
        params.force_refresh = bool(params.force_refresh)
        params.exclude_st = bool(params.exclude_st)
        return params

    def _connect(self):
        try:
            conn = duckdb.connect(str(self.db_path))
        except duckdb.IOException as exc:
            raise RuntimeError("本地数据库正在被其他进程占用，请先关闭重复打开的窗口后重试。") from exc
        thread_count = max(2, min(8, os.cpu_count() or 4))
        conn.execute(f"PRAGMA threads={thread_count}")
        self._ensure_schema(conn)
        return conn

    def _ensure_schema(self, conn):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_bars (
                code VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                amount DOUBLE,
                turn DOUBLE,
                pct_chg DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS stock_master (
                code VARCHAR,
                name VARCHAR,
                is_st BOOLEAN,
                last_seen DATE,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_members (
                universe VARCHAR,
                snapshot_date DATE,
                code VARCHAR,
                name VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cache_summary (
                code VARCHAR,
                start_date DATE,
                end_date DATE,
                bar_count BIGINT,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS forecast_reports (
                code VARCHAR,
                pub_date DATE,
                stat_date DATE,
                forecast_type VARCHAR,
                forecast_abstract VARCHAR,
                chg_pct_up DOUBLE,
                chg_pct_dwn DOUBLE,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS performance_express_reports (
                code VARCHAR,
                pub_date DATE,
                stat_date DATE,
                update_date DATE,
                total_asset DOUBLE,
                net_asset DOUBLE,
                eps_chg_pct DOUBLE,
                roe_wa DOUBLE,
                eps_diluted DOUBLE,
                gryoy DOUBLE,
                opyoy DOUBLE,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS growth_reports (
                code VARCHAR,
                pub_date DATE,
                stat_date DATE,
                yoy_equity DOUBLE,
                yoy_asset DOUBLE,
                yoy_ni DOUBLE,
                yoy_eps_basic DOUBLE,
                yoy_pni DOUBLE,
                updated_at TIMESTAMP
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_bars_code_date ON daily_bars(code, trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_bars_trade_date ON daily_bars(trade_date)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_stock_master_code ON stock_master(code)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_universe_members_lookup ON universe_members(universe, snapshot_date, code)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_summary_code ON cache_summary(code)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forecast_reports_lookup ON forecast_reports(code, pub_date)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_performance_express_reports_lookup ON performance_express_reports(code, pub_date)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_growth_reports_lookup ON growth_reports(code, pub_date)")

    def _ensure_database_ready(
        self,
        conn,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        is_cancelled: Optional[Callable[[], bool]] = None,
    ):
        if self._database_has_bars(conn):
            self._seed_stock_master_from_local_files(conn, only_missing=True)
            if self._summary_count(conn) == 0:
                self._rebuild_cache_summary(conn)
            return

        legacy_files = list(self.legacy_cache_dir.glob("*.csv"))
        if not legacy_files:
            return

        if is_cancelled and is_cancelled():
            raise RuntimeError("任务已取消。")

        if progress_callback:
            progress_callback(1, 1, "正在把旧 CSV 缓存迁移到本地数据库，首次只需一次...")

        pattern = (self.legacy_cache_dir / "*.csv").as_posix().replace("'", "''")
        conn.execute("DELETE FROM daily_bars")
        conn.execute(
            f"""
            INSERT INTO daily_bars
            SELECT
                lower(code) AS code,
                CAST(date AS DATE) AS trade_date,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS DOUBLE) AS volume,
                CAST(amount AS DOUBLE) AS amount,
                CAST(turn AS DOUBLE) AS turn,
                CAST(pctChg AS DOUBLE) AS pct_chg
            FROM read_csv_auto('{pattern}', header=True, union_by_name=True, ignore_errors=true)
            WHERE code IS NOT NULL AND date IS NOT NULL
            """
        )
        self._seed_stock_master_from_local_files(conn, only_missing=False)
        self._ensure_codes_exist_in_stock_master(conn)
        self._rebuild_cache_summary(conn)

    def _database_has_bars(self, conn) -> bool:
        return int(conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]) > 0

    def _summary_count(self, conn) -> int:
        return int(conn.execute("SELECT COUNT(*) FROM cache_summary").fetchone()[0])

    def _load_local_stock_seed(self) -> pd.DataFrame:
        rows = self._load_universe_from_file("all_a")
        if not rows:
            return pd.DataFrame(columns=["code", "name", "is_st", "last_seen", "updated_at"])
        frame = pd.DataFrame(
            {
                "code": [item.code for item in rows],
                "name": [item.name or "--" for item in rows],
            }
        ).drop_duplicates(subset=["code"], keep="last")
        frame["is_st"] = frame["name"].str.upper().str.contains("ST", na=False)
        frame["last_seen"] = pd.NaT
        frame["updated_at"] = pd.Timestamp.now()
        return frame

    def _seed_stock_master_from_local_files(self, conn, only_missing: bool):
        seed_df = self._load_local_stock_seed()
        if seed_df.empty:
            return

        conn.register("stock_seed_df", seed_df)
        if only_missing:
            conn.execute(
                """
                INSERT INTO stock_master (code, name, is_st, last_seen, updated_at)
                SELECT s.code, s.name, s.is_st, s.last_seen, s.updated_at
                FROM stock_seed_df AS s
                LEFT JOIN stock_master AS m ON m.code = s.code
                WHERE m.code IS NULL
                """
            )
        else:
            conn.execute("DELETE FROM stock_master WHERE code IN (SELECT code FROM stock_seed_df)")
            conn.execute(
                """
                INSERT INTO stock_master (code, name, is_st, last_seen, updated_at)
                SELECT code, name, is_st, last_seen, updated_at
                FROM stock_seed_df
                """
            )
        conn.unregister("stock_seed_df")

    def _ensure_codes_exist_in_stock_master(self, conn):
        conn.execute(
            """
            INSERT INTO stock_master (code, name, is_st, last_seen, updated_at)
            SELECT DISTINCT
                b.code,
                '--' AS name,
                FALSE AS is_st,
                NULL::DATE AS last_seen,
                CURRENT_TIMESTAMP
            FROM daily_bars AS b
            LEFT JOIN stock_master AS m ON m.code = b.code
            WHERE m.code IS NULL
            """
        )

    def _rebuild_cache_summary(self, conn):
        conn.execute("DELETE FROM cache_summary")
        conn.execute(
            """
            INSERT INTO cache_summary (code, start_date, end_date, bar_count, updated_at)
            SELECT
                code,
                MIN(trade_date) AS start_date,
                MAX(trade_date) AS end_date,
                COUNT(*) AS bar_count,
                CURRENT_TIMESTAMP
            FROM daily_bars
            GROUP BY code
            """
        )

    def _refresh_code_summary(self, conn, code: str):
        conn.execute("DELETE FROM cache_summary WHERE code = ?", [code])
        conn.execute(
            """
            INSERT INTO cache_summary (code, start_date, end_date, bar_count, updated_at)
            SELECT
                code,
                MIN(trade_date) AS start_date,
                MAX(trade_date) AS end_date,
                COUNT(*) AS bar_count,
                CURRENT_TIMESTAMP
            FROM daily_bars
            WHERE code = ?
            GROUP BY code
            """,
            [code],
        )

    def _get_cache_status_from_conn(self, conn) -> Dict[str, str | int]:
        row = conn.execute(
            "SELECT COALESCE(COUNT(*), 0) AS cache_count, MAX(end_date) AS latest_date FROM cache_summary"
        ).fetchone()
        latest_date = ""
        if row and row[1] is not None:
            latest_date = row[1].isoformat() if hasattr(row[1], "isoformat") else str(row[1])
        return {"cache_count": int(row[0]) if row else 0, "latest_date": latest_date}

    def _load_universe_from_source(self, client: BaoStockSession, params: SyncParams) -> List[StockInfo]:
        if params.universe == "all_a":
            reference_day = datetime.strptime(params.end_date, "%Y-%m-%d").date()
            rows: List[StockInfo] = []
            for _ in range(10):
                rows = client.query_all_stocks(reference_day.isoformat())
                if rows:
                    break
                reference_day -= timedelta(days=1)
            universe = [item for item in rows if is_a_share(item.code)]
        else:
            universe = client.query_component_stocks(params.universe)
            if not universe:
                universe = self._load_universe_from_file(params.universe)

        if params.exclude_st:
            universe = [item for item in universe if "ST" not in (item.name or "").upper()]
        return universe

    def _load_universe_frame(self, conn, universe: str, reference_date: str, exclude_st: bool) -> pd.DataFrame:
        if universe == "all_a":
            frame = conn.execute(
                """
                SELECT DISTINCT code, name, is_st
                FROM stock_master
                WHERE code LIKE 'sh.%' OR code LIKE 'sz.%'
                ORDER BY code
                """
            ).fetchdf()
        else:
            snapshot_date = conn.execute(
                """
                SELECT MAX(snapshot_date)
                FROM universe_members
                WHERE universe = ? AND snapshot_date <= ?
                """,
                [universe, reference_date],
            ).fetchone()[0]

            if snapshot_date is not None:
                frame = conn.execute(
                    """
                    SELECT u.code, COALESCE(m.name, u.name, '--') AS name, COALESCE(m.is_st, FALSE) AS is_st
                    FROM universe_members AS u
                    LEFT JOIN stock_master AS m ON m.code = u.code
                    WHERE u.universe = ? AND u.snapshot_date = ?
                    ORDER BY u.code
                    """,
                    [universe, snapshot_date],
                ).fetchdf()
            else:
                frame = pd.DataFrame(
                    {
                        "code": [item.code for item in self._load_universe_from_file(universe)],
                        "name": [item.name for item in self._load_universe_from_file(universe)],
                    }
                )
                if frame.empty:
                    return frame
                frame["is_st"] = frame["name"].str.upper().str.contains("ST", na=False)

        if frame.empty:
            return frame

        frame["name"] = frame["name"].fillna("--")
        frame["is_st"] = frame["is_st"].fillna(False)
        if exclude_st:
            frame = frame.loc[~frame["is_st"].astype(bool)].copy()
        return frame.drop_duplicates(subset=["code"], keep="last").reset_index(drop=True)

    def _load_universe_from_file(self, universe: str) -> List[StockInfo]:
        file_name = UNIVERSE_FILES.get(universe)
        if not file_name:
            return []
        file_path = self.data_dir / file_name
        if not file_path.exists():
            return []

        rows: List[StockInfo] = []
        for encoding in ("utf-8-sig", "utf-8", "gbk"):
            try:
                with open(file_path, "r", encoding=encoding) as handle:
                    lines = handle.readlines()
                break
            except UnicodeDecodeError:
                continue
        else:
            return []

        for raw_line in lines:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [part.strip() for part in line.split(",")]
            code = to_bs_code(parts[0])
            if not is_a_share(code):
                continue
            name = parts[1] if len(parts) > 1 else "--"
            rows.append(StockInfo(code=code, name=name))
        return rows

    def _upsert_stock_master(
        self,
        conn,
        rows: List[StockInfo],
        last_seen: Optional[str],
        prefer_existing: bool,
    ):
        if not rows:
            return

        frame = pd.DataFrame(
            {
                "code": [item.code for item in rows],
                "name": [item.name or "--" for item in rows],
            }
        ).drop_duplicates(subset=["code"], keep="last")
        frame["is_st"] = frame["name"].str.upper().str.contains("ST", na=False)
        frame["last_seen"] = pd.to_datetime(last_seen).date() if last_seen else pd.NaT
        frame["updated_at"] = pd.Timestamp.now()

        conn.register("stock_update_df", frame)
        if prefer_existing:
            conn.execute(
                """
                INSERT INTO stock_master (code, name, is_st, last_seen, updated_at)
                SELECT s.code, s.name, s.is_st, s.last_seen, s.updated_at
                FROM stock_update_df AS s
                LEFT JOIN stock_master AS m ON m.code = s.code
                WHERE m.code IS NULL
                """
            )
            conn.execute(
                """
                UPDATE stock_master AS m
                SET
                    name = s.name,
                    is_st = s.is_st,
                    last_seen = COALESCE(s.last_seen, m.last_seen),
                    updated_at = s.updated_at
                FROM stock_update_df AS s
                WHERE
                    m.code = s.code
                    AND (m.name IS NULL OR m.name = '' OR m.name = '--')
                """
            )
        else:
            conn.execute("DELETE FROM stock_master WHERE code IN (SELECT code FROM stock_update_df)")
            conn.execute(
                """
                INSERT INTO stock_master (code, name, is_st, last_seen, updated_at)
                SELECT code, name, is_st, last_seen, updated_at
                FROM stock_update_df
                """
            )
        conn.unregister("stock_update_df")

    def _replace_universe_snapshot(self, conn, universe: str, snapshot_date: str, rows: List[StockInfo]):
        if universe == "all_a" or not rows:
            return

        frame = pd.DataFrame(
            {
                "universe": universe,
                "snapshot_date": pd.to_datetime(snapshot_date).date(),
                "code": [item.code for item in rows],
                "name": [item.name or "--" for item in rows],
            }
        ).drop_duplicates(subset=["code"], keep="last")

        conn.register("universe_df", frame)
        conn.execute("DELETE FROM universe_members WHERE universe = ? AND snapshot_date = ?", [universe, snapshot_date])
        conn.execute(
            """
            INSERT INTO universe_members (universe, snapshot_date, code, name)
            SELECT universe, snapshot_date, code, name
            FROM universe_df
            """
        )
        conn.unregister("universe_df")

    def _load_latest_earnings_frame(self, conn, signal_date: str, universe_df: pd.DataFrame) -> pd.DataFrame:
        codes_df = universe_df[["code"]].drop_duplicates(subset=["code"]).copy()
        if codes_df.empty:
            return codes_df

        conn.register("earnings_universe_df", codes_df)
        forecast_df = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    f.code,
                    f.pub_date AS forecast_pub_date,
                    f.stat_date AS forecast_stat_date,
                    f.forecast_type,
                    f.forecast_abstract,
                    f.chg_pct_up AS forecast_chg_pct_up,
                    f.chg_pct_dwn AS forecast_chg_pct_dwn,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.code
                        ORDER BY f.pub_date DESC, f.stat_date DESC
                    ) AS rn
                FROM forecast_reports AS f
                INNER JOIN earnings_universe_df AS u ON u.code = f.code
                WHERE f.pub_date <= ?
            )
            SELECT
                code,
                forecast_pub_date,
                forecast_stat_date,
                forecast_type,
                forecast_abstract,
                forecast_chg_pct_up,
                forecast_chg_pct_dwn
            FROM ranked
            WHERE rn = 1
            """,
            [signal_date],
        ).fetchdf()
        express_df = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    e.code,
                    e.pub_date AS express_pub_date,
                    e.stat_date AS express_stat_date,
                    e.update_date AS express_update_date,
                    e.gryoy AS express_gryoy,
                    e.opyoy AS express_opyoy,
                    ROW_NUMBER() OVER (
                        PARTITION BY e.code
                        ORDER BY e.pub_date DESC, e.stat_date DESC, e.update_date DESC
                    ) AS rn
                FROM performance_express_reports AS e
                INNER JOIN earnings_universe_df AS u ON u.code = e.code
                WHERE e.pub_date <= ?
            )
            SELECT
                code,
                express_pub_date,
                express_stat_date,
                express_update_date,
                express_gryoy,
                express_opyoy
            FROM ranked
            WHERE rn = 1
            """,
            [signal_date],
        ).fetchdf()
        growth_df = conn.execute(
            """
            WITH ranked AS (
                SELECT
                    g.code,
                    g.pub_date AS growth_pub_date,
                    g.stat_date AS growth_stat_date,
                    g.yoy_ni AS growth_yoy_ni,
                    ROW_NUMBER() OVER (
                        PARTITION BY g.code
                        ORDER BY g.pub_date DESC, g.stat_date DESC
                    ) AS rn
                FROM growth_reports AS g
                INNER JOIN earnings_universe_df AS u ON u.code = g.code
                WHERE g.pub_date <= ?
            )
            SELECT
                code,
                growth_pub_date,
                growth_stat_date,
                growth_yoy_ni
            FROM ranked
            WHERE rn = 1
            """,
            [signal_date],
        ).fetchdf()
        conn.unregister("earnings_universe_df")

        merged = codes_df.merge(forecast_df, on="code", how="left")
        merged = merged.merge(express_df, on="code", how="left")
        merged = merged.merge(growth_df, on="code", how="left")

        for col in [
            "forecast_pub_date",
            "forecast_stat_date",
            "express_pub_date",
            "express_stat_date",
            "express_update_date",
            "growth_pub_date",
            "growth_stat_date",
        ]:
            if col in merged.columns:
                merged[col] = pd.to_datetime(merged[col], errors="coerce")
        return merged

    def _sync_symbol_earnings(
        self,
        conn,
        client: BaoStockSession,
        code: str,
        start_date: str,
        end_date: str,
        force_refresh: bool,
        growth_quarters: int,
    ):
        self._sync_symbol_forecast_reports(conn, client, code, start_date, end_date, force_refresh)
        self._sync_symbol_performance_express_reports(conn, client, code, start_date, end_date, force_refresh)
        self._sync_symbol_growth_reports(conn, client, code, start_date, end_date, force_refresh, growth_quarters)

    def _sync_symbol_forecast_reports(
        self,
        conn,
        client: BaoStockSession,
        code: str,
        start_date: str,
        end_date: str,
        force_refresh: bool,
    ):
        if force_refresh:
            conn.execute(
                "DELETE FROM forecast_reports WHERE code = ? AND pub_date BETWEEN ? AND ?",
                [code, start_date, end_date],
            )
            query_start = start_date
        else:
            latest_row = conn.execute("SELECT MAX(pub_date) FROM forecast_reports WHERE code = ?", [code]).fetchone()
            latest_pub_date = latest_row[0] if latest_row else None
            if latest_pub_date is None:
                query_start = start_date
            else:
                query_start = max(start_date, shift_day(latest_pub_date.isoformat(), 1))
                if query_start > end_date:
                    return

        frame = client.query_forecast_report(code, query_start, end_date)
        self._merge_forecast_reports(conn, frame)

    def _sync_symbol_performance_express_reports(
        self,
        conn,
        client: BaoStockSession,
        code: str,
        start_date: str,
        end_date: str,
        force_refresh: bool,
    ):
        if force_refresh:
            conn.execute(
                "DELETE FROM performance_express_reports WHERE code = ? AND pub_date BETWEEN ? AND ?",
                [code, start_date, end_date],
            )
            query_start = start_date
        else:
            latest_row = conn.execute(
                "SELECT MAX(pub_date) FROM performance_express_reports WHERE code = ?",
                [code],
            ).fetchone()
            latest_pub_date = latest_row[0] if latest_row else None
            if latest_pub_date is None:
                query_start = start_date
            else:
                query_start = max(start_date, shift_day(latest_pub_date.isoformat(), 1))
                if query_start > end_date:
                    return

        frame = client.query_performance_express_report(code, query_start, end_date)
        self._merge_performance_express_reports(conn, frame)

    def _sync_symbol_growth_reports(
        self,
        conn,
        client: BaoStockSession,
        code: str,
        start_date: str,
        end_date: str,
        force_refresh: bool,
        growth_quarters: int,
    ):
        recent_quarters = iter_recent_quarters(end_date, growth_quarters)
        quarter_floor = recent_quarters[0]
        quarter_start_floor, _ = quarter_bounds(*quarter_floor)
        bounded_start_date = max(start_date, quarter_start_floor.isoformat())
        quarters = iter_quarters_between(bounded_start_date, end_date)
        if not quarters:
            return

        quarter_start, _ = quarter_bounds(*quarters[0])
        _, quarter_end = quarter_bounds(*quarters[-1])
        quarter_start_text = quarter_start.isoformat()
        quarter_end_text = quarter_end.isoformat()

        existing_quarters = set()
        if force_refresh:
            conn.execute(
                "DELETE FROM growth_reports WHERE code = ? AND stat_date BETWEEN ? AND ?",
                [code, quarter_start_text, quarter_end_text],
            )
        else:
            rows = conn.execute(
                "SELECT stat_date FROM growth_reports WHERE code = ? AND stat_date BETWEEN ? AND ?",
                [code, quarter_start_text, quarter_end_text],
            ).fetchall()
            for (stat_date,) in rows:
                if stat_date is None:
                    continue
                existing_quarters.add((stat_date.year, (stat_date.month - 1) // 3 + 1))

        for year, quarter in quarters:
            if (year, quarter) in existing_quarters:
                continue
            frame = client.query_growth_data(code, year, quarter)
            self._merge_growth_reports(conn, frame)

    def _merge_forecast_reports(self, conn, frame: pd.DataFrame):
        if frame.empty:
            return

        insert_df = frame.copy()
        insert_df = insert_df.dropna(subset=["code", "profitForcastExpPubDate"])
        if insert_df.empty:
            return

        insert_df["pub_date"] = pd.to_datetime(insert_df["profitForcastExpPubDate"], errors="coerce").dt.date
        insert_df["stat_date"] = pd.to_datetime(insert_df["profitForcastExpStatDate"], errors="coerce").dt.date
        insert_df["updated_at"] = pd.Timestamp.now()
        insert_df = insert_df.rename(
            columns={
                "profitForcastType": "forecast_type",
                "profitForcastAbstract": "forecast_abstract",
                "profitForcastChgPctUp": "chg_pct_up",
                "profitForcastChgPctDwn": "chg_pct_dwn",
            }
        )
        insert_df = insert_df[
            ["code", "pub_date", "stat_date", "forecast_type", "forecast_abstract", "chg_pct_up", "chg_pct_dwn", "updated_at"]
        ].drop_duplicates(subset=["code", "pub_date", "stat_date"], keep="last")

        if insert_df.empty:
            return

        for code_value in insert_df["code"].drop_duplicates().tolist():
            code_df = insert_df.loc[insert_df["code"] == code_value]
            start_date = code_df["pub_date"].min().isoformat()
            end_date = code_df["pub_date"].max().isoformat()
            conn.execute(
                "DELETE FROM forecast_reports WHERE code = ? AND pub_date BETWEEN ? AND ?",
                [code_value, start_date, end_date],
            )
            conn.register("forecast_insert_df", code_df)
            conn.execute(
                """
                INSERT INTO forecast_reports (code, pub_date, stat_date, forecast_type, forecast_abstract, chg_pct_up, chg_pct_dwn, updated_at)
                SELECT code, pub_date, stat_date, forecast_type, forecast_abstract, chg_pct_up, chg_pct_dwn, updated_at
                FROM forecast_insert_df
                """
            )
            conn.unregister("forecast_insert_df")

    def _merge_performance_express_reports(self, conn, frame: pd.DataFrame):
        if frame.empty:
            return

        insert_df = frame.copy()
        insert_df = insert_df.dropna(subset=["code", "performanceExpPubDate"])
        if insert_df.empty:
            return

        insert_df["pub_date"] = pd.to_datetime(insert_df["performanceExpPubDate"], errors="coerce").dt.date
        insert_df["stat_date"] = pd.to_datetime(insert_df["performanceExpStatDate"], errors="coerce").dt.date
        insert_df["update_date"] = pd.to_datetime(insert_df["performanceExpUpdateDate"], errors="coerce").dt.date
        insert_df["updated_at"] = pd.Timestamp.now()
        insert_df = insert_df.rename(
            columns={
                "performanceExpressTotalAsset": "total_asset",
                "performanceExpressNetAsset": "net_asset",
                "performanceExpressEPSChgPct": "eps_chg_pct",
                "performanceExpressROEWa": "roe_wa",
                "performanceExpressEPSDiluted": "eps_diluted",
                "performanceExpressGRYOY": "gryoy",
                "performanceExpressOPYOY": "opyoy",
            }
        )
        insert_df = insert_df[
            [
                "code",
                "pub_date",
                "stat_date",
                "update_date",
                "total_asset",
                "net_asset",
                "eps_chg_pct",
                "roe_wa",
                "eps_diluted",
                "gryoy",
                "opyoy",
                "updated_at",
            ]
        ].drop_duplicates(subset=["code", "pub_date", "stat_date"], keep="last")

        if insert_df.empty:
            return

        for code_value in insert_df["code"].drop_duplicates().tolist():
            code_df = insert_df.loc[insert_df["code"] == code_value]
            start_date = code_df["pub_date"].min().isoformat()
            end_date = code_df["pub_date"].max().isoformat()
            conn.execute(
                "DELETE FROM performance_express_reports WHERE code = ? AND pub_date BETWEEN ? AND ?",
                [code_value, start_date, end_date],
            )
            conn.register("express_insert_df", code_df)
            conn.execute(
                """
                INSERT INTO performance_express_reports (
                    code, pub_date, stat_date, update_date, total_asset, net_asset, eps_chg_pct,
                    roe_wa, eps_diluted, gryoy, opyoy, updated_at
                )
                SELECT
                    code, pub_date, stat_date, update_date, total_asset, net_asset, eps_chg_pct,
                    roe_wa, eps_diluted, gryoy, opyoy, updated_at
                FROM express_insert_df
                """
            )
            conn.unregister("express_insert_df")

    def _merge_growth_reports(self, conn, frame: pd.DataFrame):
        if frame.empty:
            return

        insert_df = frame.copy()
        insert_df = insert_df.dropna(subset=["code", "pubDate", "statDate"])
        if insert_df.empty:
            return

        insert_df["pub_date"] = pd.to_datetime(insert_df["pubDate"], errors="coerce").dt.date
        insert_df["stat_date"] = pd.to_datetime(insert_df["statDate"], errors="coerce").dt.date
        insert_df["updated_at"] = pd.Timestamp.now()
        insert_df = insert_df.rename(
            columns={
                "YOYEquity": "yoy_equity",
                "YOYAsset": "yoy_asset",
                "YOYNI": "yoy_ni",
                "YOYEPSBasic": "yoy_eps_basic",
                "YOYPNI": "yoy_pni",
            }
        )
        insert_df = insert_df[
            ["code", "pub_date", "stat_date", "yoy_equity", "yoy_asset", "yoy_ni", "yoy_eps_basic", "yoy_pni", "updated_at"]
        ].drop_duplicates(subset=["code", "stat_date"], keep="last")

        if insert_df.empty:
            return

        for code_value in insert_df["code"].drop_duplicates().tolist():
            code_df = insert_df.loc[insert_df["code"] == code_value]
            start_date = code_df["stat_date"].min().isoformat()
            end_date = code_df["stat_date"].max().isoformat()
            conn.execute(
                "DELETE FROM growth_reports WHERE code = ? AND stat_date BETWEEN ? AND ?",
                [code_value, start_date, end_date],
            )
            conn.register("growth_insert_df", code_df)
            conn.execute(
                """
                INSERT INTO growth_reports (
                    code, pub_date, stat_date, yoy_equity, yoy_asset, yoy_ni, yoy_eps_basic, yoy_pni, updated_at
                )
                SELECT code, pub_date, stat_date, yoy_equity, yoy_asset, yoy_ni, yoy_eps_basic, yoy_pni, updated_at
                FROM growth_insert_df
                """
            )
            conn.unregister("growth_insert_df")

    def _sync_symbol_history(
        self,
        conn,
        client: BaoStockSession,
        code: str,
        start_date: str,
        end_date: str,
        adjustflag: str,
        force_refresh: bool,
    ) -> str:
        existing = conn.execute(
            "SELECT start_date, end_date FROM cache_summary WHERE code = ?",
            [code],
        ).fetchone()

        if force_refresh:
            history = client.query_history(code, start_date, end_date, adjustflag=adjustflag)
            self._replace_code_history(conn, code, history, start_date, end_date)
            return "created" if existing is None else "updated"

        if existing is not None:
            cached_start = existing[0].isoformat()
            cached_end = existing[1].isoformat()
            if cached_start <= start_date and cached_end >= end_date:
                return "reused"

            fetched_any = False
            if cached_start > start_date:
                left_end = shift_day(cached_start, -1)
                left = client.query_history(code, start_date, left_end, adjustflag=adjustflag)
                if not left.empty:
                    self._merge_history_into_db(conn, left)
                    fetched_any = True
            if cached_end < end_date:
                right_start = shift_day(cached_end, 1)
                right = client.query_history(code, right_start, end_date, adjustflag=adjustflag)
                if not right.empty:
                    self._merge_history_into_db(conn, right)
                    fetched_any = True
            if fetched_any:
                self._refresh_code_summary(conn, code)
                return "updated"
            return "reused"

        history = client.query_history(code, start_date, end_date, adjustflag=adjustflag)
        self._merge_history_into_db(conn, history)
        self._refresh_code_summary(conn, code)
        return "created"

    def _replace_code_history(
        self,
        conn,
        code: str,
        history: pd.DataFrame,
        start_date: str,
        end_date: str,
    ):
        conn.execute("DELETE FROM daily_bars WHERE code = ? AND trade_date BETWEEN ? AND ?", [code, start_date, end_date])
        if not history.empty:
            self._merge_history_into_db(conn, history)
        self._refresh_code_summary(conn, code)

    def _merge_history_into_db(self, conn, history: pd.DataFrame):
        if history.empty:
            return

        frame = history.copy()
        frame["code"] = frame["code"].astype(str).str.lower()
        frame["trade_date"] = pd.to_datetime(frame["date"]).dt.date
        frame = frame.rename(columns={"pctChg": "pct_chg"})
        insert_df = frame[["code", "trade_date", "open", "high", "low", "close", "volume", "amount", "turn", "pct_chg"]]
        insert_df = insert_df.dropna(subset=["code", "trade_date"]).drop_duplicates(subset=["code", "trade_date"], keep="last")

        if insert_df.empty:
            return

        codes = [str(value) for value in insert_df["code"].drop_duplicates().tolist()]
        for code in codes:
            code_frame = insert_df.loc[insert_df["code"] == code]
            start_date = code_frame["trade_date"].min().isoformat()
            end_date = code_frame["trade_date"].max().isoformat()
            conn.execute(
                "DELETE FROM daily_bars WHERE code = ? AND trade_date BETWEEN ? AND ?",
                [code, start_date, end_date],
            )
            conn.register("bar_insert_df", code_frame)
            conn.execute(
                """
                INSERT INTO daily_bars (code, trade_date, open, high, low, close, volume, amount, turn, pct_chg)
                SELECT code, trade_date, open, high, low, close, volume, amount, turn, pct_chg
                FROM bar_insert_df
                """
            )
            conn.unregister("bar_insert_df")
            self._refresh_code_summary(conn, code)

    def _build_history_range(self, params: ReviewParams) -> Tuple[str, str]:
        lookback_buffer = max(260, int(params.lookback_days * 1.8))
        future_buffer = max(30, params.hold_days * 4)
        start_date = shift_day(params.signal_date, -lookback_buffer)
        end_date = shift_day(params.signal_date, future_buffer)
        return start_date, end_date

    def _load_history_frame(self, conn, params: ReviewParams, universe_df: pd.DataFrame) -> pd.DataFrame:
        if universe_df.empty:
            return pd.DataFrame()

        start_date, end_date = self._build_history_range(params)
        conn.register("universe_screen_df", universe_df[["code", "name"]].drop_duplicates(subset=["code"]))
        history = conn.execute(
            """
            SELECT
                b.code,
                COALESCE(u.name, m.name, '--') AS name,
                b.trade_date AS date,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.amount,
                b.turn,
                b.pct_chg
            FROM daily_bars AS b
            INNER JOIN universe_screen_df AS u ON u.code = b.code
            LEFT JOIN stock_master AS m ON m.code = b.code
            WHERE b.trade_date BETWEEN ? AND ?
            ORDER BY b.code, b.trade_date
            """,
            [start_date, end_date],
        ).fetchdf()
        conn.unregister("universe_screen_df")

        if history.empty:
            return history

        history["date"] = pd.to_datetime(history["date"])
        history["name"] = history["name"].fillna("--")
        return history

    def _evaluate_universe(
        self,
        history: pd.DataFrame,
        params: ReviewParams,
        earnings_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        frame = history.copy()
        frame = frame.sort_values(["code", "date"]).reset_index(drop=True)
        grouped = frame.groupby("code", sort=False)

        frame["prev_close"] = grouped["close"].shift(1)
        frame["buy_date"] = grouped["date"].shift(-1)
        frame["buy_open"] = grouped["open"].shift(-1)
        sell_shift = sell_shift_from_hold_days(params.hold_days)
        frame["sell_date"] = grouped["date"].shift(-sell_shift)
        frame["sell_close"] = grouped["close"].shift(-sell_shift)
        frame["turnover_rate"] = frame["turn"]
        frame["ema"] = np.nan
        frame["prev_ema"] = np.nan
        frame["volume_ratio"] = np.nan
        frame["daily_k_slope_pct"] = np.nan
        frame["daily_k_angle"] = np.nan
        frame["relative_low_position"] = np.nan

        if params.enable_ema_breakout:
            frame["ema"] = grouped["close"].transform(lambda series: series.ewm(span=params.ema_period, adjust=False).mean())
            frame["prev_ema"] = frame.groupby("code", sort=False)["ema"].shift(1)

        if params.enable_volume_ratio:
            frame["volume_ratio"] = grouped["volume"].transform(
                lambda series: series / series.shift(1).rolling(params.volume_ma_window).mean()
            )

        if params.enable_daily_k_angle:
            frame["daily_k_slope_pct"] = (
                grouped["close"]
                .apply(lambda series: series.rolling(params.daily_k_angle_window).apply(calculate_daily_k_slope_pct, raw=True))
                .reset_index(level=0, drop=True)
            )
            frame["daily_k_angle"] = frame["daily_k_slope_pct"].apply(slope_pct_to_angle)

        if params.enable_relative_low:
            rolling_low = grouped["low"].transform(lambda series: series.rolling(params.relative_low_window).min())
            rolling_high = grouped["high"].transform(lambda series: series.rolling(params.relative_low_window).max())
            range_span = rolling_high - rolling_low
            frame["relative_low_position"] = np.where(
                range_span.abs() < 1e-12,
                0.0,
                (frame["close"] - rolling_low) / range_span,
            )

        signal_rows = frame.loc[frame["date"] == pd.Timestamp(params.signal_date)].copy()
        if signal_rows.empty:
            return pd.DataFrame(columns=[field for field in ReviewResult.__dataclass_fields__])

        if earnings_df is not None and not earnings_df.empty:
            signal_rows = signal_rows.merge(earnings_df, on="code", how="left")
        else:
            signal_rows["forecast_pub_date"] = pd.NaT
            signal_rows["forecast_type"] = ""
            signal_rows["forecast_chg_pct_up"] = np.nan
            signal_rows["forecast_chg_pct_dwn"] = np.nan
            signal_rows["express_pub_date"] = pd.NaT
            signal_rows["express_gryoy"] = np.nan
            signal_rows["express_opyoy"] = np.nan
            signal_rows["growth_pub_date"] = pd.NaT
            signal_rows["growth_yoy_ni"] = np.nan

        signal_rows["signal_change_pct"] = signal_rows["close"] / signal_rows["prev_close"] - 1.0
        signal_rows["hold_return_pct"] = signal_rows["sell_close"] / signal_rows["buy_open"] - 1.0
        signal_rows["breakout_pct"] = signal_rows["close"] / signal_rows["ema"] - 1.0
        signal_rows["forecast_chg_pct_max"] = signal_rows[["forecast_chg_pct_up", "forecast_chg_pct_dwn"]].max(
            axis=1,
            skipna=True,
        )
        earnings_yoy_threshold = params.earnings_yoy_min / 100.0
        signal_rows["forecast_pass"] = (
            signal_rows["forecast_type"].fillna("").isin(POSITIVE_FORECAST_TYPES)
            & signal_rows["forecast_chg_pct_max"].notna()
            & (signal_rows["forecast_chg_pct_max"] >= params.earnings_forecast_change_min)
        )
        signal_rows["express_pass"] = (
            signal_rows["express_gryoy"].notna()
            & signal_rows["express_opyoy"].notna()
            & (signal_rows["express_gryoy"] >= earnings_yoy_threshold)
            & (signal_rows["express_opyoy"] >= earnings_yoy_threshold)
        )
        signal_rows["growth_pass"] = signal_rows["growth_yoy_ni"].notna() & (
            signal_rows["growth_yoy_ni"] >= earnings_yoy_threshold
        )

        mask = (
            signal_rows["prev_close"].notna()
            & signal_rows["buy_date"].notna()
            & signal_rows["buy_open"].notna()
            & signal_rows["sell_date"].notna()
            & signal_rows["sell_close"].notna()
            & signal_rows["open"].notna()
            & signal_rows["high"].notna()
            & signal_rows["low"].notna()
            & signal_rows["close"].notna()
            & signal_rows["turnover_rate"].notna()
        )

        if params.enable_ema_breakout:
            mask &= signal_rows["ema"].notna() & signal_rows["prev_ema"].notna()
            mask &= (signal_rows["prev_close"] <= signal_rows["prev_ema"]) & (signal_rows["close"] > signal_rows["ema"])

        if params.enable_volume_ratio:
            mask &= signal_rows["volume_ratio"].notna()
            mask &= signal_rows["volume_ratio"] >= params.volume_ratio_min

        if params.enable_daily_k_angle:
            mask &= signal_rows["daily_k_angle"].notna()
            mask &= signal_rows["daily_k_angle"] >= params.daily_k_angle_min

        if params.enable_relative_low:
            mask &= signal_rows["relative_low_position"].notna()
            mask &= signal_rows["relative_low_position"] <= params.relative_low_position_max

        if params.enable_earnings_filter:
            mask &= signal_rows["forecast_pass"] | signal_rows["express_pass"] | signal_rows["growth_pass"]

        if params.enable_price_max:
            mask &= signal_rows["close"] < params.price_max

        if params.enable_turnover:
            mask &= signal_rows["turnover_rate"] >= params.turnover_min

        result = signal_rows.loc[mask].copy()
        if result.empty:
            return pd.DataFrame(columns=[field for field in ReviewResult.__dataclass_fields__])

        result["universe_label"] = UNIVERSE_LABELS.get(params.universe, params.universe)
        result["signal_date"] = result["date"].dt.date.astype(str)
        result["signal_open"] = result["open"]
        result["signal_high"] = result["high"]
        result["signal_low"] = result["low"]
        result["signal_close"] = result["close"]
        result["ema_value"] = result["ema"]
        result["buy_date"] = pd.to_datetime(result["buy_date"]).dt.date.astype(str)
        result["sell_date"] = pd.to_datetime(result["sell_date"]).dt.date.astype(str)
        result["earnings_pub_date"] = result.apply(self._select_earnings_pub_date, axis=1)
        result["earnings_signal"] = result.apply(self._build_earnings_signal, axis=1)
        result["hold_days"] = params.hold_days
        result["rule_label"] = build_rule_label(params)

        output_columns = [
            "code",
            "name",
            "universe_label",
            "signal_date",
            "signal_open",
            "signal_high",
            "signal_low",
            "signal_close",
            "signal_change_pct",
            "ema_value",
            "breakout_pct",
            "daily_k_slope_pct",
            "daily_k_angle",
            "relative_low_position",
            "earnings_pub_date",
            "earnings_signal",
            "volume_ratio",
            "turnover_rate",
            "volume",
            "amount",
            "buy_date",
            "buy_open",
            "sell_date",
            "sell_close",
            "hold_days",
            "hold_return_pct",
            "rule_label",
        ]
        return result[output_columns].reset_index(drop=True)

    def _select_earnings_pub_date(self, row: pd.Series) -> str:
        candidates: List[Tuple[pd.Timestamp, str]] = []
        for pass_col, date_col in (
            ("forecast_pass", "forecast_pub_date"),
            ("express_pass", "express_pub_date"),
            ("growth_pass", "growth_pub_date"),
        ):
            pub_date = row.get(date_col)
            if bool(row.get(pass_col, False)) and pd.notna(pub_date):
                candidates.append((pd.Timestamp(pub_date), date_col))
        if not candidates:
            return ""
        latest_date = max(item[0] for item in candidates)
        return latest_date.date().isoformat()

    def _build_earnings_signal(self, row: pd.Series) -> str:
        candidates: List[Tuple[pd.Timestamp, str]] = []
        if bool(row.get("forecast_pass", False)) and pd.notna(row.get("forecast_pub_date")):
            forecast_low = row.get("forecast_chg_pct_dwn")
            forecast_high = row.get("forecast_chg_pct_up")
            forecast_range = "--"
            if pd.notna(forecast_low) and pd.notna(forecast_high):
                forecast_range = f"{float(forecast_low):.2f}%~{float(forecast_high):.2f}%"
            elif pd.notna(forecast_high):
                forecast_range = f"{float(forecast_high):.2f}%"
            candidates.append(
                (
                    pd.Timestamp(row["forecast_pub_date"]),
                    f"业绩预告 {row.get('forecast_type', '--')} ({forecast_range})",
                )
            )
        if bool(row.get("express_pass", False)) and pd.notna(row.get("express_pub_date")):
            candidates.append(
                (
                    pd.Timestamp(row["express_pub_date"]),
                    (
                        "业绩快报 "
                        f"营收同比{format_percent_value(row.get('express_gryoy', float('nan')))} "
                        f"营业利润同比{format_percent_value(row.get('express_opyoy', float('nan')))}"
                    ),
                )
            )
        if bool(row.get("growth_pass", False)) and pd.notna(row.get("growth_pub_date")):
            candidates.append(
                (
                    pd.Timestamp(row["growth_pub_date"]),
                    f"成长数据 净利润同比{format_percent_value(row.get('growth_yoy_ni', float('nan')))}",
                )
            )
        if not candidates:
            return ""
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _enrich_history(self, history: pd.DataFrame, params: ReviewParams) -> pd.DataFrame:
        frame = history.copy()
        frame = frame.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        frame["ema"] = frame["close"].ewm(span=params.ema_period, adjust=False).mean()
        frame["volume_ma_prev"] = frame["volume"].shift(1).rolling(params.volume_ma_window).mean()
        frame["volume_ma"] = frame["volume"].rolling(params.volume_ma_window).mean()
        frame["volume_ratio"] = frame["volume"] / frame["volume_ma_prev"]
        frame["daily_k_slope_pct"] = frame["close"].rolling(params.daily_k_angle_window).apply(
            calculate_daily_k_slope_pct,
            raw=True,
        )
        frame["daily_k_angle"] = frame["daily_k_slope_pct"].apply(slope_pct_to_angle)
        rolling_low = frame["low"].rolling(params.relative_low_window).min()
        rolling_high = frame["high"].rolling(params.relative_low_window).max()
        range_span = rolling_high - rolling_low
        frame["relative_low_position"] = np.where(
            range_span.abs() < 1e-12,
            0.0,
            (frame["close"] - rolling_low) / range_span,
        )
        frame["turnover_rate"] = frame["turn"]
        frame["pct_change_close"] = frame["close"].pct_change()
        return frame

    def _build_summary(self, frame: pd.DataFrame, params: ReviewParams) -> Dict[str, float | int | str]:
        if frame.empty:
            return {
                "count": 0,
                "avg_return_pct": 0.0,
                "median_return_pct": 0.0,
                "win_rate": 0.0,
                "best_return_pct": 0.0,
                "worst_return_pct": 0.0,
                "signal_date": params.signal_date,
                "universe_label": UNIVERSE_LABELS.get(params.universe, params.universe),
            }

        returns = frame["hold_return_pct"]
        return {
            "count": int(len(frame)),
            "avg_return_pct": safe_mean(returns),
            "median_return_pct": safe_median(returns),
            "win_rate": float((returns > 0).mean()),
            "best_return_pct": float(returns.max()),
            "worst_return_pct": float(returns.min()),
            "signal_date": params.signal_date,
            "universe_label": UNIVERSE_LABELS.get(params.universe, params.universe),
        }

    def _save_report(self, frame: pd.DataFrame, params: ReviewParams) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.report_dir / f"review_{params.universe}_{params.signal_date.replace('-', '')}_{timestamp}.csv"
        frame.to_csv(report_path, index=False, encoding="utf-8-sig")
        return report_path

    def _read_last_cached_date(self, file_path: Path) -> str:
        try:
            with open(file_path, "rb") as handle:
                handle.seek(0, 2)
                file_size = handle.tell()
                if file_size <= 0:
                    return ""

                position = file_size - 1
                while position > 0:
                    handle.seek(position)
                    char = handle.read(1)
                    if char == b"\n" and position < file_size - 1:
                        break
                    position -= 1

                if position <= 0:
                    handle.seek(0)

                line = handle.readline().decode("utf-8", errors="ignore").strip()
                if not line or line.startswith("date,"):
                    return ""
                return line.split(",", 1)[0].strip()
        except OSError:
            return ""

    def _session(self):
        class _SessionContext:
            def __init__(self):
                self.client = BaoStockSession()

            def __enter__(self):
                self.client.login()
                return self.client

            def __exit__(self, exc_type, exc_val, exc_tb):
                self.client.logout()
                return False

        return _SessionContext()
