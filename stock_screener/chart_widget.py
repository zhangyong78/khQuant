from __future__ import annotations

from typing import Dict, Optional

import matplotlib as mpl
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from matplotlib.ticker import FuncFormatter
from PyQt5.QtWidgets import QSizePolicy


mpl.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "Arial Unicode MS"]
mpl.rcParams["axes.unicode_minus"] = False


def _format_large_number(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1e8:
        return f"{value / 1e8:.2f}亿"
    if abs_value >= 1e4:
        return f"{value / 1e4:.2f}万"
    return f"{value:.0f}"


class ReviewChartWidget(FigureCanvas):
    def __init__(self, parent=None):
        self.figure = Figure(figsize=(12, 8), facecolor="#fbfdff")
        super().__init__(self.figure)
        self.setParent(parent)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.updateGeometry()
        self._draw_placeholder("运行筛选后，选择一只股票查看 K 线复盘。")

    def _draw_placeholder(self, message: str):
        self.figure.clear()
        axis = self.figure.add_subplot(111)
        axis.set_axis_off()
        axis.text(
            0.5,
            0.5,
            message,
            ha="center",
            va="center",
            fontsize=14,
            color="#64748b",
            transform=axis.transAxes,
        )
        self.draw_idle()

    def show_message(self, message: str):
        self._draw_placeholder(message)

    def plot_review(self, payload: Dict, row: Dict):
        frame = payload.get("frame")
        if frame is None or frame.empty:
            self._draw_placeholder("没有可用图表数据。")
            return

        self.figure.clear()
        grid = self.figure.add_gridspec(2, 1, height_ratios=[3.3, 1.1], hspace=0.04)
        ax_price = self.figure.add_subplot(grid[0, 0])
        ax_volume = self.figure.add_subplot(grid[1, 0], sharex=ax_price)

        frame = frame.copy().reset_index(drop=True)
        width = 0.62
        x_values = list(range(len(frame)))
        date_to_index = {item.date().isoformat(): idx for idx, item in enumerate(frame["date"])}

        for idx, bar in frame.iterrows():
            color = "#cf3f4f" if bar["close"] >= bar["open"] else "#2f8f63"
            ax_price.vlines(idx, bar["low"], bar["high"], color=color, linewidth=1.0, alpha=0.95)

            body_low = min(bar["open"], bar["close"])
            body_high = max(bar["open"], bar["close"])
            body_height = max(body_high - body_low, max(bar["close"], 1.0) * 0.001)
            body_start = body_low if body_high != body_low else body_low - body_height / 2
            rect = Rectangle(
                (idx - width / 2, body_start),
                width,
                body_height,
                facecolor=color,
                edgecolor=color,
                linewidth=0.8,
            )
            ax_price.add_patch(rect)

            ax_volume.bar(idx, bar["volume"], width=width, color=color, alpha=0.45)

        ax_price.plot(
            x_values,
            frame["ema"],
            color="#2563eb",
            linewidth=1.5,
            label=f"EMA{payload['ema_period']}",
        )
        ax_volume.plot(
            x_values,
            frame["volume_ma"],
            color="#7c3aed",
            linewidth=1.2,
            label=f"量均线 {payload['volume_ma_window']}",
        )

        marker_specs = [
            ("signal_date", "信号", row.get("signal_close", 0.0), "#f59e0b", "o"),
            ("buy_date", "买入", row.get("buy_open", 0.0), "#2563eb", "^"),
            ("sell_date", "卖出", row.get("sell_close", 0.0), "#dc2626", "v"),
        ]
        for date_key, label, value, color, marker in marker_specs:
            target_date = payload.get(date_key)
            if not target_date or target_date not in date_to_index:
                continue
            x_pos = date_to_index[target_date]
            ax_price.axvline(x_pos, color=color, linestyle="--", linewidth=1.0, alpha=0.18)
            ax_price.scatter(
                [x_pos],
                [value],
                s=80,
                color=color,
                marker=marker,
                edgecolors="#ffffff",
                linewidths=0.8,
                zorder=5,
            )
            ax_price.annotate(
                label,
                xy=(x_pos, value),
                xytext=(0, 14 if marker != "v" else -18),
                textcoords="offset points",
                ha="center",
                color=color,
                fontsize=10,
                weight="bold",
            )

        hold_days = int(row.get("hold_days", 0) or 0)
        hold_text = (
            f"买入当日收益 {row.get('hold_return_pct', 0.0):.2%}"
            if hold_days == 0
            else f"持有 {hold_days} 天收益 {row.get('hold_return_pct', 0.0):.2%}"
        )

        title = (
            f"{row.get('code', '--')}  {row.get('name', '--')}    "
            f"信号日 {row.get('signal_date', '--')}    "
            f"{hold_text}"
        )
        ax_price.set_title(title, loc="left", fontsize=13, pad=10, color="#0f172a")

        stats_text = (
            f"信号收盘: {row.get('signal_close', 0.0):.2f}\n"
            f"EMA: {row.get('ema_value', 0.0):.2f}\n"
            f"量比: {row.get('volume_ratio', 0.0):.2f}\n"
            f"日K角度({payload.get('daily_k_angle_window', 0)}日): {row.get('daily_k_angle', 0.0):.2f}°\n"
            f"区间位置({payload.get('relative_low_window', 0)}日): {row.get('relative_low_position', 0.0):.2%}\n"
            f"业绩公告: {row.get('earnings_pub_date', '--') or '--'}\n"
            f"业绩依据: {row.get('earnings_signal', '--') or '--'}\n"
            f"换手率: {row.get('turnover_rate', 0.0):.2f}%\n"
            f"买入: {row.get('buy_date', '--')} @ {row.get('buy_open', 0.0):.2f}\n"
            f"卖出: {row.get('sell_date', '--')} @ {row.get('sell_close', 0.0):.2f}"
        )
        ax_price.text(
            0.012,
            0.98,
            stats_text,
            transform=ax_price.transAxes,
            va="top",
            ha="left",
            fontsize=9.5,
            color="#1e293b",
            bbox={
                "boxstyle": "round,pad=0.45",
                "facecolor": "#ffffff",
                "edgecolor": "#dbe4f0",
                "alpha": 0.92,
            },
        )

        ax_price.set_facecolor("#ffffff")
        ax_volume.set_facecolor("#ffffff")
        ax_price.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.16)
        ax_volume.grid(axis="y", linestyle="--", linewidth=0.5, alpha=0.12)
        ax_price.spines["top"].set_visible(False)
        ax_price.spines["right"].set_visible(False)
        ax_volume.spines["top"].set_visible(False)
        ax_volume.spines["right"].set_visible(False)

        ax_price.legend(loc="upper left", frameon=False, fontsize=9)
        ax_volume.legend(loc="upper left", frameon=False, fontsize=9)
        ax_price.set_ylabel("价格")
        ax_volume.set_ylabel("成交量")

        tick_step = max(1, len(frame) // 8)
        ticks = list(range(0, len(frame), tick_step))
        if ticks[-1] != len(frame) - 1:
            ticks.append(len(frame) - 1)
        labels = [frame.iloc[idx]["date"].strftime("%m-%d") for idx in ticks]
        ax_volume.set_xticks(ticks)
        ax_volume.set_xticklabels(labels, rotation=0)

        ax_volume.yaxis.set_major_formatter(FuncFormatter(lambda value, _: _format_large_number(value)))
        ax_price.margins(x=0.01)
        ax_volume.margins(x=0.01)
        self.figure.subplots_adjust(left=0.07, right=0.985, top=0.93, bottom=0.08)
        self.draw_idle()
