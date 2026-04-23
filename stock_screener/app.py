from __future__ import annotations

import math
import os
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Dict, List, Optional

from PyQt5.QtCore import QDate, QObject, Qt, QThread, pyqtSignal, QLockFile
from PyQt5.QtGui import QFont, QIcon, QKeySequence
from PyQt5.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QDoubleSpinBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QShortcut,
    QSpinBox,
    QSplitter,
    QStatusBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from version import get_version

from .chart_widget import ReviewChartWidget
from .engine import ReviewEngine, ReviewParams, SyncParams, UNIVERSE_LABELS, to_bs_code
from .runtime import get_app_root, get_icon_path


TABLE_COLUMNS = [
    ("code", "代码"),
    ("name", "名称"),
    ("signal_close", "信号收盘"),
    ("ema_value", "EMA"),
    ("volume_ratio", "量比"),
    ("daily_k_angle", "日K角度"),
    ("relative_low_position", "区间位置"),
    ("turnover_rate", "换手率"),
    ("signal_change_pct", "信号涨幅"),
    ("buy_date", "买入日"),
    ("buy_open", "买入价"),
    ("sell_date", "卖出日"),
    ("sell_close", "卖出价"),
    ("hold_return_pct", "持有收益"),
]


def format_value(column: str, value) -> str:
    if value in (None, ""):
        return "--"
    if isinstance(value, float) and math.isnan(value):
        return "--"
    if column in {"signal_change_pct", "hold_return_pct", "breakout_pct"}:
        return f"{float(value):.2%}"
    if column == "relative_low_position":
        return f"{float(value):.2%}"
    if column == "turnover_rate":
        return f"{float(value):.2f}%"
    if column == "daily_k_angle":
        return f"{float(value):.2f}°"
    if column in {"signal_close", "ema_value", "buy_open", "sell_close"}:
        return f"{float(value):.2f}"
    if column == "volume_ratio":
        return f"{float(value):.2f}"
    return str(value)


def build_rule_text(params: ReviewParams) -> str:
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
        rule_parts.append("不设置额外筛选条件")

    hold_text = "买入当日收盘" if params.hold_days == 0 else f"持有第 {params.hold_days} 个交易日收盘"

    return (
        f"规则: {params.signal_date} {'，'.join(rule_parts)}。"
        f"买入按次日开盘，卖出按{hold_text}。"
    )


def build_sync_mode_text(params: SyncParams) -> str:
    parts: List[str] = []
    if params.sync_history:
        parts.append("日线")
    if params.sync_earnings:
        parts.append(f"业绩(近{params.growth_quarters}季成长)")
    return " + ".join(parts) if parts else "未选择同步内容"


class ScanWorker(QObject):
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, project_root: Path, params: ReviewParams):
        super().__init__()
        self.project_root = Path(project_root)
        self.params = params
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            engine = ReviewEngine(self.project_root)
            outcome = engine.screen(
                self.params,
                progress_callback=self._emit_progress,
                is_cancelled=lambda: self._cancelled,
            )
            self.finished.emit(outcome)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _emit_progress(self, current: int, total: int, message: str):
        self.progress.emit(current, total, message)


class SyncWorker(QObject):
    progress = pyqtSignal(int, int, str)
    finished = pyqtSignal(object)
    failed = pyqtSignal(str)

    def __init__(self, project_root: Path, params: SyncParams):
        super().__init__()
        self.project_root = Path(project_root)
        self.params = params
        self._cancelled = False

    def cancel(self):
        self._cancelled = True

    def run(self):
        try:
            engine = ReviewEngine(self.project_root)
            outcome = engine.sync_data(
                self.params,
                progress_callback=self._emit_progress,
                is_cancelled=lambda: self._cancelled,
            )
            self.finished.emit(outcome)
        except Exception as exc:
            self.failed.emit(str(exc))

    def _emit_progress(self, current: int, total: int, message: str):
        self.progress.emit(current, total, message)


class MainWindow(QMainWindow):
    def __init__(self, project_root: Path):
        super().__init__()
        self.project_root = Path(project_root)
        self.engine = ReviewEngine(self.project_root)
        self.worker_thread: Optional[QThread] = None
        self.worker = None
        self.active_task = ""
        self.current_rows: List[Dict] = []
        self.filtered_rows: List[Dict] = []
        self.current_params = ReviewParams(signal_date=QDate.currentDate().addDays(-10).toString("yyyy-MM-dd"))
        self.current_report_path = ""

        self.setWindowTitle(f"日线复盘选股器 v{get_version()}")
        self.resize(1620, 940)
        self.setMinimumSize(1280, 780)
        self._build_ui()
        self._apply_style()
        self._update_rule_hint()
        self._update_summary({})
        self.chart_widget.show_message("运行筛选后，按上下键即可逐只翻看图表。")
        self._sync_date_mode_changed()
        self.refresh_cache_status()

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)
        root_layout.setContentsMargins(18, 18, 18, 12)
        root_layout.setSpacing(14)

        root_layout.addWidget(self._build_header_panel())

        splitter = QSplitter(Qt.Horizontal)
        splitter.setChildrenCollapsible(False)
        splitter.addWidget(self._build_result_panel())
        splitter.addWidget(self._build_chart_panel())
        splitter.setSizes([640, 980])
        root_layout.addWidget(splitter, 1)

        status_bar = QStatusBar()
        status_bar.setSizeGripEnabled(False)
        self.setStatusBar(status_bar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setFixedWidth(220)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        status_bar.addPermanentWidget(self.progress_bar)
        status_bar.showMessage("就绪")

        QShortcut(QKeySequence(Qt.Key_Up), self, activated=self.select_previous_row)
        QShortcut(QKeySequence(Qt.Key_Down), self, activated=self.select_next_row)

    def _build_header_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(12)

        title = QLabel(f"日线复盘选股器 v{get_version()}")
        title.setObjectName("titleLabel")
        subtitle = QLabel("独立于 QMT 的历史复盘工具，专门用于条件筛选、次日买入回测和图形复核。")
        subtitle.setObjectName("subtitleLabel")
        layout.addWidget(title)
        layout.addWidget(subtitle)

        self.cache_status_label = QLabel("当前数据最新日期：--")
        self.cache_status_label.setObjectName("cacheStatusLabel")
        layout.addWidget(self.cache_status_label)

        controls = QHBoxLayout()
        controls.setSpacing(10)

        self.signal_date_edit = QDateEdit(QDate.currentDate().addDays(-10))
        self.signal_date_edit.setCalendarPopup(True)
        self.signal_date_edit.setDisplayFormat("yyyy-MM-dd")
        self.signal_date_edit.dateChanged.connect(self._update_rule_hint)

        self.sync_start_date_edit = QDateEdit(QDate.currentDate().addYears(-2))
        self.sync_start_date_edit.setCalendarPopup(True)
        self.sync_start_date_edit.setDisplayFormat("yyyy-MM-dd")

        self.sync_end_date_edit = QDateEdit(QDate.currentDate())
        self.sync_end_date_edit.setCalendarPopup(True)
        self.sync_end_date_edit.setDisplayFormat("yyyy-MM-dd")

        self.universe_combo = QComboBox()
        self.universe_combo.addItem("沪深所有A股", "all_a")
        self.universe_combo.addItem("沪深300", "hs300")
        self.universe_combo.addItem("中证500", "zz500")
        self.universe_combo.addItem("上证50", "sz50")
        self.universe_combo.currentIndexChanged.connect(self._update_rule_hint)

        self.ema_spin = QSpinBox()
        self.ema_spin.setRange(2, 120)
        self.ema_spin.setValue(21)
        self.ema_spin.valueChanged.connect(self._update_rule_hint)
        self.ema_check = QCheckBox("启用 EMA 上穿")
        self.ema_check.setChecked(False)
        self.ema_check.toggled.connect(self._sync_filter_states)
        self.ema_check.toggled.connect(self._update_rule_hint)

        self.volume_window_spin = QSpinBox()
        self.volume_window_spin.setRange(2, 60)
        self.volume_window_spin.setValue(20)
        self.volume_window_spin.valueChanged.connect(self._update_rule_hint)

        self.volume_ratio_spin = QDoubleSpinBox()
        self.volume_ratio_spin.setRange(1.0, 20.0)
        self.volume_ratio_spin.setDecimals(2)
        self.volume_ratio_spin.setSingleStep(0.1)
        self.volume_ratio_spin.setValue(3.00)
        self.volume_ratio_spin.valueChanged.connect(self._update_rule_hint)
        self.volume_check = QCheckBox("启用带量过滤")
        self.volume_check.setChecked(True)
        self.volume_check.toggled.connect(self._sync_filter_states)
        self.volume_check.toggled.connect(self._update_rule_hint)

        self.daily_k_window_spin = QSpinBox()
        self.daily_k_window_spin.setRange(2, 20)
        self.daily_k_window_spin.setValue(5)
        self.daily_k_window_spin.valueChanged.connect(self._update_rule_hint)

        self.daily_k_angle_spin = QDoubleSpinBox()
        self.daily_k_angle_spin.setRange(0.0, 89.0)
        self.daily_k_angle_spin.setDecimals(2)
        self.daily_k_angle_spin.setSingleStep(1.0)
        self.daily_k_angle_spin.setValue(40.0)
        self.daily_k_angle_spin.valueChanged.connect(self._update_rule_hint)
        self.daily_k_angle_check = QCheckBox("启用日K角度过滤")
        self.daily_k_angle_check.setChecked(True)
        self.daily_k_angle_check.toggled.connect(self._sync_filter_states)
        self.daily_k_angle_check.toggled.connect(self._update_rule_hint)

        self.relative_low_window_spin = QSpinBox()
        self.relative_low_window_spin.setRange(2, 360)
        self.relative_low_window_spin.setValue(120)
        self.relative_low_window_spin.valueChanged.connect(self._update_rule_hint)

        self.relative_low_position_spin = QDoubleSpinBox()
        self.relative_low_position_spin.setRange(0.0, 100.0)
        self.relative_low_position_spin.setDecimals(2)
        self.relative_low_position_spin.setSingleStep(1.0)
        self.relative_low_position_spin.setSuffix("%")
        self.relative_low_position_spin.setValue(30.0)
        self.relative_low_position_spin.valueChanged.connect(self._update_rule_hint)
        self.relative_low_check = QCheckBox("启用相对低位过滤")
        self.relative_low_check.setChecked(True)
        self.relative_low_check.toggled.connect(self._sync_filter_states)
        self.relative_low_check.toggled.connect(self._update_rule_hint)

        self.earnings_forecast_spin = QDoubleSpinBox()
        self.earnings_forecast_spin.setRange(0.0, 500.0)
        self.earnings_forecast_spin.setDecimals(2)
        self.earnings_forecast_spin.setSingleStep(5.0)
        self.earnings_forecast_spin.setSuffix("%")
        self.earnings_forecast_spin.setValue(20.0)
        self.earnings_forecast_spin.valueChanged.connect(self._update_rule_hint)

        self.earnings_yoy_spin = QDoubleSpinBox()
        self.earnings_yoy_spin.setRange(0.0, 200.0)
        self.earnings_yoy_spin.setDecimals(2)
        self.earnings_yoy_spin.setSingleStep(1.0)
        self.earnings_yoy_spin.setSuffix("%")
        self.earnings_yoy_spin.setValue(10.0)
        self.earnings_yoy_spin.valueChanged.connect(self._update_rule_hint)
        self.earnings_check = QCheckBox("启用业绩预期过滤")
        self.earnings_check.setChecked(True)
        self.earnings_check.toggled.connect(self._sync_filter_states)
        self.earnings_check.toggled.connect(self._update_rule_hint)

        self.price_max_spin = QDoubleSpinBox()
        self.price_max_spin.setRange(0.0, 100000.0)
        self.price_max_spin.setDecimals(2)
        self.price_max_spin.setSingleStep(1.0)
        self.price_max_spin.setValue(50.0)
        self.price_max_spin.valueChanged.connect(self._update_rule_hint)
        self.price_max_check = QCheckBox("启用股价上限过滤")
        self.price_max_check.setChecked(True)
        self.price_max_check.toggled.connect(self._sync_filter_states)
        self.price_max_check.toggled.connect(self._update_rule_hint)

        self.turnover_spin = QDoubleSpinBox()
        self.turnover_spin.setRange(0.0, 100.0)
        self.turnover_spin.setDecimals(2)
        self.turnover_spin.setSingleStep(0.5)
        self.turnover_spin.setValue(10.0)
        self.turnover_spin.valueChanged.connect(self._update_rule_hint)
        self.turnover_check = QCheckBox("启用换手率过滤")
        self.turnover_check.setChecked(True)
        self.turnover_check.toggled.connect(self._sync_filter_states)
        self.turnover_check.toggled.connect(self._update_rule_hint)

        self.hold_days_spin = QSpinBox()
        self.hold_days_spin.setRange(0, 30)
        self.hold_days_spin.setValue(0)
        self.hold_days_spin.valueChanged.connect(self._update_rule_hint)

        for label_text, widget in (
            ("信号日期", self.signal_date_edit),
            ("股票池", self.universe_combo),
            ("EMA周期", self.ema_spin),
            ("量均线", self.volume_window_spin),
            ("量比阈值", self.volume_ratio_spin),
            ("日K窗口", self.daily_k_window_spin),
            ("日K角度", self.daily_k_angle_spin),
            ("低位周期", self.relative_low_window_spin),
            ("区间位置上限", self.relative_low_position_spin),
            ("预告增幅下限", self.earnings_forecast_spin),
            ("同比下限", self.earnings_yoy_spin),
            ("股价上限", self.price_max_spin),
            ("换手率阈值", self.turnover_spin),
            ("持有天数", self.hold_days_spin),
        ):
            group = QVBoxLayout()
            group.setSpacing(4)
            label = QLabel(label_text)
            label.setObjectName("fieldLabel")
            group.addWidget(label)
            group.addWidget(widget)
            controls.addLayout(group)

        controls.addStretch(1)

        self.run_button = QPushButton("开始筛选")
        self.run_button.clicked.connect(self.run_scan)

        self.stop_button = QPushButton("停止任务")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.cancel_task)

        self.open_report_button = QPushButton("打开报告目录")
        self.open_report_button.setEnabled(False)
        self.open_report_button.clicked.connect(self.open_report_dir)

        self.sync_button = QPushButton("同步数据")
        self.sync_button.clicked.connect(self.run_sync)

        self.open_cache_button = QPushButton("打开缓存目录")
        self.open_cache_button.clicked.connect(self.open_cache_dir)

        controls.addWidget(self.run_button)
        controls.addWidget(self.sync_button)
        controls.addWidget(self.stop_button)
        controls.addWidget(self.open_report_button)
        controls.addWidget(self.open_cache_button)
        layout.addLayout(controls)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(18)
        filter_row.addWidget(self.ema_check)
        filter_row.addWidget(self.volume_check)
        filter_row.addWidget(self.daily_k_angle_check)
        filter_row.addWidget(self.relative_low_check)
        filter_row.addWidget(self.earnings_check)
        filter_row.addWidget(self.price_max_check)
        filter_row.addWidget(self.turnover_check)
        filter_row.addStretch(1)
        layout.addLayout(filter_row)

        sync_row = QHBoxLayout()
        sync_row.setSpacing(10)
        for label_text, widget in (
            ("同步开始", self.sync_start_date_edit),
            ("同步结束", self.sync_end_date_edit),
        ):
            group = QVBoxLayout()
            group.setSpacing(4)
            label = QLabel(label_text)
            label.setObjectName("fieldLabel")
            group.addWidget(label)
            group.addWidget(widget)
            sync_row.addLayout(group)

        self.sync_history_check = QCheckBox("同步日线")
        self.sync_history_check.setChecked(True)

        self.sync_earnings_check = QCheckBox("同步业绩")
        self.sync_earnings_check.setChecked(False)
        self.sync_earnings_check.toggled.connect(self._sync_filter_states)

        self.growth_quarters_spin = QSpinBox()
        self.growth_quarters_spin.setRange(1, 24)
        self.growth_quarters_spin.setValue(6)

        growth_group = QVBoxLayout()
        growth_group.setSpacing(4)
        growth_label = QLabel("成长季度数")
        growth_label.setObjectName("fieldLabel")
        growth_group.addWidget(growth_label)
        growth_group.addWidget(self.growth_quarters_spin)
        sync_row.addLayout(growth_group)

        self.force_refresh_check = QCheckBox("强制刷新已缓存")
        self.sync_to_today_check = QCheckBox("同步到今天")
        self.sync_to_today_check.setChecked(True)
        self.sync_to_today_check.toggled.connect(self._sync_date_mode_changed)
        sync_row.addWidget(self.sync_history_check)
        sync_row.addWidget(self.sync_earnings_check)
        sync_row.addWidget(self.sync_to_today_check)
        sync_row.addWidget(self.force_refresh_check)
        sync_row.addStretch(1)
        layout.addLayout(sync_row)

        self.rule_hint_label = QLabel()
        self.rule_hint_label.setWordWrap(True)
        self.rule_hint_label.setObjectName("ruleHint")
        layout.addWidget(self.rule_hint_label)
        self._sync_filter_states()
        return panel

    def _build_result_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        summary_title = QLabel("结果摘要")
        summary_title.setObjectName("sectionTitle")
        layout.addWidget(summary_title)

        summary_grid = QGridLayout()
        summary_grid.setHorizontalSpacing(12)
        summary_grid.setVerticalSpacing(10)
        self.summary_labels: Dict[str, QLabel] = {}
        cards = [
            ("count", "命中数量"),
            ("avg_return_pct", "平均收益"),
            ("median_return_pct", "中位收益"),
            ("win_rate", "胜率"),
            ("best_return_pct", "最好收益"),
            ("worst_return_pct", "最差收益"),
        ]
        for index, (key, title) in enumerate(cards):
            card = QFrame()
            card.setObjectName("summaryCard")
            card_layout = QVBoxLayout(card)
            card_layout.setContentsMargins(12, 10, 12, 10)
            card_layout.setSpacing(6)

            title_label = QLabel(title)
            title_label.setObjectName("cardTitle")
            value_label = QLabel("--")
            value_label.setObjectName("cardValue")
            card_layout.addWidget(title_label)
            card_layout.addWidget(value_label)

            self.summary_labels[key] = value_label
            summary_grid.addWidget(card, index // 3, index % 3)

        layout.addLayout(summary_grid)

        result_title = QLabel("命中股票")
        result_title.setObjectName("sectionTitle")
        layout.addWidget(result_title)

        filter_bar = QHBoxLayout()
        filter_bar.setSpacing(10)

        self.result_search_edit = QLineEdit()
        self.result_search_edit.setPlaceholderText("筛选代码或名称")
        self.result_search_edit.textChanged.connect(self.apply_result_filters)

        self.result_price_min_spin = QDoubleSpinBox()
        self.result_price_min_spin.setRange(0.0, 100000.0)
        self.result_price_min_spin.setDecimals(2)
        self.result_price_min_spin.setSingleStep(1.0)
        self.result_price_min_spin.setSpecialValueText("不限")
        self.result_price_min_spin.valueChanged.connect(self.apply_result_filters)

        self.result_price_max_spin = QDoubleSpinBox()
        self.result_price_max_spin.setRange(0.0, 100000.0)
        self.result_price_max_spin.setDecimals(2)
        self.result_price_max_spin.setSingleStep(1.0)
        self.result_price_max_spin.setSpecialValueText("不限")
        self.result_price_max_spin.valueChanged.connect(self.apply_result_filters)

        self.clear_filter_button = QPushButton("清空筛选")
        self.clear_filter_button.clicked.connect(self.clear_result_filters)

        self.filter_status_label = QLabel("显示 0 / 0")
        self.filter_status_label.setObjectName("fieldLabel")

        filter_bar.addWidget(QLabel("结果筛选"))
        filter_bar.addWidget(self.result_search_edit, 1)
        filter_bar.addWidget(QLabel("信号收盘"))
        filter_bar.addWidget(self.result_price_min_spin)
        filter_bar.addWidget(QLabel("至"))
        filter_bar.addWidget(self.result_price_max_spin)
        filter_bar.addWidget(self.clear_filter_button)
        filter_bar.addWidget(self.filter_status_label)
        layout.addLayout(filter_bar)

        self.table = QTableWidget(0, len(TABLE_COLUMNS))
        self.table.setHorizontalHeaderLabels([item[1] for item in TABLE_COLUMNS])
        self.table.verticalHeader().setVisible(False)
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setFocusPolicy(Qt.StrongFocus)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.itemSelectionChanged.connect(self.on_selection_changed)
        layout.addWidget(self.table, 1)

        self.result_footer = QLabel("使用上下键可切换当前图表。")
        self.result_footer.setObjectName("footerLabel")
        layout.addWidget(self.result_footer)
        return panel

    def _build_chart_panel(self) -> QWidget:
        panel = QFrame()
        panel.setObjectName("panel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        chart_title = QLabel("K 线复盘")
        chart_title.setObjectName("sectionTitle")
        layout.addWidget(chart_title)

        self.chart_widget = ReviewChartWidget()
        layout.addWidget(self.chart_widget, 1)
        return panel

    def _apply_style(self):
        self.setStyleSheet(
            """
            QWidget {
                background: #eef4fb;
                color: #0f172a;
                font-family: "Microsoft YaHei";
                font-size: 13px;
            }
            QFrame#panel {
                background: #ffffff;
                border: 1px solid #d9e2ef;
                border-radius: 14px;
            }
            QFrame#summaryCard {
                background: #f8fbff;
                border: 1px solid #d7e3f2;
                border-radius: 12px;
            }
            QLabel#titleLabel {
                font-size: 24px;
                font-weight: 700;
                color: #0f172a;
            }
            QLabel#subtitleLabel {
                color: #475569;
                font-size: 13px;
            }
            QLabel#fieldLabel, QLabel#cardTitle, QLabel#footerLabel {
                color: #64748b;
                font-size: 12px;
            }
            QLabel#ruleHint {
                color: #1d4ed8;
                background: #eff6ff;
                border: 1px solid #bfdbfe;
                border-radius: 10px;
                padding: 10px 12px;
            }
            QLabel#cacheStatusLabel {
                color: #0f766e;
                background: #ecfeff;
                border: 1px solid #99f6e4;
                border-radius: 10px;
                padding: 10px 12px;
            }
            QLabel#sectionTitle {
                font-size: 15px;
                font-weight: 700;
                color: #0f172a;
            }
            QLabel#cardValue {
                font-size: 22px;
                font-weight: 700;
                color: #0f172a;
            }
            QPushButton {
                background: #1d4ed8;
                border: none;
                border-radius: 10px;
                color: white;
                font-weight: 600;
                padding: 10px 16px;
            }
            QPushButton:hover {
                background: #1e40af;
            }
            QPushButton:disabled {
                background: #94a3b8;
            }
            QComboBox, QDateEdit, QSpinBox, QDoubleSpinBox, QLineEdit {
                background: #ffffff;
                border: 1px solid #cbd5e1;
                border-radius: 10px;
                padding: 7px 10px;
                min-width: 110px;
            }
            QTableWidget {
                background: #ffffff;
                alternate-background-color: #f8fbff;
                gridline-color: #e2e8f0;
                border: 1px solid #d9e2ef;
                border-radius: 12px;
            }
            QHeaderView::section {
                background: #f1f5f9;
                color: #334155;
                padding: 8px;
                border: none;
                border-bottom: 1px solid #dbe4f0;
                font-weight: 600;
            }
            QTableWidget::item:selected {
                background: #dbeafe;
                color: #0f172a;
            }
            QStatusBar {
                background: #ffffff;
                border-top: 1px solid #d9e2ef;
            }
            """
        )

    def collect_params(self) -> ReviewParams:
        return ReviewParams(
            signal_date=self.signal_date_edit.date().toString("yyyy-MM-dd"),
            universe=self.universe_combo.currentData(),
            ema_period=self.ema_spin.value(),
            enable_ema_breakout=self.ema_check.isChecked(),
            volume_ma_window=self.volume_window_spin.value(),
            enable_volume_ratio=self.volume_check.isChecked(),
            volume_ratio_min=self.volume_ratio_spin.value(),
            daily_k_angle_window=self.daily_k_window_spin.value(),
            enable_daily_k_angle=self.daily_k_angle_check.isChecked(),
            daily_k_angle_min=self.daily_k_angle_spin.value(),
            relative_low_window=self.relative_low_window_spin.value(),
            enable_relative_low=self.relative_low_check.isChecked(),
            relative_low_position_max=self.relative_low_position_spin.value() / 100.0,
            enable_earnings_filter=self.earnings_check.isChecked(),
            earnings_forecast_change_min=self.earnings_forecast_spin.value(),
            earnings_yoy_min=self.earnings_yoy_spin.value(),
            enable_price_max=self.price_max_check.isChecked(),
            price_max=self.price_max_spin.value(),
            enable_turnover=self.turnover_check.isChecked(),
            turnover_min=self.turnover_spin.value(),
            hold_days=self.hold_days_spin.value(),
            lookback_days=160,
            adjustflag="2",
            exclude_st=True,
        )

    def collect_sync_params(self) -> SyncParams:
        sync_end_date = (
            QDate.currentDate().toString("yyyy-MM-dd")
            if self.sync_to_today_check.isChecked()
            else self.sync_end_date_edit.date().toString("yyyy-MM-dd")
        )
        return SyncParams(
            universe=self.universe_combo.currentData(),
            start_date=self.sync_start_date_edit.date().toString("yyyy-MM-dd"),
            end_date=sync_end_date,
            sync_history=self.sync_history_check.isChecked(),
            sync_earnings=self.sync_earnings_check.isChecked(),
            growth_quarters=self.growth_quarters_spin.value(),
            adjustflag="2",
            exclude_st=True,
            force_refresh=self.force_refresh_check.isChecked(),
        )

    def clear_result_filters(self):
        self.result_search_edit.blockSignals(True)
        self.result_price_min_spin.blockSignals(True)
        self.result_price_max_spin.blockSignals(True)
        self.result_search_edit.clear()
        self.result_price_min_spin.setValue(0.0)
        self.result_price_max_spin.setValue(0.0)
        self.result_search_edit.blockSignals(False)
        self.result_price_min_spin.blockSignals(False)
        self.result_price_max_spin.blockSignals(False)
        self.apply_result_filters()

    def apply_result_filters(self):
        keyword = self.result_search_edit.text().strip().lower()
        price_min = float(self.result_price_min_spin.value())
        price_max = float(self.result_price_max_spin.value())

        filtered: List[Dict] = []
        for row in self.current_rows:
            code = str(row.get("code", "")).lower()
            name = str(row.get("name", "")).lower()
            signal_close = float(row.get("signal_close", 0.0) or 0.0)

            if keyword and keyword not in code and keyword not in name:
                continue
            if price_min > 0 and signal_close < price_min:
                continue
            if price_max > 0 and signal_close > price_max:
                continue
            filtered.append(row)

        self.filtered_rows = filtered
        self.populate_table(self.filtered_rows)
        self._update_filter_status()

        if self.filtered_rows:
            self.select_row(0)
        elif self.current_rows:
            self.chart_widget.show_message("当前结果筛选条件下没有股票。")
            self.statusBar().showMessage("结果筛选后无匹配股票")

    def _update_filter_status(self):
        self.filter_status_label.setText(f"显示 {len(self.filtered_rows)} / {len(self.current_rows)}")

    def _sync_filter_states(self):
        self.ema_spin.setEnabled(self.ema_check.isChecked())
        self.volume_window_spin.setEnabled(self.volume_check.isChecked())
        self.volume_ratio_spin.setEnabled(self.volume_check.isChecked())
        self.daily_k_window_spin.setEnabled(self.daily_k_angle_check.isChecked())
        self.daily_k_angle_spin.setEnabled(self.daily_k_angle_check.isChecked())
        self.relative_low_window_spin.setEnabled(self.relative_low_check.isChecked())
        self.relative_low_position_spin.setEnabled(self.relative_low_check.isChecked())
        self.earnings_forecast_spin.setEnabled(self.earnings_check.isChecked())
        self.earnings_yoy_spin.setEnabled(self.earnings_check.isChecked())
        self.price_max_spin.setEnabled(self.price_max_check.isChecked())
        self.turnover_spin.setEnabled(self.turnover_check.isChecked())
        self.growth_quarters_spin.setEnabled(self.sync_earnings_check.isChecked())

    def _sync_date_mode_changed(self):
        use_today = self.sync_to_today_check.isChecked()
        self.sync_end_date_edit.setEnabled(not use_today)
        if use_today:
            self.sync_end_date_edit.setDate(QDate.currentDate())

    def refresh_cache_status(self):
        status = self.engine.get_cache_status()
        latest_date = status.get("latest_date") or "--"
        cache_count = status.get("cache_count", 0)
        self.cache_status_label.setText(f"当前数据最新日期：{latest_date}    本地缓存：{cache_count} 只股票")

    def _update_rule_hint(self):
        params = self.collect_params()
        universe_label = UNIVERSE_LABELS.get(params.universe, params.universe)
        text = f"{universe_label} | {build_rule_text(params)}"
        self.rule_hint_label.setText(text)

    def run_scan(self):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.information(self, "正在运行", "当前已有筛选任务在运行，请先等待完成。")
            return

        self.current_params = self.collect_params()
        self.current_rows = []
        self.filtered_rows = []
        self.current_report_path = ""
        self.table.setRowCount(0)
        self._update_filter_status()
        self.chart_widget.show_message("正在筛选，请稍候...")
        self.result_footer.setText("正在检查缓存并筛选...")
        self._update_summary({})
        self.open_report_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self._start_worker(
            ScanWorker(self.project_root, self.current_params),
            task_name="scan",
            progress_slot=self.on_scan_progress,
            finished_slot=self.on_scan_finished,
            failed_slot=self.on_scan_failed,
            start_message="开始筛选...",
        )

    def run_sync(self):
        if self.worker_thread and self.worker_thread.isRunning():
            QMessageBox.information(self, "正在运行", "当前已有任务在运行，请先等待完成。")
            return

        sync_params = self.collect_sync_params()
        if not sync_params.sync_history and not sync_params.sync_earnings:
            QMessageBox.warning(self, "同步内容为空", "请至少勾选“同步日线”或“同步业绩”。")
            return
        if sync_params.start_date > sync_params.end_date:
            QMessageBox.warning(self, "日期范围错误", "同步开始日期不能晚于同步结束日期。")
            return

        sync_mode_text = build_sync_mode_text(sync_params)
        self.progress_bar.setValue(0)
        self.result_footer.setText(f"正在同步 {sync_mode_text}...")
        self.statusBar().showMessage(f"开始同步 {sync_mode_text}...")
        self._start_worker(
            SyncWorker(self.project_root, sync_params),
            task_name="sync",
            progress_slot=self.on_sync_progress,
            finished_slot=self.on_sync_finished,
            failed_slot=self.on_sync_failed,
            start_message=f"开始同步 {sync_mode_text}...",
        )

    def cancel_task(self):
        if self.worker:
            self.worker.cancel()
            if self.active_task == "sync":
                self.statusBar().showMessage("正在取消同步...")
            else:
                self.statusBar().showMessage("正在取消筛选...")

    def _start_worker(self, worker, task_name: str, progress_slot, finished_slot, failed_slot, start_message: str):
        self.active_task = task_name
        self.run_button.setEnabled(False)
        self.sync_button.setEnabled(False)
        self.stop_button.setEnabled(True)

        self.worker_thread = QThread(self)
        self.worker = worker
        self.worker.moveToThread(self.worker_thread)

        self.worker_thread.started.connect(self.worker.run)
        self.worker.progress.connect(progress_slot)
        self.worker.finished.connect(finished_slot)
        self.worker.failed.connect(failed_slot)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.failed.connect(self.worker_thread.quit)
        self.worker_thread.finished.connect(self._cleanup_worker)
        self.worker_thread.start()
        self.statusBar().showMessage(start_message)

    def on_scan_progress(self, current: int, total: int, message: str):
        progress = int(current / total * 100) if total else 0
        self.progress_bar.setValue(progress)
        self.statusBar().showMessage(message)
        self.result_footer.setText(message)

    def on_scan_finished(self, outcome: Dict):
        self.current_report_path = outcome.get("report_path", "")
        self.current_rows = outcome.get("rows", [])
        self.current_params = ReviewParams(**outcome.get("params", asdict(self.current_params)))
        self._update_summary(outcome.get("summary", {}))
        self.apply_result_filters()

        if self.current_rows:
            self.result_footer.setText(
                f"共命中 {len(self.current_rows)} 只股票。可在表头上方二次筛选，按上下键翻图。"
            )
            self.open_report_button.setEnabled(bool(self.current_report_path))
        else:
            self.chart_widget.show_message("本次条件下没有筛出股票。")
            self.result_footer.setText("本次条件下没有筛出股票。")

        self.progress_bar.setValue(100 if self.current_rows else 0)
        self.statusBar().showMessage("筛选完成")

    def on_scan_failed(self, message: str):
        if message == "筛选已取消。":
            self.chart_widget.show_message("筛选已取消。")
            self.result_footer.setText("筛选已取消。")
            self.statusBar().showMessage("筛选已取消")
            self.progress_bar.setValue(0)
            return

        self.chart_widget.show_message("运行失败，请检查网络或数据源后重试。")
        self.result_footer.setText(message)
        self.statusBar().showMessage("运行失败")
        QMessageBox.critical(self, "运行失败", message)

    def on_sync_progress(self, current: int, total: int, message: str):
        progress = int(current / total * 100) if total else 0
        self.progress_bar.setValue(progress)
        self.statusBar().showMessage(message)
        self.result_footer.setText(message)

    def on_sync_finished(self, outcome: Dict):
        summary = outcome.get("summary", {})
        failed_rows = outcome.get("failed_rows", [])
        sync_label = summary.get("sync_label", "日线")
        processed = summary.get("processed", 0)
        self.refresh_cache_status()
        self.progress_bar.setValue(100)
        self.chart_widget.show_message("数据同步完成。现在可以直接做条件筛选，避免边筛边拉数据。")
        footer_parts = [f"同步完成（{sync_label}）", f"成功 {processed}", f"失败 {summary.get('failed', 0)}"]
        if summary.get("sync_history"):
            footer_parts.insert(1, f"新建 {summary.get('created', 0)}")
            footer_parts.insert(2, f"更新 {summary.get('updated', 0)}")
            footer_parts.insert(3, f"复用 {summary.get('reused', 0)}")
        if not summary.get("sync_earnings"):
            footer_parts.append("当前未更新业绩库")
        self.result_footer.setText("，".join(footer_parts) + "。")
        self.statusBar().showMessage("数据同步完成")

        if failed_rows:
            preview = "\n".join(f"{row['code']} {row['error']}" for row in failed_rows[:5])
            more_text = "" if len(failed_rows) <= 5 else f"\n... 另有 {len(failed_rows) - 5} 只失败"
            QMessageBox.warning(
                self,
                "同步完成，但有部分失败",
                "以下股票同步失败：\n" + preview + more_text,
            )

    def on_sync_failed(self, message: str):
        if message == "数据同步已取消。":
            self.result_footer.setText("数据同步已取消。")
            self.statusBar().showMessage("数据同步已取消")
            self.progress_bar.setValue(0)
            return

        self.result_footer.setText(message)
        self.statusBar().showMessage("同步失败")
        QMessageBox.critical(self, "同步失败", message)

    def _cleanup_worker(self):
        self.run_button.setEnabled(True)
        self.sync_button.setEnabled(True)
        self.stop_button.setEnabled(False)
        if self.worker:
            self.worker.deleteLater()
        if self.worker_thread:
            self.worker_thread.deleteLater()
        self.worker = None
        self.worker_thread = None
        self.active_task = ""

    def populate_table(self, rows: List[Dict]):
        self.table.blockSignals(True)
        self.table.clearSelection()
        self.table.clearContents()
        self.table.setRowCount(len(rows))

        for row_index, row in enumerate(rows):
            for col_index, (column, _) in enumerate(TABLE_COLUMNS):
                item = QTableWidgetItem(format_value(column, row.get(column)))
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row_index, col_index, item)

        self.table.blockSignals(False)
        self.table.resizeColumnsToContents()

    def _update_summary(self, summary: Dict):
        value_map = {
            "count": f"{summary.get('count', 0)}",
            "avg_return_pct": f"{summary.get('avg_return_pct', 0.0):.2%}",
            "median_return_pct": f"{summary.get('median_return_pct', 0.0):.2%}",
            "win_rate": f"{summary.get('win_rate', 0.0):.2%}",
            "best_return_pct": f"{summary.get('best_return_pct', 0.0):.2%}",
            "worst_return_pct": f"{summary.get('worst_return_pct', 0.0):.2%}",
        }
        for key, label in self.summary_labels.items():
            label.setText(value_map.get(key, "--"))

    def select_row(self, row_index: int):
        if self.table.rowCount() == 0:
            return
        row_index = max(0, min(row_index, self.table.rowCount() - 1))
        self.table.setCurrentCell(row_index, 0)
        item = self.table.item(row_index, 0)
        if item:
            self.table.scrollToItem(item)

    def select_next_row(self):
        if self.table.rowCount() == 0:
            return
        current_row = self.table.currentRow()
        if current_row < 0:
            current_row = 0
        self.select_row(min(current_row + 1, self.table.rowCount() - 1))

    def select_previous_row(self):
        if self.table.rowCount() == 0:
            return
        current_row = self.table.currentRow()
        if current_row < 0:
            current_row = 0
        self.select_row(max(current_row - 1, 0))

    def on_selection_changed(self):
        row_index = self.table.currentRow()
        if row_index < 0 or row_index >= len(self.filtered_rows):
            return

        row = self.filtered_rows[row_index]
        try:
            payload = self.engine.load_chart_payload(to_bs_code(row["code"]), self.current_params)
        except Exception as exc:
            self.chart_widget.show_message(str(exc))
            self.statusBar().showMessage("图表加载失败")
            return

        self.chart_widget.plot_review(payload, row)
        self.statusBar().showMessage(
            f"{row['code']} {row['name']} | 收益 {row.get('hold_return_pct', 0.0):.2%}"
        )

    def open_report_dir(self):
        if not self.current_report_path:
            return
        report_dir = str(Path(self.current_report_path).parent)
        os.startfile(report_dir)

    def open_cache_dir(self):
        os.startfile(str(self.engine.cache_dir))


def build_app() -> QApplication:
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    app = QApplication(sys.argv)
    app.setApplicationName("KHQuant Review Screener")
    app.setStyle("Fusion")
    font = QFont("Microsoft YaHei", 10)
    app.setFont(font)
    icon_path = get_icon_path("stock_icon.ico")
    if not icon_path.exists():
        icon_path = get_icon_path("stock_icon.png")
    if icon_path.exists():
        app.setWindowIcon(QIcon(str(icon_path)))
    return app


def main():
    app = build_app()
    lock_path = Path(tempfile.gettempdir()) / "khquant_stock_screener.lock"
    instance_lock = QLockFile(str(lock_path))
    if not instance_lock.tryLock(100):
        QMessageBox.information(None, "已在运行", "日线复盘选股器已经在运行，请先关闭已打开的窗口后再启动。")
        return 0
    app.instance_lock = instance_lock
    project_root = get_app_root()
    window = MainWindow(project_root)
    window.show()
    exit_code = app.exec_()
    if instance_lock.isLocked():
        instance_lock.unlock()
    return exit_code
