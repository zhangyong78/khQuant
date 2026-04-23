# 看海量化交易系统 (KHQuant) - 快速入门手册

> **注意**: 本文档是看海量化交易系统 (KHQuant) 的快速入门指南，内容节选自官方文档的第一至四章。我们强烈建议您访问[看海量化官方网站](https://khsci.com/khQuant/)以获取最新、最完整的用户手册和更多高级功能。

---

## 📖 关于本开源代码

本仓库提供的是看海量化交易系统的**完整程序源码**，面向具备一定编程基础的开发者和量化研究人员。

**如果您希望直接使用，无需配置开发环境，可以选择：**

💡 **推荐使用打包好的exe程序** - 已经打包好的可执行程序，开箱即用，无需安装Python环境和依赖库。

🔗 **获取方式**：请访问[看海量化官方网站](https://khsci.com/khQuant/)下载最新的安装包。

## 源码版快速启动

如果你需要直接运行源码版，可以按下面的方式启动：

```powershell
python -m pip install -r requirements.txt
python GUIkhQuant.py
```

新增的日线复盘选股器可以单独启动：

```powershell
.\start_review_screener.bat
```

或者：

```powershell
.\.venv311\Scripts\python -m stock_screener
```

## 2026-04 发布更新

- 新增独立的 `stock_screener` 日线复盘选股器，支持条件筛选、次日开盘买入回测和 K 线复核。
- 复盘缓存从逐股 CSV 升级为 `DuckDB` 本地数据库，更适合全市场扫描。
- 依赖清单新增 `baostock` 与 `duckdb`，源码版环境请重新安装 `requirements.txt`。

## 使用提示

- `stock_screener` 当前按单实例使用设计，同一份本地缓存数据库不要重复打开多个窗口。
- 发布源码时建议不要携带 `.venv311/`、`stock_screener/cache/`、`stock_screener/reports/`、`__pycache__/` 等本地产物。

## Windows 便携版

当前发布方案按 `stock_screener` 单独便携版执行：

- 产物为一个 zip，用户解压后直接双击 exe 即可运行
- 数据默认写到 exe 同目录下的 `cache/` 与 `reports/`
- 内置股票池 CSV 会随 `data/` 目录一起打包
- 便携版不依赖本机预装 Python
