# KHQuant 2.2.1 发布说明

发布日期：2026-04-24

## 主要变化

- 新增 `stock_screener` Windows 便携版，解压后可直接运行。
- 便携版数据统一写在 exe 同目录下的 `cache/` 和 `reports/`。
- 修复便携包入口的相对导入问题，避免启动时报 `attempted relative import with no known parent package`。
- 增加单实例保护，避免同一份数据库被重复打开。

## 打包说明

- 发布物：`KHQuantReviewScreener-v2.2.1-windows-portable.zip`
- 可执行文件：`KHQuantReviewScreener.exe`

## 使用提示

- 首次运行后会在解压目录下生成 `cache/`、`reports/`。
- 请保持整个解压目录结构完整，不要只单独移动 exe。
