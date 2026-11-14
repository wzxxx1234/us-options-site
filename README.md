# 美股选股手册

美股三大指数成分股、常用 ETF 和期权交易速查。期权数据来自 Cboe 公开的延时行情，
每个美股交易日收盘后（北京时间早上 6 点左右，GitHub 常会晚一些）由 GitHub Actions 自动更新。

线上网址：https://wzxxx1234.github.io/us-options-site/

- `src/fetch_cboe.py`：抓网站上个股、ETF、指数的期权链，算成交量、P/C、IV30、平值买卖价差、近期到期日
- `src/build.py`：把数据嵌进 `src/template.html`，生成 `index.html`
- `src/data/`：网站数据（`data.json` 公司和 ETF 资料、`options_latest.json` 最新期权数据和新闻、`intraday.json` 当天分时走势、`hist.json` 每日期权成交和 IV30 记录、`mcap.json` 市值）
- `.github/workflows/update.yml`：定时运行上面两个脚本并发布到 GitHub Pages；完整更新后把最新数据存回 `src/data/`

在电脑上拿最新数据：在这个文件夹里运行 `git pull`。本地预览：`python3 src/build.py` 后运行 `python3 src/serve.py`，打开 http://127.0.0.1:8765/ 。

手动更新：在仓库的 Actions 页面选“每日更新期权数据”，点 Run workflow。

数据仅供参考，是延时快照，不构成投资建议。
