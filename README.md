# 美股选股手册

美股三大指数成分股、常用 ETF 和期权交易速查。期权数据来自 Cboe 公开的延时行情，
每个美股交易日收盘后（北京时间早上 7:30 左右）由 GitHub Actions 自动更新。

- `src/fetch_cboe.py`：抓网站上个股、ETF、指数的期权链，算成交量、P/C、IV30、平值买卖价差、近期到期日
- `src/build.py`：把数据嵌进 `src/template.html`，生成 `index.html`
- `.github/workflows/update.yml`：定时运行上面两个脚本并发布到 GitHub Pages

手动更新：在仓库的 Actions 页面选“每日更新期权数据”，点 Run workflow。

数据仅供参考，是延时快照，不构成投资建议。
