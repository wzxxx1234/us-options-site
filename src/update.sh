#!/bin/zsh
# 抓 Cboe 期权数据并重新生成网站：在终端里运行  zsh ~/Desktop/美股选股/src/update.sh
set -e
cd "$(dirname "$0")"
python3 -I fetch_cboe.py
python3 -I build.py
echo "已更新 $(cd .. && pwd)/index.html"
