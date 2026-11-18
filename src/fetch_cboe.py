"""从 Cboe 公开的延时行情接口抓网站上个股、ETF、指数的期权数据，生成 data/options_latest.json。

用法：python3 fetch_cboe.py [--limit N]
数据来源：
  - 可交易期权名单  https://www.cboe.com/us/options/symboldir/?download=csv
  - 周度期权名单    https://www.cboe.com/available_weeklys/get_csv_download/
  - 每只标的期权链  https://cdn.cboe.com/api/global/delayed_quotes/options/<代码>.json
这些接口 Cboe 没有正式公开说明，格式可能随时变化。
"""
import csv, io, json, os, re, sys, time, datetime, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
UA = {'User-Agent': 'Mozilla/5.0'}

def get(url, tries=6, timeout=90):
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
                return r.read()
        except Exception as e:
            # 403/404 = Cboe 明确没有这只的数据（比如已退市或不提供），重试也没用，直接放弃
            if k == tries - 1 or getattr(e, 'code', 0) in (403, 404):
                raise
            # 429 = 请求太频繁，多等一会儿
            time.sleep((20 if getattr(e, 'code', 0) == 429 else 3) * (k + 1))

# 指数期权：网站上的代码 -> Cboe 上的期权代码（前面加下划线）
INDEX_OPT = {'SPX': ['SPX', 'XSP'], 'NDX': ['NDX', 'XND'], 'DJI': ['DJX'], 'RUT': ['RUT', 'MRUT'],
             'OEX': ['OEX'], 'VIX': ['VIX']}

# ---------- 名单 ----------
ETF_YES = re.compile(r'\b(ETF|ETN|ETP|ETFS|ISHARES|SPDR|PROSHARES|DIREXION|INVESCO|VANECK|GLOBAL X|WISDOMTREE|GRANITESHARES|'
                     r'ROUNDHILL|YIELDMAX|DEFIANCE|TIDAL|T-REX|TRADR|LEVERAGE SHARES|KRANESHARES|FIRST TR|FIRST TRUST|'
                     r'SELECT SECTOR|ULTRA|ULTRAPRO|ULTRASHORT|2X|3X|-1X|1X|VANGUARD|SCHWAB|FIDELITY COVINGTON|'
                     r'TEUCRIUM|UNITED STS|UNITED STATES (OIL|NATURAL|GASOLINE|BRENT|12)|GRAYSCALE|BITWISE|ARK |'
                     r'CURRENCYSHARES|INDEX FD|ETF TR|EXCHANGE[- ]TRADED|MICROSECTORS|STRATEGY SHARES|AMPLIFY|'
                     r'PACER|J P MORGAN EXCHANGE|JPMORGAN|DIMENSIONAL|SPROTT|ABRDN|NEOS|SIMPLIFY)\b', re.I)
ETF_NO = re.compile(r'\b(INC|CORP|CORPORATION|BANCORP|HOLDINGS?|HLDGS|PLC|LTD|LIMITED|N\.?V\.?|S\.?A\.?|AG|ADR|ADS|'
                     r'REALTY|PROPERTIES|BANCSHARES|COMPANY|CO|L\.?P\.? UNIT|ACQUISITION)\b', re.I)
ETF_FORCE_NO = re.compile(r'\b(SPROTT INC|INVESCO LTD|WISDOMTREE INC|VANGUARD NATURAL|SCHWAB CHARLES|FIRST TR(UST)? BANK)\b', re.I)

ETF_PREFIX = re.compile(r'^(ISHARES|SPDR|PROSHARES|DIREXION|INVESCO (EXCHANGE|DB|QQQ|CURRENCY|ACTIVELY)|VANECK|GLOBAL X|'
                        r'WISDOMTREE TR|ADVISORSHARES|EA SERIES|EXCHANGE LISTED|FIRST TR(UST)? (EXCHANGE|NASDAQ|LARGE|VALUE|ENERGY|SER)|'
                        r'SELECT SECTOR|TIDAL|ETF (SER|OPPORTUNITIES|MANAGERS)|AMPLIFY|PACER|J ?P ?MORGAN EXCHANGE|ROUNDHILL|'
                        r'GRANITESHARES|DEFIANCE|VANGUARD (INTL|INDEX|WORLD|SPECIALIZED|BD|SCOTTSDALE|ADMIRAL|WHITEHALL|STAR|TAX|MUN)|'
                        r'SCHWAB STRATEGIC|KRANESHARES|THEMES ETF|DBX ETF|COLUMBIA ETF|SPROTT (PHYSICAL|URANIUM|GOLD)|'
                        r'UNITED STS|UNITED STATES (OIL|NATURAL|GASOLINE|BRENT|12)|TEUCRIUM|GRAYSCALE)', re.I)

def is_etf(name):
    if ETF_FORCE_NO.search(name):
        return False
    if ETF_PREFIX.search(name) or re.search(r'\b(BITCOIN|ETHER(EUM)?|SOLANA|XRP)\b.*\b(FUND|TRUST|TR)\b', name, re.I):
        return True
    if re.search(r'\b(ETF|ETN)\b', name, re.I):
        return True
    return bool(ETF_YES.search(name)) and not ETF_NO.search(name.replace('FD LP', 'FD'))

def symbol_dir():
    t = get('https://www.cboe.com/us/options/symboldir/?download=csv').decode('utf-8', 'replace')
    rows = list(csv.reader(io.StringIO(t)))[1:]
    return {r[1].strip(): r[0].strip() for r in rows if len(r) > 1 and r[1].strip()}

def weeklys():
    t = get('https://www.cboe.com/available_weeklys/get_csv_download/').decode('utf-8', 'replace')
    etp, eq, sec = set(), set(), None
    for line in t.splitlines():
        if 'Exchange Traded Products' in line: sec = etp; continue
        if 'Available Weeklys - Equity' in line: sec = eq; continue
        if sec is None or not line.strip(): continue
        r = next(csv.reader([line]))
        if r and re.fullmatch(r'[A-Z][A-Z.]{0,6}', r[0]): sec.add(r[0])
    return etp, eq

# ---------- 市值（纳斯达克网站公开的股票筛选器，单位：亿美元） ----------
def market_caps(tickers):
    """先从纳斯达克取最新市值，取不到的用仓库里的 data/mcap.json 补上。"""
    out, src = {}, 'data/mcap.json'
    try:
        req = urllib.request.Request('https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&download=true',
                                     headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120 Safari/537.36',
                                              'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=60) as r:
            rows = json.loads(r.read())['data']['rows']
        for row in rows:
            try:
                v = float(row.get('marketCap') or 0)
            except ValueError:
                v = 0
            if v > 0:
                out[row['symbol'].replace('/', '.')] = round(v / 1e8, 1)
        src = 'Nasdaq stock screener'
    except Exception as e:
        print('纳斯达克市值取不到，改用 data/mcap.json：', e, flush=True)
    try:
        old = json.load(open(os.path.join(DATA, 'mcap.json')))['data']
    except Exception:
        old = {}
    return {t: out.get(t) or old.get(t) for t in tickers if out.get(t) or old.get(t)}, src

# ---------- 财报日期（纳斯达克网站公开的财报日历） ----------
def earnings(days=28):
    """未来 days 天内的财报日：{代码: [日期, 'pre'|'post'|'']}。"""
    out, d0 = {}, datetime.date.today()
    hdr = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120 Safari/537.36',
           'Accept': 'application/json'}
    for k in range(days):
        day = d0 + datetime.timedelta(days=k)
        if day.weekday() >= 5: continue
        try:
            req = urllib.request.Request(f'https://api.nasdaq.com/api/calendar/earnings?date={day.isoformat()}', headers=hdr)
            with urllib.request.urlopen(req, timeout=40) as r:
                rows = (json.loads(r.read()).get('data') or {}).get('rows') or []
        except Exception as e:
            print('财报日历取不到', day, e, flush=True); continue
        for row in rows:
            t = (row.get('symbol') or '').replace('/', '.')
            tm = row.get('time') or ''
            if t and t not in out:
                out[t] = [day.isoformat(), 'pre' if 'pre' in tm else 'post' if 'after' in tm else '']
        time.sleep(0.3)
    return out

# ---------- 新闻标题（雅虎财经公开的 RSS，只存标题、链接、来源时间） ----------
def headlines(sym, n=4):
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime
    url = f'https://feeds.finance.yahoo.com/rss/2.0/headline?s={urllib.parse.quote(sym)}&region=US&lang=en-US'
    root = ET.fromstring(get(url, tries=2, timeout=30))
    out = []
    for it in root.iter('item'):
        t, l, d = (it.findtext('title') or '').strip(), (it.findtext('link') or '').strip(), it.findtext('pubDate') or ''
        try:
            d = parsedate_to_datetime(d).astimezone(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%MZ')
        except Exception:
            d = ''
        if t and l.startswith('https://'):
            out.append([t[:160], l, d])
        if len(out) >= n: break
    return out

def news(symbols):
    out = {}
    for s in ['^GSPC'] + symbols:
        try:
            h = headlines(s, 8 if s == '^GSPC' else 4)
            if h: out['_mkt' if s == '^GSPC' else s] = h
        except Exception as e:
            print('新闻取不到', s, e, flush=True)
        time.sleep(0.4)
    return out

# ---------- 中文新闻 ----------
def sina_us(n=8):
    """新浪财经美股频道的中文滚动新闻（只留美股栏目的文章）。"""
    j = json.loads(get('https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&num=50&page=1', tries=2, timeout=30))
    out = []
    for x in j['result']['data']:
        u = x.get('url', '')
        if '/usstock/' not in u or not u.startswith('https://'): continue
        d = datetime.datetime.fromtimestamp(int(x['ctime']), datetime.timezone.utc).strftime('%Y-%m-%dT%H:%MZ')
        out.append([x['title'].strip()[:120], u, d])
        if len(out) >= n: break
    return out

def translate(titles, old=None, budget=4800):
    """英文标题机器翻译成中文（MyMemory 免费接口，每天约 5000 字符额度）。
    old 是上一次网站上已经翻过的 {英文: 中文}，直接复用，不占额度。翻不了的就留英文。"""
    zh = {t: old[t] for t in titles if old and t in old}
    todo = [t for t in dict.fromkeys(titles) if t not in zh]
    used, batch = 0, []
    def run(b):
        q = urllib.parse.urlencode({'q': '\n'.join(b), 'langpair': 'en|zh-CN'})
        j = json.loads(get('https://api.mymemory.translated.net/get?' + q, tries=2, timeout=30))
        if j.get('quotaFinished') or int(j.get('responseStatus') or 0) != 200:
            raise RuntimeError('额度用完或出错：' + str(j.get('responseDetails'))[:80])
        r = j['responseData']['translatedText'].split('\n')
        if len(r) == len(b):
            for a, c in zip(b, r):
                c = c.strip()
                if c and c != a and re.search(r'[\u4e00-\u9fff]', c): zh[a] = c
        time.sleep(1)
    try:
        for t in todo:
            if used + len(t) > budget: break
            if batch and len('\n'.join(batch + [t])) > 480:
                run(batch); batch = []
            batch.append(t); used += len(t)
        if batch: run(batch)
    except Exception as e:
        print('翻译中断', e, flush=True)
    return zh

# ---------- 分时走势：个股和 ETF 用纳斯达克的 1 分钟线，指数用 Cboe 的；压缩成 5 分钟一个点 ----------
# （Cboe 的个股分时文件只有热门股是当天的，冷门股常停在几天前，所以不用）
NQ = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36',
      'Accept': 'application/json'}

def _bars(sym, kind, day):
    """返回 [(美东分钟数, 价格)] 和昨收（取不到就是 None）。"""
    if kind == 2 or '.' in sym:   # 指数；以及纳斯达克查不到的 BRK.B 这类带点的代码（Cboe 的可能不是当天的，日期不对就不画）
        j = json.loads(get(f'https://cdn.cboe.com/api/global/delayed_quotes/charts/intraday/{"_" if kind == 2 else ""}{sym}.json', tries=3, timeout=60))
        return [(int(b['datetime'][11:13]) * 60 + int(b['datetime'][14:16]), (b.get('price') or {}).get('close'))
                for b in j.get('data', []) if b.get('datetime', '')[:10] == day], None
    q = urllib.parse.quote(sym.replace('.', '/'), safe='')
    for k in range(3):
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    f'https://api.nasdaq.com/api/quote/{q}/chart?assetclass={"etf" if kind == 1 else "stocks"}', headers=NQ), timeout=30) as r:
                j = json.loads(r.read()); break
        except Exception:
            if k == 2: raise
            time.sleep(3 * (k + 1))
    d = j.get('data') or {}
    out = []
    for b in d.get('chart') or []:
        t = datetime.datetime.fromtimestamp(b['x'] / 1000, datetime.timezone.utc)   # 纳斯达克的时间戳是“美东钟点当成 UTC”
        if t.date().isoformat() == day: out.append((t.hour * 60 + t.minute, b.get('y')))
    try:
        prev = float(str(d.get('previousClose', '')).replace('$', '').replace(',', ''))
    except ValueError:
        prev = None
    return out, prev

def _pack(bars, prev, px):
    """1 分钟线 -> 9:30 起每 5 分钟一格（共 79 格，最后一格是 16:00 收盘），存成相对昨收的万分比整数。"""
    pts = [None] * 79
    last = -1
    for m, c in bars:
        if not c or not 570 < m <= 960: continue   # 只要正常交易时段
        i = min((m - 571) // 5, 77)
        pts[i] = c; last = max(last, i)
    if last < 6 or not prev: return None
    pts[last + 1] = px or pts[last]   # 收盘价接在最后（提前收盘的日子就在中间结束）
    for i in range(1, last + 2):  # 中间没成交的格子沿用前一格
        if pts[i] is None: pts[i] = pts[i - 1]
    if pts[0] is None:
        k = next(i for i, v in enumerate(pts) if v is not None); pts[:k] = [pts[k]] * k
    # 网页再用昨收换算回价格
    return [round(prev, 4), [round((v / prev - 1) * 1e4) for v in pts[:last + 2]]]

def intraday_one(sym, kind, day, px, chg):
    bars, prev = _bars(sym, kind, day)
    prev = prev or (px / (1 + chg / 100) if px and chg > -100 else 0)
    return _pack(bars, prev, px)

# 没有指数期权的指数：能取到当天走势的直接取（网页上的代码: (来源, 那边的代码)）
INDEX_CHART = {'IXIC': ('nq', 'COMP'), 'SOX': ('nq', 'SOX'), 'BKX': ('nq', 'BKX'),
               'DJT': ('cboe', 'DJT'), 'RUI': ('cboe', 'RUI')}
# 取不到的用跟踪它的 ETF 代替（网站上没有的这几只单独取）
PROXY_ETF = ['IWB', 'IWV', 'UUP']

def intraday_index(src, sym, day):
    if src == 'cboe':
        bars, _ = _bars(sym, 2, day)
        q = json.loads(get(f'https://cdn.cboe.com/api/global/delayed_quotes/quotes/_{sym}.json', tries=3, timeout=60))['data']
        prev, px = q.get('prev_day_close'), q.get('current_price')
        if not prev or prev == px: return None   # 昨收和现价一样，多半是 Cboe 没更新昨收，不用
        return _pack(bars, prev, None)
    q = urllib.parse.quote(sym, safe='')
    with urllib.request.urlopen(urllib.request.Request(
            f'https://api.nasdaq.com/api/quote/{q}/chart?assetclass={"index" if src == "nq" else "etf"}', headers=NQ), timeout=30) as r:
        d = json.loads(r.read()).get('data') or {}
    bars = []
    for b in d.get('chart') or []:
        t = datetime.datetime.fromtimestamp(b['x'] / 1000, datetime.timezone.utc)
        if t.date().isoformat() == day: bars.append((t.hour * 60 + t.minute, b.get('y')))
    try:
        prev = float(str(d.get('previousClose', '')).replace('$', '').replace(',', ''))
    except ValueError:
        return None
    return _pack(bars, prev, None)

def intraday(res, day):
    out, t0, fails = {}, time.time(), []
    with ThreadPoolExecutor(6) as ex:
        fut = {ex.submit(intraday_one, s, r['k'], day, r['px'], r['chg']): s
               for s, r in res.items() if r.get('px')}
        for f in as_completed(fut):
            try:
                v = f.result()
                if v: out[fut[f]] = v
            except Exception as e:
                fails.append(fut[f])
    extra = [(k, v) for k, v in INDEX_CHART.items()] + [(e, ('etf', e)) for e in PROXY_ETF]
    for k, (src, sym) in extra:
        try:
            v = intraday_index(src, sym, day)
            if v: out[k] = v
            else: fails.append(k)
        except Exception:
            fails.append(k)
        time.sleep(0.5)
    if fails: print(f'分时取不到 {len(fails)} 只：{" ".join(sorted(fails)[:40])}', flush=True)
    json.dump({'d': day, 's': out}, open(os.path.join(DATA, 'intraday.json'), 'w'), separators=(',', ':'))
    print(f'分时走势 {len(out)} 只，用时 {time.time() - t0:.0f}s', flush=True)

# ---------- 小盘正股池：每天从全市场里筛出“小盘 + 有流动性”的股票，算好指标，存成 data/small.json ----------
# 门槛：市值 3000 万 ~ 20 亿美元，股价 ≥ 2 美元，当天成交额 ≥ 500 万美元（太冷清的进出都吃亏，不收）
SMALL_RULE = {'mcap_min': 3e7, 'mcap_max': 20e8, 'price_min': 2.0, 'dv_min': 5e6}
SEC_CN = {'Health Care': '医疗保健', 'Finance': '金融', 'Technology': '科技', 'Consumer Discretionary': '可选消费',
          'Industrials': '工业', 'Energy': '能源', 'Real Estate': '房地产', 'Basic Materials': '材料',
          'Consumer Staples': '必需消费', 'Utilities': '公用事业', 'Telecommunications': '通信', 'Miscellaneous': '其他'}
JUNK = re.compile(r'Acquisition (Corp|Corporation|Co)\b|\bWarrants?\b|\bRights?\b|\bUnits?\b|Depositary Shares|Preferred', re.I)
NAME_TAIL = re.compile(r'\s+(Class [A-Z]\s+)?(Common Stock|Ordinary Shares?|Common Shares?|Shares of Beneficial Interest|American Depositary Shares?).*$', re.I)

def _num(x):
    try:
        return float(str(x).replace('$', '').replace(',', '').replace('%', '').strip())
    except Exception:
        return 0.0

def nq_json(url, tries=3):
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=NQ), timeout=30) as r:
                return json.loads(r.read())
        except Exception as e:
            if k == tries - 1 or getattr(e, 'code', 0) in (400, 404):
                return None
            time.sleep((10 if getattr(e, 'code', 0) == 429 else 2) * (k + 1))

def small_pool():
    j = nq_json('https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=10000&download=true')
    rows = j['data']['rows']
    pool = []
    for r in rows:
        s = r['symbol']
        if not re.fullmatch(r'[A-Z]{1,5}', s): continue      # 只要普通代码（不含 BRK/B 这类、权证后缀）
        p, mc, v = _num(r['lastsale']), _num(r['marketCap']), _num(r['volume'])
        if not (SMALL_RULE['mcap_min'] <= mc < SMALL_RULE['mcap_max'] and p >= SMALL_RULE['price_min'] and p * v >= SMALL_RULE['dv_min']): continue
        if JUNK.search(r['name']): continue
        pool.append(r)
    return pool, len(rows)

def _hist(sym, day):
    d1 = datetime.date.fromisoformat(day)
    j = nq_json(f'https://api.nasdaq.com/api/quote/{sym}/historical?assetclass=stocks&fromdate={(d1 - datetime.timedelta(days=380)).isoformat()}&limit=300&todate={day}')
    rows = ((j or {}).get('data') or {}).get('tradesTable', {}) or {}
    out = []
    for r in rows.get('rows') or []:
        try:
            m, d, y = r['date'].split('/')
            out.append((f'{y}-{m}-{d}', _num(r['open']), _num(r['high']), _num(r['low']), _num(r['close']), _num(r['volume'])))
        except Exception:
            pass
    out.sort(reverse=True)    # 新的在前
    return out

def _metrics(h):
    """日线 -> 成交量比、52 周高低、近期涨幅、连涨连跌、日波动幅度、跳空、60 日走势。h 是新的在前的 (日期, 开, 高, 低, 收, 量)。"""
    if len(h) < 25 or not h[0][4] or not h[1][4]: return None
    c = [x[4] for x in h]; hi = [x[2] for x in h]; lo = [x[3] for x in h]; vo = [x[5] for x in h]
    av = sum(vo[1:31]) / len(vo[1:31])
    m = {'av': int(av), 'rv': round(vo[0] / av, 2) if av else 0}
    # 价格、涨跌、成交量都用日线里最新一天的（纳斯达克的筛选器列表有时还停在前一天，和日线对不上）
    m['p'], m['c'], m['v'], m['dv'] = c[0], round((c[0] / c[1] - 1) * 100, 2), int(vo[0]), round(c[0] * vo[0] / 1e4)
    m['dt'] = h[0][0]
    n = min(len(h), 252)
    m['h'], m['l'] = round(max(hi[:n]), 3), round(min(lo[:n]), 3)
    if hi[0] >= max(hi[1:n]): m['nh'] = 1      # 今天创 52 周新高
    if lo[0] <= min(lo[1:n]): m['nl'] = 1      # 今天创 52 周新低
    m['r5'] = round((c[0] / c[5] - 1) * 100, 1)
    m['r20'] = round((c[0] / c[20] - 1) * 100, 1)
    up = c[0] > c[1]; s = 0
    for i in range(len(c) - 1):
        if (c[i] > c[i + 1]) if up else (c[i] < c[i + 1]): s += 1
        else: break
    m['st'] = s if up else -s
    tr = [max(hi[i] - lo[i], abs(hi[i] - c[i + 1]), abs(lo[i] - c[i + 1])) for i in range(14)]
    m['atr'] = round(sum(tr) / 14 / c[0] * 100, 1)
    m['gap'] = round((h[0][1] / c[1] - 1) * 100, 1)
    m['sp'] = [round(x * 100) for x in c[:60][::-1]]
    return m

def _short(sym):
    j = nq_json(f'https://api.nasdaq.com/api/quote/{sym}/short-interest?assetClass=stocks')
    t = ((j or {}).get('data') or {}).get('shortInterestTable', {}) or {}
    rows = t.get('rows') or []
    if not rows: return None
    r = rows[0]
    try:
        m, d, y = r['settlementDate'].split('/'); sd = f'{y}-{m}-{d}'
    except Exception:
        sd = ''
    return {'si': int(_num(r.get('interest'))), 'sd': sd, 'dtc': round(_num(r.get('daysToCover')), 1)}

def finra_short(day):
    """FINRA 每天公布的卖空成交量（占当天总成交量的百分比），取不到当天的就往前找几天。"""
    d = datetime.date.fromisoformat(day)
    for k in range(5):
        dd = d - datetime.timedelta(days=k)
        if dd.weekday() >= 5: continue
        try:
            req = urllib.request.Request(f'https://cdn.finra.org/equity/regsho/daily/CNMSshvol{dd:%Y%m%d}.txt', headers={'User-Agent': 'Mozilla/5.0'})
            t = urllib.request.urlopen(req, timeout=60).read().decode('utf-8', 'replace')
        except Exception:
            continue
        out = {}
        for line in t.splitlines()[1:]:
            p = line.split('|')
            if len(p) >= 5:
                try:
                    tv = float(p[4])
                    if tv > 0: out[p[1]] = round(float(p[2]) / tv * 100)
                except ValueError:
                    pass
        return dd.isoformat(), out
    return None, {}

def small_scan(day, earn=None):
    t0 = time.time()
    pool, total = small_pool()
    print(f'小盘池：全市场 {total} 只，符合条件 {len(pool)} 只', flush=True)

    def one(r):
        s = r['symbol']
        m = _metrics(_hist(s, day))
        if not m: return s, None
        p, mc = _num(r['lastsale']), _num(r['marketCap'])
        rec = {'n': NAME_TAIL.sub('', r['name']).strip()[:40], 'mc': round(mc / 1e8 * (m['p'] / p if p else 1), 2),
               'sec': SEC_CN.get(r.get('sector') or '', '未分类'), 'ind': (r.get('industry') or '')[:60],
               'cty': r.get('country') or '', 'ipo': int(_num(r.get('ipoyear')))}
        rec.update(m)
        sh = _short(s)
        if sh:
            rec.update(sh)
            if mc and p: rec['sip'] = round(sh['si'] / (mc / p) * 100, 1)    # 占总股本的百分比（用市值÷股价估算总股本）
        if earn and s in earn: rec['er'] = earn[s]
        return s, rec

    out, fail = {}, 0
    with ThreadPoolExecutor(6) as ex:
        fut = [ex.submit(one, r) for r in pool]
        for n, f in enumerate(as_completed(fut), 1):
            try:
                s, rec = f.result()
            except Exception:
                fail += 1; continue
            if rec: out[s] = rec
            else: fail += 1
            if n % 200 == 0: print(f'  小盘 {n}/{len(pool)}  {time.time() - t0:.0f}s', flush=True)
    # 数据日 = 大多数股票日线最新一天的日期；停牌等日期对不上的先去掉
    from collections import Counter
    dd = Counter(v['dt'] for v in out.values()).most_common(1)[0][0] if out else day
    stale = [s for s, v in out.items() if v['dt'] != dd]
    for s in stale: del out[s]
    low = [s for s, v in out.items() if v['dv'] < SMALL_RULE['dv_min'] / 1e4 or v['p'] < SMALL_RULE['price_min']]
    for s in low: del out[s]      # 名单是按前一天的数据圈的，这里再按最新一天的成交额和股价筛一遍
    print(f'按最新一天的成交额和股价再筛一遍，去掉 {len(low)} 只', flush=True)
    for v in out.values(): v.pop('dt', None)
    day = dd
    d0 = datetime.date.fromisoformat(day)
    fd, fin = finra_short(day)
    print(f'数据日 {day}，{len(stale)} 只日期对不上被去掉；FINRA 卖空成交量：{fd}，{len(fin)} 只', flush=True)
    for s, v in out.items():
        if fin.get(s) is not None: v['sv'] = fin[s]
    # 今天异动的（涨跌最多、成交量比最高、财报临近）顺便抓几条雅虎新闻
    L = list(out.items())
    pick = [k for k, v in sorted(L, key=lambda kv: -kv[1]['c'])[:12]] + [k for k, v in sorted(L, key=lambda kv: kv[1]['c'])[:12]] \
        + [k for k, v in sorted(L, key=lambda kv: -kv[1]['rv'])[:12]] \
        + [k for k, v in sorted(L, key=lambda kv: -kv[1]['dv']) if v.get('er') and 0 <= (datetime.date.fromisoformat(v['er'][0]) - d0).days <= 7][:15]
    nn = 0
    for s in dict.fromkeys(pick):
        try:
            h = headlines(s, 4)
            if h: out[s]['nw'] = h; nn += 1
        except Exception:
            pass
        time.sleep(0.4)
    json.dump({'d': day, 'n': len(out), 'rule': SMALL_RULE, 'finra': fd, 's': out},
              open(os.path.join(DATA, 'small.json'), 'w'), separators=(',', ':'), ensure_ascii=False)
    print(f'小盘池完成：{len(out)} 只（{fail} 只没取到日线），其中 {nn} 只带新闻，用时 {time.time() - t0:.0f}s', flush=True)
    try:
        small_profiles(list(out))
    except Exception as e:
        print('公司简介更新失败（不影响其他数据）：', e, flush=True)

# ---------- 盘前行情：每个交易日开盘前抓一次小盘池的盘前价和盘前成交量，存成 data/premarket.json ----------
def premarket_scan(force=False, limit=0):
    from zoneinfo import ZoneInfo
    now = datetime.datetime.now(ZoneInfo('America/New_York'))
    if not force and (now.weekday() >= 5 or now.hour * 60 + now.minute >= 9 * 60 + 40):
        print('不是交易日，或者已经开盘，不抓盘前数据', flush=True)
        return
    t0 = time.time()
    syms = sorted(json.load(open(os.path.join(DATA, 'small.json')))['s'])
    if limit: syms = syms[:limit]      # 测试用
    today = f'{now:%b} {now.day}, {now.year}'     # 例如 Nov 18, 2026，和纳斯达克写的“数据更新时间”对得上才算今天的

    def one(s):
        j = nq_json(f'https://api.nasdaq.com/api/quote/{s}/extended-trading?assetclass=stocks&markettype=pre')
        d = (j or {}).get('data') or {}
        if f'Data last updated {today}' not in ' '.join(d.get('lastUpdateInfo') or []): return s, None      # 不是今天的数据（盘前这句在第二行，盘后在第一行，所以整段找）
        rows = (d.get('infoTable') or {}).get('rows') or []
        if not rows: return s, None                                                  # 盘前还没有成交
        m = re.search(r'\$\s*([\d,]+\.?\d*)', rows[0].get('consolidated') or '')
        if not m: return s, None
        return s, [round(float(m.group(1).replace(',', '')), 4), int(_num(rows[0].get('volume')))]

    out = {}
    with ThreadPoolExecutor(6) as ex:
        fut = [ex.submit(one, s) for s in syms]
        for n, f in enumerate(as_completed(fut), 1):
            try:
                s, v = f.result()
            except Exception:
                continue
            if v: out[s] = v
            if n % 200 == 0: print(f'  盘前 {n}/{len(syms)}  {time.time() - t0:.0f}s', flush=True)
    if not out:       # 一只都没有（盘前还没开始、节假日等）就不写文件，网站上也就不显示盘前行情
        print('没有取到任何盘前成交，不更新', flush=True)
        return
    json.dump({'t': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), 'd': now.date().isoformat(), 's': out},
              open(os.path.join(DATA, 'premarket.json'), 'w'), separators=(',', ':'))
    print(f'盘前行情：{len(out)}/{len(syms)} 只有盘前成交，用时 {time.time() - t0:.0f}s', flush=True)

# ---------- 小盘股公司简介：纳斯达克的公司简介（英文），只抓新进名单的，存成 data/small_profiles.json ----------
def _prof_one(sym):
    j = nq_json(f'https://api.nasdaq.com/api/company/{sym}/company-profile')
    d = (j or {}).get('data') or {}
    v = lambda k: ((d.get(k) or {}).get('value') or '')
    desc = re.sub(r'\s+', ' ', v('CompanyDescription')).strip()
    if not desc: return None
    if len(desc) > 900:      # 太长的在句子结尾处截断
        cut = desc[:900]
        desc = cut[:cut.rfind('. ') + 1] if '. ' in cut[300:] else cut
    return {'de': desc, 'url': v('CompanyUrl')}

def small_profiles(syms):
    p = os.path.join(DATA, 'small_profiles.json')
    try:
        old = json.load(open(p)).get('p', {})
    except Exception:
        old = {}
    todo = [s for s in syms if s not in old]
    t0 = time.time()
    new, miss = {}, 0
    with ThreadPoolExecutor(6) as ex:
        fut = {ex.submit(_prof_one, s): s for s in todo}
        for n, f in enumerate(as_completed(fut), 1):
            try:
                r = f.result()
            except Exception:
                r = None
            if r: new[fut[f]] = r
            else: miss += 1
            if n % 200 == 0: print(f'  简介 {n}/{len(todo)}  {time.time() - t0:.0f}s', flush=True)
    keep = {s: v for s, v in old.items() if s in set(syms)}      # 掉出名单的不留，文件不会越来越大
    keep.update(new)
    json.dump({'d': datetime.date.today().isoformat(), 'p': keep}, open(p, 'w'), separators=(',', ':'), ensure_ascii=False)
    print(f'公司简介：新抓 {len(new)} 只（{miss} 只没有简介），共 {len(keep)} 只，用时 {time.time() - t0:.0f}s', flush=True)

# ---------- 单只标的 ----------
OPT = re.compile(r'^(.+?)(\d{6})([CP])(\d{8})$')

def third_friday(x):
    d = datetime.date(x.year, x.month, 15)
    return d + datetime.timedelta(days=(4 - d.weekday()) % 7)

def summarize(sym, today, name, earn=None):
    j = json.loads(get(f'https://cdn.cboe.com/api/global/delayed_quotes/options/{sym}.json'))
    d = j['data']
    root = name.replace('.', '')
    cv = pv = coi = poi = 0
    exps = {}
    px = d.get('current_price') or d.get('close') or 0
    for o in d.get('options', []):
        m = OPT.match(o['option'])
        if not m: continue
        r, ymd, cp, k = m.groups()
        if r != root and r != name and not (sym.startswith('_') and r.startswith(root)):  # 跳过调整合约；指数的 SPXW、NDXP 等算进来
            continue
        v, oi = o.get('volume') or 0, o.get('open_interest') or 0
        if cp == 'C': cv += v; coi += oi
        else: pv += v; poi += oi
        exps.setdefault(ymd, []).append((float(k) / 1000, cp, o.get('bid') or 0, o.get('ask') or 0, v, oi))
    key = lambda x: x.strftime('%y%m%d')
    dates = sorted(datetime.date(2000 + int(x[:2]), int(x[2:4]), int(x[4:])) for x in exps)
    dates = [x for x in dates if x >= today]
    near7 = [x for x in dates if (x - today).days <= 7]
    mid = lambda b, a: (a + b) / 2 if b > 0 and a > 0 else None

    def straddle(x):
        """到期日 x 最接近现价、Call 和 Put 都有报价的行权价：[到期日, 跨式价, 占现价 %]。"""
        if not px: return None
        legs = {}
        for k, cp, b, a, v, oi in exps[key(x)]:
            m_ = mid(b, a)
            if m_ is not None: legs.setdefault(k, {})[cp] = m_
        both = [k for k, z in legs.items() if 'C' in z and 'P' in z]
        if not both: return None
        k = min(both, key=lambda k: abs(k - px))
        s = legs[k]['C'] + legs[k]['P']
        return [x.isoformat(), round(s, 2), round(s / px * 100, 1)]

    # 流动性：到期 5 天以上最近一档、最接近现价的 Call 的买卖价差；同一个到期日算预期波动
    spread = em = None
    for x in dates:
        if (x - today).days < 5: continue
        legs = [l for l in exps[key(x)] if l[1] == 'C' and l[2] > 0 and l[3] > 0]
        if px and legs:
            k, cp, b, a = min(legs, key=lambda l: abs(l[0] - px))[:4]
            spread = round((a - b) / ((a + b) / 2) * 100, 1)
        em = straddle(x)
        break
    # 财报后第一个到期日的预期波动（盘后公布的要再往后一天）
    eem = None
    if earn:
        ed = datetime.date.fromisoformat(earn[0]) + datetime.timedelta(days=1 if earn[1] == 'post' else 0)
        after = [x for x in dates if x >= ed]
        if after: eem = straddle(after[0])
    # 关键行权价：45 天内（不含当天到期）现价上方未平仓最多的 Call、下方未平仓最多的 Put
    coik, poik = {}, {}
    for x in dates:
        if x <= today: continue
        if (x - today).days > 45: break
        for k, cp, b, a, v, oi in exps[key(x)]:
            if cp == 'C' and k >= px: coik[k] = coik.get(k, 0) + oi
            if cp == 'P' and k <= px: poik[k] = poik.get(k, 0) + oi
    walls = None
    if coik and poik:
        ck = max(coik, key=coik.get); pk = max(poik, key=poik.get)
        walls = [ck, int(coik[ck]), pk, int(poik[pk])]
    # 最大痛点：最近一个月度到期日（每月第三个周五）
    mp = None
    # 第三个周五；遇到节假日（如耶稣受难日）提前到周四
    monthly = [x for x in dates if x == third_friday(x) or (x.weekday() == 3 and x + datetime.timedelta(days=1) == third_friday(x) and third_friday(x) not in dates)]
    if monthly:
        rows = exps[key(monthly[0])]
        ks = sorted({l[0] for l in rows})
        if ks and sum(l[5] for l in rows) > 0:
            pain = lambda s: sum(oi * max(0, s - k) if cp == 'C' else oi * max(0, k - s) for k, cp, b, a, v, oi in rows)
            mp = [monthly[0].isoformat(), min(ks, key=pain)]
    # 异动：成交量超过未平仓量、且当天成交 ≥ 500 张的合约
    tk = key(today)  # 当天及已过期的合约不算（末日期权的成交天然大于未平仓）
    unu = [(v, ymd, cp, k, oi) for ymd, rows in exps.items() if ymd > tk for k, cp, b, a, v, oi in rows if v >= 500 and v > oi]
    top = None
    if unu:
        v, ymd, cp, k, oi = max(unu)
        top = ['20' + ymd[:2] + '-' + ymd[2:4] + '-' + ymd[4:], cp, k, int(v), int(oi)]
    return {
        'ts': j.get('timestamp'), 'last': d.get('last_trade_time'),
        'px': round(px, 2), 'chg': round(d.get('price_change_percent') or 0, 2),
        'iv': round(d.get('iv30') or 0, 1), 'ivc': round(d.get('iv30_change') or 0, 1),
        'cv': int(cv), 'pv': int(pv), 'coi': int(coi), 'poi': int(poi),
        'n7': len(near7), 'nexp': len(dates), 'next': dates[0].isoformat() if dates else None,
        'spr': spread, 'em': em, 'eem': eem, 'walls': walls, 'mp': mp, 'unu': top, 'nunu': len(unu),
    }

def main():
    limit = int(sys.argv[sys.argv.index('--limit') + 1]) if '--limit' in sys.argv else 0
    t0 = time.time()
    if '--premarket' in sys.argv:    # 只抓盘前行情（--force 不看时间，测试用）
        premarket_scan('--force' in sys.argv, limit)
        return
    if '--profiles-only' in sys.argv:    # 只更新小盘股公司简介（用现有的 small.json 名单）
        small_profiles(list(json.load(open(os.path.join(DATA, 'small.json')))['s']))
        return
    if '--small-only' in sys.argv:   # 只更新小盘池（用现有的数据日期），本地测试用
        day0 = json.load(open(os.path.join(DATA, 'options_latest.json')))['date']
        small_scan(day0, earnings())
        return
    site = json.load(open(os.path.join(DATA, 'data.json')))
    # 已退市、网站上已经去掉的公司不抓（和 build.py 保持一致）
    stocks = sorted(({r[0] for r in site['全部公司'][1:]} | {'SPCX'}) - {'EA', 'WBD', 'KVUE'})
    extra = json.load(open(os.path.join(DATA, 'etf_extra.json')))
    # 杠杆和反向 ETF 不要（类别是杠杆与反向，或跟踪说明里带“倍”）
    lev = lambda r: r[2].startswith('杠杆') or '倍' in r[3]
    etfs = sorted({r[0] for r in site['ETF'][1:] if not lev(r)} | {'DRAM'} | {r[0] for r in extra})
    idx = sorted({x for v in INDEX_OPT.values() for x in v})
    uni = [('_' + x, x, 2) for x in idx] + [(x, x, 1) for x in etfs] + [(x, x, 0) for x in stocks]
    if limit: uni = uni[:limit]
    print(f'指数 {len(idx)}，ETF {len(etfs)}，个股 {len(stocks)}，共 {len(uni)}', flush=True)
    today = datetime.date.today()
    earn = earnings()
    print(f'财报日历 {len(earn)} 只', flush=True)
    res, fail, nodata = {}, [], set()
    with ThreadPoolExecutor(4) as ex:
        fut = {ex.submit(summarize, q, today, s, earn.get(s)): (s, k) for q, s, k in uni}
        for n, f in enumerate(as_completed(fut), 1):
            s, kind = fut[f]
            try:
                r = f.result(); r['k'] = kind; r['name'] = ''; res[s] = r
            except Exception as e:
                fail.append(s)
                if getattr(e, 'code', 0) in (403, 404): nodata.add(s)
            if n % 200 == 0: print(f'  {n}/{len(uni)}  {time.time() - t0:.0f}s', flush=True)

    # 第一轮失败的（多半是 Cboe 临时限流），最后再一只一只慢慢重试一次
    if nodata:
        print(f'Cboe 没有这些标的的数据，不再重试：{" ".join(sorted(nodata))}', flush=True)
    retry = [s for s in fail if s not in nodata]
    if retry:
        print(f'重试 {len(retry)} 只：{" ".join(retry)}', flush=True)
        kinds = {s: k for q, s, k in uni}
        for s in retry:
            time.sleep(5)
            try:
                r = summarize(('_' + s) if kinds[s] == 2 else s, today, s, earn.get(s)); r['k'] = kinds[s]; r['name'] = ''
                res[s] = r; fail.remove(s)
            except Exception:
                pass

    ts = max((r['ts'] for r in res.values() if r.get('ts')), default=None)
    # 数据日 = 大多数标的最后一笔成交的日期（美东），不是 Cboe 文件生成时间（UTC）
    from collections import Counter
    lasts = [r['last'][:10] for r in res.values() if r.get('last')]
    day = Counter(lasts).most_common(1)[0][0] if lasts else (ts or today.isoformat())[:10]
    mcap, mcap_src = market_caps(stocks)
    # 新闻：7 天内有财报的 + 期权成交最多的 30 只 + 大盘
    d0 = datetime.date.fromisoformat(day)
    soon = [t for t, e in earn.items() if t in res and 0 <= (datetime.date.fromisoformat(e[0]) - d0).days <= 7]
    top = sorted(res, key=lambda t: -(res[t]['cv'] + res[t]['pv']))[:30]
    nw = news(list(dict.fromkeys(soon + top))) if not limit or '--news' in sys.argv else {}
    if nw:
        # 大盘新闻换成新浪的中文新闻；个股新闻把英文标题翻成中文（7 天内财报的优先），翻不了的保留英文
        try:
            cn = sina_us()
            if len(cn) >= 3: nw['_mkt'] = cn
        except Exception as e:
            print('新浪新闻取不到，大盘新闻用英文', e, flush=True)
        old = {}
        try:
            for L in json.load(open(os.path.join(DATA, 'prev_news.json'))).get('news', {}).values():
                for x in L:
                    if len(x) > 3 and x[3]: old[x[0]] = x[3]
        except Exception:
            pass
        order = [t for t in dict.fromkeys(soon + top) if t in nw]
        en = [x[0] for t in order for x in nw[t][:3]] + [x[0] for t in order for x in nw[t][3:]]
        zh = translate(en, old)
        for t, L in nw.items():
            if t == '_mkt' and L and re.search(r'[\u4e00-\u9fff]', L[0][0]): continue
            for x in L:
                if x[0] in zh: x.append(zh[x[0]])
        print(f'新闻 {len(nw)} 组，翻译 {len(zh)}/{len(en)} 条（复用 {len([t for t in en if t in old])} 条）', flush=True)
    else:
        print('新闻 0 组', flush=True)
    print(f'市值 {len(mcap)} 只（{mcap_src}）', flush=True)
    out = {'source': 'Cboe delayed quotes', 'fetched': datetime.datetime.now().isoformat(timespec='seconds'),
           'timestamp': ts, 'date': day, 'n': len(res), 'failed': fail, 'data': res,
           'mcap': mcap, 'mcap_source': mcap_src,
           'earn': {t: v for t, v in earn.items() if t in res}, 'news': nw}
    json.dump(out, open(os.path.join(DATA, 'options_latest.json'), 'w'), separators=(',', ':'))
    print(f'完成 {len(res)} 只，失败 {len(fail)} 只，数据时间 {ts}，用时 {time.time() - t0:.0f}s')
    if not limit:
        update_history(day, res)
    if not limit or '--intraday' in sys.argv:
        intraday(res, day)
    if not limit or '--small' in sys.argv:
        try:
            small_scan(day, earn)
        except Exception as e:      # 小盘池出问题不影响期权、新闻这些主要数据
            print('小盘池更新失败（不影响其他数据）：', e, flush=True)
    # --min N：成功少于 N 只就报错退出，不生成、不发布残缺的网页
    need = int(sys.argv[sys.argv.index('--min') + 1]) if '--min' in sys.argv else 0
    if len(res) < need:
        sys.exit(f'只抓到 {len(res)} 只，少于 {need} 只，这次不更新网页')

def update_history(day, res, keep=60):
    """每天记一笔期权成交量和 IV30，存在 data/hist.json，最多留 keep 个交易日，网页用来画走势小图。"""
    p = os.path.join(DATA, 'hist.json')
    try:
        h = json.load(open(p))
    except Exception:
        h = {'d': [], 'v': {}, 'i': {}}
    if day in h['d']:
        n = h['d'].index(day)
    else:
        h['d'].append(day); n = len(h['d']) - 1
        for k in ('v', 'i'):
            for t in h[k]: h[k][t].append(None)
    L = len(h['d'])
    for t, r in res.items():
        for k, val in (('v', r['cv'] + r['pv']), ('i', r['iv'] or None)):
            arr = h[k].setdefault(t, [None] * L)
            arr += [None] * (L - len(arr))
            arr[n] = val
    order = sorted(range(L), key=lambda i: h['d'][i])[-keep:]
    h['d'] = [h['d'][i] for i in order]
    for k in ('v', 'i'):
        h[k] = {t: [a[i] for i in order] for t, a in h[k].items()}
    json.dump(h, open(p, 'w'), separators=(',', ':'))
    print(f'历史记录 {len(h["d"])} 天', flush=True)

if __name__ == '__main__':
    main()
