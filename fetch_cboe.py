"""从 Cboe 公开的延时行情接口抓网站上个股、ETF、指数的期权数据，生成 data/options_latest.json。

用法：python3 fetch_cboe.py [--limit N]
数据来源：
  - 可交易期权名单  https://www.cboe.com/us/options/symboldir/?download=csv
  - 周度期权名单    https://www.cboe.com/available_weeklys/get_csv_download/
  - 每只标的期权链  https://cdn.cboe.com/api/global/delayed_quotes/options/<代码>.json
这些接口 Cboe 没有正式公开说明，格式可能随时变化。
"""
import csv, io, json, os, re, sys, time, datetime, urllib.request
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
            if k == tries - 1:
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

# ---------- 单只标的 ----------
OPT = re.compile(r'^(.+?)(\d{6})([CP])(\d{8})$')

def summarize(sym, today, name):
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
        exps.setdefault(ymd, []).append((float(k) / 1000, cp, o.get('bid') or 0, o.get('ask') or 0))
    dates = sorted(datetime.date(2000 + int(x[:2]), int(x[2:4]), int(x[4:])) for x in exps)
    dates = [x for x in dates if x >= today]
    near7 = [x for x in dates if (x - today).days <= 7]
    # 流动性：到期 5 天以上最近一档、最接近现价的 Call 的买卖价差
    spread = None
    for x in dates:
        if (x - today).days < 5: continue
        legs = [l for l in exps[x.strftime('%y%m%d')] if l[1] == 'C' and l[2] > 0 and l[3] > 0]
        if px and legs:
            k, cp, b, a = min(legs, key=lambda l: abs(l[0] - px))
            spread = round((a - b) / ((a + b) / 2) * 100, 1)
        break
    return {
        'ts': j.get('timestamp'), 'last': d.get('last_trade_time'),
        'px': round(px, 2), 'chg': round(d.get('price_change_percent') or 0, 2),
        'iv': round(d.get('iv30') or 0, 1), 'ivc': round(d.get('iv30_change') or 0, 1),
        'cv': int(cv), 'pv': int(pv), 'coi': int(coi), 'poi': int(poi),
        'n7': len(near7), 'nexp': len(dates), 'next': dates[0].isoformat() if dates else None,
        'spr': spread,
    }

def main():
    limit = int(sys.argv[sys.argv.index('--limit') + 1]) if '--limit' in sys.argv else 0
    t0 = time.time()
    site = json.load(open(os.path.join(DATA, 'data.json')))
    stocks = sorted({r[0] for r in site['全部公司'][1:]} | {'SPCX'})
    extra = json.load(open(os.path.join(DATA, 'etf_extra.json')))
    # 杠杆和反向 ETF 不要（类别是杠杆与反向，或跟踪说明里带“倍”）
    lev = lambda r: r[2].startswith('杠杆') or '倍' in r[3]
    etfs = sorted({r[0] for r in site['ETF'][1:] if not lev(r)} | {'DRAM'} | {r[0] for r in extra})
    idx = sorted({x for v in INDEX_OPT.values() for x in v})
    uni = [('_' + x, x, 2) for x in idx] + [(x, x, 1) for x in etfs] + [(x, x, 0) for x in stocks]
    if limit: uni = uni[:limit]
    print(f'指数 {len(idx)}，ETF {len(etfs)}，个股 {len(stocks)}，共 {len(uni)}', flush=True)
    today = datetime.date.today()
    res, fail = {}, []
    with ThreadPoolExecutor(4) as ex:
        fut = {ex.submit(summarize, q, today, s): (s, k) for q, s, k in uni}
        for n, f in enumerate(as_completed(fut), 1):
            s, kind = fut[f]
            try:
                r = f.result(); r['k'] = kind; r['name'] = ''; res[s] = r
            except Exception as e:
                fail.append(s)
            if n % 200 == 0: print(f'  {n}/{len(uni)}  {time.time() - t0:.0f}s', flush=True)

    ts = max((r['ts'] for r in res.values() if r.get('ts')), default=None)
    day = (ts or today.isoformat())[:10]
    out = {'source': 'Cboe delayed quotes', 'fetched': datetime.datetime.now().isoformat(timespec='seconds'),
           'timestamp': ts, 'date': day, 'n': len(res), 'failed': fail, 'data': res}
    json.dump(out, open(os.path.join(DATA, 'options_latest.json'), 'w'), separators=(',', ':'))
    print(f'完成 {len(res)} 只，失败 {len(fail)} 只，数据时间 {ts}，用时 {time.time() - t0:.0f}s')
    # --min N：成功少于 N 只就报错退出，不生成、不发布残缺的网页
    need = int(sys.argv[sys.argv.index('--min') + 1]) if '--min' in sys.argv else 0
    if len(res) < need:
        sys.exit(f'只抓到 {len(res)} 只，少于 {need} 只，这次不更新网页')

if __name__ == '__main__':
    main()
