import json,sys,os,re,csv,datetime
HERE=os.path.dirname(os.path.abspath(__file__));os.chdir(HERE)
d=json.load(open('data/data.json'))
SECT=['信息技术','金融','医疗保健','可选消费','通信服务','工业','必需消费','能源','公用事业','房地产','材料']
rows=[r for r in d['全部公司'][1:]]
by={r[0]:r for r in rows}
flags={r[0]:(1 if r[4] else 0)|(2 if r[5] else 0)|(4 if r[6] else 0) for r in rows}
# ---- corrections verified 2026-11-12
for t in ['ALAB','CRWV','HONA','LITE','MRNA','NBIS','RKLB','SNDK','TER']: flags[t]|=2
for t in ['CHTR','CSGP','CTSH','INSM','KHC','TEAM','VRSK','ZS']: flags[t]&=~2
flags['GOOGL']|=4; flags['VZ']&=~4
flags['OKTA']|=1
for t in ['EA','WBD','KVUE']:
    rows.remove(by[t]); del flags[t]
by['KMB'][9]='做纸尿裤（好奇）、纸巾（舒洁）、女性护理（高洁丝）。2026 年 11 月完成收购科赴 Kenvue，新增泰诺、邦迪、李施德林、露得清等消费健康品牌。'
by['KHC'][9]='亨氏番茄酱、卡夫奶酪、Oscar Mayer 肉制品等，伯克希尔是大股东。拆分为酱料和北美杂货两家公司的计划已于 2026 年 2 月暂停；2026 年 9 月从纳斯达克转到纽交所上市，因此退出 NASDAQ-100。'
by['CHTR'][9]='美国最大的有线运营商，品牌 Spectrum，卖宽带、有线电视和移动套餐。2026 年 8 月完成与 Cox 的合并，母公司将在一年内更名为 Cox Communications。'
by['GOOGL'][9]+='2026 年 6 月取代威瑞森进入道琼斯30。'
by['VZ'][9]+='2026 年 6 月被 Alphabet 取代，退出道琼斯30。'
by['MSTR'][10]='公司已更名为 Strategy，代码仍为 MSTR'
spcx=['SPCX','SpaceX','Space Exploration Technologies','','','✓','','工业','航天与卫星互联网','马斯克创办的航天公司：猎鹰 9 号可回收火箭占全球商业发射的大部分，星链 Starlink 卫星互联网是主要收入来源，正在研发重型火箭星舰 Starship，2026 年初并入 xAI。2026 年 6 月在纳斯达克上市，7 月按新股快速纳入规则进入 NASDAQ-100。','2026 年 6 月上市']
i=rows.index(by['RKLB']) ; rows.insert(i,spcx); flags['SPCX']=2
C=[[r[0],r[1],r[2],flags[r[0]],SECT.index(r[7]),r[8],r[9],r[10] or ''] for r in rows]
from collections import Counter
print('total',len(C),'sp',sum(1 for c in C if c[3]&1),'ndx',sum(1 for c in C if c[3]&2),'dow',sum(1 for c in C if c[3]&4),'none',sum(1 for c in C if not c[3]))
I=[r for r in d['指数'][1:]]
for r in I:
    if r[2]=='NDX': r[8]='在纳斯达克上市的最大100家非金融公司，科技股占比过半，波动大于标普500。2026 年起改为每季度调整成分股，大型新股上市满 15 个交易日即可快速纳入。'
    if r[2]=='RUT': r[8]=r[8].replace('每年6月重新编制。','2026 年起改为每年 6 月和 11 月各重新编制一次。')
    if r[2]=='DJI': r[8]+='最近一次调整是 2026 年 6 月 Alphabet 取代威瑞森。'
E=[r for r in d['ETF'][1:]]
i=[r[0] for r in E].index('SOXX')+1
if 'DRAM' not in [r[0] for r in E]: E.insert(i,['DRAM','Roundhill Memory ETF','细分行业与主题','存储芯片','集中持有存储芯片公司（DRAM、NAND、HBM），比 SMH 更押注存储周期。'])

# ---------- 2026-11-14 补入：Macroption 2026 年 ETF 期权日均成交前 100 名里网站原来没有的行业、主题、商品、加密 ETF（杠杆和反向的不收） ----------
EXTRA=json.load(open('data/etf_extra.json'))
_have={r[0] for r in E}
E+=[r for r in EXTRA if r[0] not in _have]

# ---------- 期权数据：Cboe 延时数据 ----------
L=json.load(open('data/options_latest.json')); Q=L['data']
# 数据日按大多数标的最后一笔成交的日期（美东）算，不用 Cboe 文件的生成时间（UTC，收盘后会跨到第二天）
from collections import Counter
_lasts=[r['last'][:19] for r in Q.values() if r.get('last')]
OD=Counter(x[:10] for x in _lasts).most_common(1)[0][0] if _lasts else L['date']
OTS=max([x for x in _lasts if x.startswith(OD) and x[11:19]<='16:15:59'] or [L['timestamp'] or '']).replace('T',' ')  # 盘后零星成交不算
O={}
for t,r in Q.items():
    vol=r['cv']+r['pv']; oi=r['coi']+r['poi']
    if not vol and not oi: continue
    ivr=ivp=None; src=0   # IV Rank 已按用户要求去掉，位置留空
    sd=(r.get('last') or r.get('ts') or '')[:10]
    O[t]=[vol,ivr,ivp,round(r['pv']/r['cv'],2) if r['cv'] else None,round(r['pv']/vol*100,1) if vol else 0,r['px'],r['chg'],r['name'],r['k'],0,r['iv'],oi,r['n7'],r['spr'],sd if sd and sd<OD else 0,src]
    # 16 财报 [日期, pre/post]  17 预期波动 [到期日, 跨式价, %]  18 财报后预期波动  19 关键行权价 [Call 价, OI, Put 价, OI]
    # 20 最大痛点 [到期日, 价]  21 最大异动合约 [到期日, C/P, 行权价, 成交, 未平仓]  22 异动合约数
    O[t]+=[(L.get('earn') or {}).get(t) or 0,r.get('em') or 0,r.get('eem') or 0,r.get('walls') or 0,r.get('mp') or 0,r.get('unu') or 0,r.get('nunu') or 0]
for k in (0,1,2):
    for n,t in enumerate(sorted([t for t,o in O.items() if o[8]==k and o[0]>0],key=lambda t:-O[t][0]),1): O[t][9]=n
print('options rows',len(O),'date',OD,'ts',OTS)
# ---------- 市值（亿美元）：优先用这次抓数据时取到的，没有就用仓库里的 data/mcap.json ----------
try: _mc_file=json.load(open('data/mcap.json'))['data']
except Exception: _mc_file={}
MC=dict(_mc_file); MC.update(L.get('mcap') or {})
for c in C: c.append(MC.get(c[0]))
print('mcap',sum(1 for c in C if c[8]),'of',len(C),'source',L.get('mcap_source') or 'data/mcap.json')
# ---------- 补齐全部有期权的 ETF ----------
def cat(n):
    u=n.upper()
    if re.search(r'\bVIX\b|VOLATIL',u): return '波动率'
    if re.search(r'COVERED CALL|OPTION INCOME|PREMIUM INCOME|BUY-?WRITE|WEEKLYPAY|YIELDMAX|OPTION STRAT|INCOME STRAT|DISTRIBUTION|BUFFER|DEFINED|\b0DTE\b|ENHANCED INCOME|\bCC\b',u): return '期权收益与缓冲'
    if re.search(r'\b(-?\d(\.\d)?X|ULTRA|ULTRAPRO|ULTRASHORT|ULTR|ULT|BULL|BEAR|SHORT|INVERSE|LEVERAGE[D]?|DAILY TARGET|DAIL)\b',u): return '杠杆与反向'
    if re.search(r'BITCOIN|ETHER|CRYPTO|SOLANA|\bXRP\b|BLOCKCHAIN|\bBTC\b|\bETH\b|DOGE|LITECOIN|DIGITAL ASSET',u): return '加密货币'
    if re.search(r'GOLD|SILVER|\bOIL\b|NATURAL GAS|NAT GAS|GASOLINE|COMMOD|COPPER|PLATINUM|PALLADIUM|URANIUM|AGRICULT|WHEAT|CORN|SOYBEAN|BRENT|PRECIOUS|METALS',u): return '商品'
    if re.search(r'TREAS|BOND|\bBD\b|\bTIPS\b|HIGH (YIELD|YLD)|MUNI|AGGREGATE|AGG\b|FLOATING|T-?BILL|CREDIT|MORTGAGE|INV(ESTMENT)? GRADE|IBOXX|CORP BD|FIXED INC|ULTRASHORT INCOME|MBS|CLO\b',u): return '债券与现金'
    if re.search(r'MSCI|\bFTSE\b|CHINA|JAPAN|BRAZIL|INDIA|EMERG|\bEMG\b|\bEM\b|EAFE|EUROPE|KOREA|TAIWAN|MEXICO|GERMANY|CANADA|AUSTRALIA|LATIN|ISRAEL|SAUDI|VIETNAM|\bINTL\b|INTERNATIONAL|DEVELOPED|ASIA|HONG KONG|AFRICA|TURKEY|POLAND|ARGENTINA|\bCSI\b|EX[- ]US|WORLD|GLOBAL',u): return '海外市场'
    if re.search(r'S&P ?500|SP500|NASDAQ|RUSSELL|DOW JONES|TOTAL (STOCK|MKT|MARKET)|MID ?CAP|SMALL ?CAP|LARGE ?CAP|\b500\b|1000|2000|BROAD',u): return '宽基指数'
    if re.search(r'DIVIDEND|\bDIV\b|VALUE|GROWTH|MOMENTUM|QUALITY|LOW VOL|MIN VOL|EQUAL WEIGHT|EQL WT|FACTOR|YIELD',u): return '风格与高股息'
    return '细分行业与主题'
def track(n):
    u=n.upper(); m=re.search(r'(-?\d(?:\.\d)?)X\b',u)
    x=abs(float(m.group(1))) if m else 3 if 'ULTRAPRO' in u else 2 if re.search(r'\bULTRA|\bULTR\b|\bULT\b',u) else 0
    short=bool(re.search(r'SHORT|BEAR|INVERSE|-\d(\.\d)?X|\bBR\b',u))
    if not x and not short: return ''
    x=x or 1
    return {1:'一倍',1.5:'1.5 倍',2:'两倍',3:'三倍'}.get(x,f'{x:g} 倍')+('做空' if short else '做多')
EC=[r[0] for r in E if r[4]]
print('etf total',len(E))
for r in E: r[2]=r[2].replace('（只适合短线，长期持有有损耗）','').replace('（标普500 十一大板块）','')
# 2026-11-14 按用户要求去掉全部杠杆和反向 ETF（含 UVXY、SVXY）
is_lev=lambda r:r[2].startswith('杠杆') or '倍' in r[3]
DROPPED=[r[0] for r in E if is_lev(r)];E=[r for r in E if not is_lev(r)]
for t in DROPPED: O.pop(t,None)
for n,t in enumerate(sorted([t for t,o in O.items() if o[8]==1 and o[0]>0],key=lambda t:-O[t][0]),1): O[t][9]=n
print('dropped leveraged',len(DROPPED),' '.join(DROPPED))
data=json.dumps({'S':SECT,'C':C,'I':I,'E':E,'O':O,'EC':EC,'OD':{'date':OD,'ts':OTS,'n':len(O)},'N':L.get('news') or {}},ensure_ascii=False,separators=(',',':')).replace('</','<\\/')
t=open('template.html').read().replace('/*__DATA__*/',data).replace('（Cboe 时间戳）','（美东时间，最后一笔成交）')
os.makedirs('dist',exist_ok=True)
open('dist/美股选股手册.html','w').write(t)
open('../index.html','w').write('<!doctype html>\n<html lang="zh-CN">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n<style>body{margin:0}[hidden]{display:none!important}img{max-width:100%}</style>\n</head>\n<body>\n'+t+'\n</body>\n</html>\n')
print('bytes',len(t.encode()))
