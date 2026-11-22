// Cloudflare Worker：① 定时通知 GitHub 开始更新网站数据；② 给网页代取实时新闻（带几分钟缓存）。
//
// 两个 Cron 触发（星期几一定要写 MON-FRI，不要写 1-5：Cloudflare 的数字是 1-7 且 1 是周日，1-5 会变成周日到周四，漏掉周五）：
//   50 21 * * MON-FRI  收盘后的完整更新（北京时间周二到周六早上 5:50）  -> mode=auto
//   0 13 * * MON-FRI   盘前更新（美东 8:00 或 9:00，北京时间晚上 21:00）-> mode=pre
// GitHub 访问令牌放在保密变量 GH_TOKEN 里，不写在代码里。
// mode=auto：网站上的数据如果 10 小时内已经更新过，这次就自动跳过。
//
// 新闻入口（只给自己的网站用，别的网站的浏览器读不到）：
//   /news?s=AAPL  雅虎财经这只股票最新的英文新闻标题（缓存 5 分钟）
//   /mkt          新浪财经 7×24 国际快讯（缓存 1 分钟）
// 返回格式和网站里每天抓的一样：[[标题, 链接, UTC 时间 "YYYY-MM-DDTHH:MMZ"], ...]
const REPO = 'wzxxx1234/us-options-site';
const WORKFLOW = 'update.yml';
const SITE = 'https://stock.wzxxx123456.top';
const ALLOW = /^(https:\/\/stock\.wzxxx123456\.top|https:\/\/wzxxx1234\.github\.io|http:\/\/(127\.0\.0\.1|localhost)(:\d+)?)$/;
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36';

async function trigger(env, mode) {
  if (!env.GH_TOKEN) throw new Error('没有设置保密变量 GH_TOKEN');
  for (let i = 0; i < 3; i++) {
    const r = await fetch(`https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/dispatches`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${env.GH_TOKEN}`,
        Accept: 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28',
        'User-Agent': 'us-options-site-trigger',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ ref: 'main', inputs: { mode } }),
    });
    if (r.status === 204) { console.log(`已通知 GitHub 开始更新（${mode}）`); return; }
    const t = await r.text();
    console.log(`第 ${i + 1} 次失败：${r.status} ${t.slice(0, 200)}`);
    if ([401, 403, 404, 422].includes(r.status)) break; // 令牌不对、过期或权限不够，重试没用
    await new Promise((res) => setTimeout(res, 5000 * (i + 1)));
  }
  throw new Error('通知 GitHub 失败：令牌可能过期，或权限不对');
}

const dec = (s) => s.replace(/<!\[CDATA\[|\]\]>/g, '').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
  .replace(/&quot;/g, '"').replace(/&#0?39;|&apos;/g, "'").replace(/&#(\d+);/g, (m, n) => String.fromCharCode(+n)).trim();
const iso = (d) => { const t = new Date(d); return isNaN(t) ? '' : t.toISOString().slice(0, 16) + 'Z'; };

async function yahooNews(sym) {
  const r = await fetch(`https://feeds.finance.yahoo.com/rss/2.0/headline?s=${encodeURIComponent(sym)}&region=US&lang=en-US`, { headers: { 'User-Agent': UA } });
  if (!r.ok) throw new Error('雅虎 ' + r.status);
  const x = await r.text(), out = [];
  for (const m of x.matchAll(/<item>([\s\S]*?)<\/item>/g)) {
    const g = (tag) => { const k = m[1].match(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`)); return k ? dec(k[1]) : ''; };
    const ti = g('title'), l = g('link');
    if (ti && l.startsWith('https://')) out.push([ti.slice(0, 160), l, iso(g('pubDate'))]);
    if (out.length >= 6) break;
  }
  return out;
}

async function sinaFlash() {
  const r = await fetch('https://zhibo.sina.com.cn/api/zhibo/feed?page=1&page_size=20&zhibo_id=152&tag_id=102&dire=f&dpc=1&type=0',
    { headers: { 'User-Agent': UA, Referer: 'https://finance.sina.com.cn/' } });
  if (!r.ok) throw new Error('新浪 ' + r.status);
  const j = await r.json();
  return ((((j.result || {}).data || {}).feed || {}).list || []).map((x) => [
    String(x.rich_text || '').replace(/<[^>]+>/g, '').trim().slice(0, 160),
    /^https:\/\//.test(x.docurl || '') ? x.docurl : 'https://finance.sina.com.cn/7x24/',
    iso(String(x.create_time || '').replace(' ', 'T') + '+08:00'),   // 新浪给的是北京时间
  ]).filter((x) => x[0]);
}

// 同一个请求几分钟内只真正去取一次（Cloudflare 边缘缓存）
async function cached(url, ttl, make, ctx) {
  const cache = caches.default, key = new Request(url);
  const hit = await cache.match(key);
  if (hit) return hit.text();
  const body = JSON.stringify(await make());
  ctx.waitUntil(cache.put(key, new Response(body, { headers: { 'Content-Type': 'application/json', 'Cache-Control': `public, max-age=${ttl}` } })));
  return body;
}

export default {
  // 用开头的时间判断是哪个定时，不要比较整句，改星期写法时不会出错
  async scheduled(event, env) { await trigger(env, /^0 13 /.test(event.cron) ? 'pre' : 'auto'); },
  // 只开放两个只读的新闻入口；不对外开放触发更新，避免被别人乱用
  async fetch(req, env, ctx) {
    const u = new URL(req.url), o = req.headers.get('Origin') || '';
    const h = { 'Access-Control-Allow-Origin': ALLOW.test(o) ? o : SITE, Vary: 'Origin', 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' };
    if (req.method === 'OPTIONS') return new Response(null, { headers: { ...h, 'Access-Control-Allow-Methods': 'GET' } });
    try {
      if (u.pathname === '/news') {
        const s = (u.searchParams.get('s') || '').toUpperCase().replace('.', '-');
        if (!/^[A-Z][A-Z0-9-]{0,9}$/.test(s)) return new Response('[]', { status: 400, headers: h });
        return new Response(await cached(`https://cache.local/news/${s}`, 300, () => yahooNews(s), ctx), { headers: h });
      }
      if (u.pathname === '/mkt') return new Response(await cached('https://cache.local/mkt', 60, sinaFlash, ctx), { headers: h });
    } catch (e) {
      return new Response(JSON.stringify({ error: String(e.message || e) }), { status: 502, headers: h });
    }
    return new Response('ok');
  },
};
