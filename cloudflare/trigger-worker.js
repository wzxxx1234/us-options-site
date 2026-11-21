// Cloudflare Worker：定时通知 GitHub 开始更新网站数据；另外提供一个转发 Cboe 请求的入口。
//
// 两个 Cron 触发（星期几一定要写 MON-FRI，不要写 1-5：Cloudflare 的数字是 1-7 且 1 是周日，1-5 会变成周日到周四，漏掉周五）：
//   50 21 * * MON-FRI  收盘后的完整更新（北京时间周二到周六早上 5:50）  -> mode=auto
//   0 13 * * MON-FRI   盘前更新（美东 8:00 或 9:00，北京时间晚上 21:00）-> mode=pre
// GitHub 访问令牌放在保密变量 GH_TOKEN 里，不写在代码里。
// mode=auto：网站上的数据如果 10 小时内已经更新过，这次就自动跳过。
//
// /cboe?sym=AAPL&kind=options|quotes|intraday：转发 Cboe 的延时行情。Cboe 现在拦截机房 IP（GitHub 的服务器、VPS 都被拦），
// 经 Cloudflare 转发可能能通。设了保密变量 PROXY_KEY 后，必须带请求头 x-key 才能用；没设之前只允许查 SPY（用来测试）。
const REPO = 'wzxxx1234/us-options-site';
const WORKFLOW = 'update.yml';

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

async function cboe(request, env, url) {
  const sym = (url.searchParams.get('sym') || '').toUpperCase();
  const kind = url.searchParams.get('kind') || 'options';
  if (!/^_?[A-Z0-9.\-]{1,10}$/.test(sym) || !['options', 'quotes', 'intraday'].includes(kind)) return new Response('bad request', { status: 400 });
  if (env.PROXY_KEY) {
    if (request.headers.get('x-key') !== env.PROXY_KEY) return new Response('forbidden', { status: 403 });
  } else if (sym !== 'SPY') {
    return new Response('PROXY_KEY 还没设置，现在只能测试 SPY', { status: 403 });
  }
  const path = kind === 'intraday' ? `charts/intraday/${sym}.json` : kind === 'quotes' ? `quotes/${sym}.json` : `options/${sym}.json`;
  const r = await fetch(`https://cdn-api.cboe.com/api/global/delayed_quotes/${path}`, {
    headers: { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', Accept: 'application/json' },
  });
  return new Response(r.body, { status: r.status, headers: { 'content-type': r.headers.get('content-type') || 'application/json', 'x-upstream-status': String(r.status) } });
}

export default {
  // 用开头的时间判断是哪个定时，不要比较整句，改星期写法时不会出错
  async scheduled(event, env) { await trigger(env, /^0 13 /.test(event.cron) ? 'pre' : 'auto'); },
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/cboe') return cboe(request, env, url);
    return new Response('ok'); // 其他一律不对外开放触发，避免被别人乱用
  },
};
