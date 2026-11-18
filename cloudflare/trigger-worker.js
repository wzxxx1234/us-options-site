// Cloudflare Worker：每个美股交易日收盘后，通知 GitHub 开始更新网站数据。
// 用 Cron 触发（北京时间周二到周六早上 5:50，即 UTC 周一到周五 21:50）。
// GitHub 访问令牌放在 Worker 的保密变量 GH_TOKEN 里，不写在代码里。
// mode=auto：网站上的数据如果 10 小时内已经更新过（比如 GitHub 自己的定时先跑了），这次就自动跳过。
const REPO = 'wzxxx1234/us-options-site';
const WORKFLOW = 'update.yml';

async function trigger(env) {
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
      body: JSON.stringify({ ref: 'main', inputs: { mode: 'auto' } }),
    });
    if (r.status === 204) { console.log('已通知 GitHub 开始更新'); return; }
    const t = await r.text();
    console.log(`第 ${i + 1} 次失败：${r.status} ${t.slice(0, 200)}`);
    if ([401, 403, 404, 422].includes(r.status)) break; // 令牌不对、过期或权限不够，重试没用
    await new Promise((res) => setTimeout(res, 5000 * (i + 1)));
  }
  throw new Error('通知 GitHub 失败：令牌可能过期，或权限不对');
}

export default {
  async scheduled(event, env) { await trigger(env); },
  async fetch() { return new Response('ok'); }, // 不对外开放触发，避免被别人乱用
};
