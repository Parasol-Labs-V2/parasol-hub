require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const MD_DASHBOARD_URL  = 'https://messagedesk-dashboard.vercel.app';
const MD_NINE_URL       = 'https://messagedesk-dashboard-nine.vercel.app'; // has /api/activity
const MD_INSTANTLY_KEY  = process.env.MD_INSTANTLY_KEY  || 'OTNhZDdjZjgtMDU4Yi00ZDgyLThjMTEtZWExZjY4NjE3YmM3OndYdUl2T0JZeGFaag==';
const MD_HEYREACH_KEY   = process.env.MD_HEYREACH_KEY   || 'jLny4sT0Hm8mw0K9Or68xWtIR0zx1msornUUW3uiBp8=';
const MD_FATHOM_KEY     = process.env.MD_FATHOM_KEY     || 'jHABMx60Sb4MjTLPNnJ9zw.FNKRJcbRIYxKjf8PWuAXOvcOjjdJQ2tM32ijTOjAecI';
const ATTIO_KEY         = process.env.ATTIO_KEY         || '1cfe31396fa6b1f36f90691f320ccdd28575e23bac77e8f9f772f3f8cc77c152';
const TB_INSTANTLY_KEY  = process.env.TB_INSTANTLY_KEY  || 'YzhkNzU1ODUtYjFlMS00YWQzLTlhYjMtMGVlNGMxNDdhNWIzOkVIbGRydmFsckZnWg==';
const TB_ECOMM_CAMP_ID  = '170a61eb-446f-4d5a-9916-5311d58ecf50';

// Notion CRM snapshot — updated 2026-06-11 from individual deal pages
// Names are safe for dashboard; FedEx/LAD are tracked separately as no-name-in-outbound
const TB_HEYREACH_KEY = 'k4yQ8ha0LaKZV1xQpyrRS9JbJt71IH0qpfjJQrP2iMw=';
// Notion CRM snapshot — Live Deals (collection 1bbfb75b). Refresh from Notion as deals update.
const TB_CRM_DEALS = [
  { name: 'HealNow', stage: 'Closed Won', owner: 'Shilpan', vertical: 'Other',
    arr: null, close_date: null, next_step_date: null,
    note: 'Live: 50+ AML rules, >$1.5B net payments monitored. Expanding to ops dashboards + embedded analytics.' },
  { name: 'Swiggy Instamart', stage: 'Mutual Evaluation', owner: 'Shilpan', vertical: 'FMCG',
    arr: 100000, close_date: '2026-07-18', next_step_date: '2026-06-17',
    note: 'Rides on freshness/self-maintenance demo. POC agreed; meeting tentatively 6/17.' },
  { name: 'Truly Free Home', stage: 'Mutual Evaluation', owner: 'Shilpan', vertical: 'Retail',
    arr: 100000, close_date: '2026-07-31', next_step_date: null,
    note: 'Pre-recorded e-comm demo + Chad-bridge pre-read ahead of exec offsite. Win CDAO Chad Buckendahl (skeptic).' },
  { name: 'HRPL / McDonald\'s of India', stage: 'Negotiation', owner: 'Jainit', vertical: 'FMCG',
    arr: 5000, close_date: null, next_step_date: '2026-06-09',
    note: '$5K setup + ~$100/restaurant/mo across 500+ on hitting thresholds. Gated on data readiness.' },
  { name: 'FedEx (Flight Safety)', stage: 'Proposal', owner: 'Shilpan', vertical: 'Other',
    arr: null, close_date: null, next_step_date: null,
    note: 'Pilot proposal A/B/C sent (Option B, ~8 wks). VP-of-Safety meeting target June. Flight-safety intel layer.' },
  { name: 'LAD NYC Solutions', stage: 'Proposal', owner: 'Blair', vertical: 'Channel',
    arr: 4200, close_date: null, next_step_date: null,
    note: 'Channel: $250/mo base + $100/mo upload add-on, 10% recurring commission to LAD. 2-3 restaurant pilot.' },
  { name: 'Toast', stage: 'Solution Exploration', owner: 'Shilpan', vertical: 'Retail',
    arr: null, close_date: null, next_step_date: null,
    note: 'Dual motion: internal analytics (Kristine) + embed partnership (Craig). Gated on data readiness.' },
  { name: 'Haldiram\'s', stage: 'Solution Exploration', owner: 'Shilpan', vertical: 'FMCG',
    arr: null, close_date: null, next_step_date: null,
    note: '9-dim data-readiness diagnostic built. Meeting postponed; gated on SKU dedup cleanup.' },
];
// Advisor Calls snapshot — updated 2026-06-15 from Notion (collection 42208258)
// Wassim Karawani (Shakepay) completed 6/15: graduating per Slack; Notion status still "Follow-up"
const TB_ADVISOR_CALLS = [
  { call_date: '2026-05-28', status: 'Scheduled',            graduating: false },
  { call_date: '2026-05-28', status: 'Scheduled',            graduating: false },
  { call_date: '2026-05-28', status: 'Peer / Non-commercial', graduating: false },
  { call_date: '2026-05-28', status: 'Scheduled',            graduating: false },
  { call_date: '2026-05-29', status: 'Peer / Non-commercial', graduating: false },
  { call_date: '2026-06-06', status: 'Scheduled',            graduating: false },
  { call_date: '2026-06-08', status: 'Scheduled',            graduating: false },
  { call_date: '2026-06-15', status: 'Follow-up',            graduating: false },
];
// Provenance for the Terrabase Tier-B snapshots above (last manual sync from Notion).
// /api/brief surfaces these as the real freshness, since the endpoint updated_at only
// reflects when the code last ran — NOT when the underlying CRM data was edited.
const TB_CRM_SNAPSHOT_DATE     = '2026-06-11'; // TB_CRM_DEALS ← Notion Live Deals (collection 1bbfb75b)
const TB_ADVISOR_SNAPSHOT_DATE = '2026-06-15'; // TB_ADVISOR_CALLS ← Notion advisor tracker (collection 42208258)

// Joe Carbonaro + Josh Irwin (Parasol) actor IDs in the Roebling Attio workspace
const ROEBLING_OWNERS   = new Set(['d17e3b6d-c768-4010-a442-8fce66c22f0e','f5c6155e-4bea-4691-b333-735f1f682bac']);
const ROE_PIPELINE_S    = ['Connected','Demo booked','Demo completed','Mutual evaluation','Champion confirmed','Proposal sent','Legal'];
const ROE_WON_S         = 'Won: H1 Early Access';
const ROE_COMMITTED_S   = 'Waitlist';

async function attioPost(path, body) {
  const r = await axios.post(`https://api.attio.com${path}`, body, {
    headers: { Authorization: `Bearer ${ATTIO_KEY}`, 'Content-Type': 'application/json' },
    timeout: 20000
  });
  return r.data;
}
const ACCOIL_TOKEN      = process.env.ACCOIL_TOKEN      || 'accoil_qhJMx5RJsdtesWSTAGjEhp9xdCfFYe64Q5rC';
const ACCOIL_WORKSPACE  = '192';

// V2 matched customers from expansion-scored.csv (57 accounts)
const MD_V2_ACCOIL = [{"id":4,"name":"Polyad","email":"anakaoka@trinet-hi.com","mrr":503.7},{"id":7,"name":"JAA Flight Operations","email":"jaasafety@flyja.com","mrr":156.0},{"id":10,"name":"OneBeat Trucking","email":"Trucking@onebeatdance.com","mrr":39.0},{"id":11,"name":"Lead Support","email":"support@leadhqcrm.com","mrr":297.0},{"id":14,"name":"Asian Sun Martial Arts","email":"info@asiansun.net","mrr":351.0},{"id":15,"name":"Atlas","email":"nativ@atlashealthadvisors.com","mrr":39.0},{"id":17,"name":"Jackie Reckson Yoga","email":"jackiereckson@gmail.com","mrr":29.0},{"id":18,"name":"AquaCats Mobile Swim School Inbox","email":"swim@aquacats.org","mrr":78.0},{"id":22,"name":"Stand Proud Inbox","email":"StandProudK9@gmail.com","mrr":87.0},{"id":27,"name":"Pro Dough","email":"wkdough8030@gmail.com","mrr":29.0},{"id":30,"name":"Red Hen Turf Farm","email":"accounts@redhenturf.com","mrr":117.0},{"id":31,"name":"Balanced Care Community Services","email":"accountinginfo2@cccsofrochester.org","mrr":585.0},{"id":32,"name":"PTP Transport","email":"ciarra_scheirer@ptptransportllc.com","mrr":117.0},{"id":34,"name":"Lynk's Racing, Inc  Inbox","email":"lynksracing@gmavt.net","mrr":29.0},{"id":36,"name":"Mark Martin Motors","email":"joshua@markmartinmotors.com","mrr":158.4},{"id":37,"name":"Stephen Disney","email":"stephendisney17@icloud.com","mrr":495.0},{"id":38,"name":"BehaviorSpan","email":"m.duthie@behaviorspan.com","mrr":312.0},{"id":39,"name":"JDSB Trucking","email":"joe@chicagotl.com","mrr":234.0},{"id":40,"name":"Regions Hospital EMS","email":"bridget.m.voelker@healthpartners.com","mrr":174.0},{"id":41,"name":"FoW Document Collection","email":"Drew.C.Michel@ey.com","mrr":518.7},{"id":43,"name":"Just In Time Roofing & Construction","email":"brian@justintimeroofky.com","mrr":78.0},{"id":46,"name":"USA Sod","email":"lisa@usasod.com","mrr":203.0},{"id":49,"name":"Chesak","email":"randi@chesakseedhouse.com","mrr":29.0},{"id":51,"name":"Mid South Outdoor Lighting & Audio","email":"office@mid-southirrigation.com","mrr":87.0},{"id":52,"name":"Sylva","email":"mwest@sylvacorp.com","mrr":117.0},{"id":53,"name":"Alliance Pro Inspections","email":"noah@allianceproinspections.com","mrr":58.0},{"id":54,"name":"Maple Lane Farm","email":"info@maplelanefarm.us","mrr":29.0},{"id":58,"name":"McGuire Furniture Inbox","email":"rental@McGuireFurnitureRental.com","mrr":39.0},{"id":59,"name":"Sudol Tax","email":"brooke@sudoltax.com","mrr":234.0},{"id":63,"name":"We Grow Hair","email":"marketing@wegrowhair.com","mrr":351.0},{"id":69,"name":"Candice Miele's Assistant","email":"admin@candicemieleskin.com","mrr":117.0},{"id":70,"name":"Haven","email":"shaneaghoian@myhavenstores.com","mrr":138.0},{"id":72,"name":"National Distributors","email":"brad@breakwatertech.com","mrr":109.2},{"id":74,"name":"Outer Reefs Consulting","email":"kristin@outerreefs.com","mrr":87.0},{"id":78,"name":"MMLG in box","email":"mark@northernarizonainjurylaw.com","mrr":116.0},{"id":81,"name":"Stan's Body Shop, Inc","email":"office@stansbodyshopinc.com","mrr":78.0},{"id":96,"name":"Home Systems","email":"contact@hsbd.com","mrr":232.0},{"id":97,"name":"Autoworld Collision","email":"randi@autoworldcollision.com","mrr":78.0},{"id":98,"name":"Ceiba Inbox","email":"admin@ceibaadventures.com","mrr":29.0},{"id":100,"name":"TLC Text","email":"tonja@tlcbookkeepingllc.com","mrr":58.0},{"id":111,"name":"Balance","email":"samhallowsdc@gmail.com","mrr":29.0},{"id":112,"name":"Kids Speak","email":"info@kidsspeakdenton.com","mrr":39.0},{"id":115,"name":"Langer Homes inbox","email":"admin@langerhomes.com","mrr":99.0},{"id":117,"name":"Crosstalk Mobile LLC","email":"david@CrosstalkMobile.com","mrr":117.0},{"id":119,"name":"Gold Stallion Services","email":"operations@goldstallionservices.com","mrr":99.0},{"id":120,"name":"Murphy Beds","email":"murphybedsormond@yahoo.com","mrr":39.0},{"id":125,"name":"InvestorBootz","email":"david.queen@investorbootz.com","mrr":195.0},{"id":129,"name":"Texas Materials - Gulf Coast Region","email":"brach.whitman@texasmaterials.com","mrr":395.0},{"id":130,"name":"Kiana Pan","email":"tours@mail.hsa.net","mrr":39.0},{"id":141,"name":"John Paul Restoration Leads","email":"monnieq30@gmail.com","mrr":58.0},{"id":147,"name":"Intero Digital - MessageDesk","email":"interoap@interodigital.com","mrr":39.0},{"id":148,"name":"Patrick Quinn","email":"pat@pquinnlaw.com","mrr":39.0},{"id":155,"name":"Rare Blue Moon Marketing","email":"alexis@rarebluemoon.io","mrr":39.0},{"id":158,"name":"Rodney Westhafer","email":"accounting@westhafer.com","mrr":39.0},{"id":160,"name":"HMS Admissions","email":"admissions@highmowing.org","mrr":39.0},{"id":161,"name":"BayCCS Support","email":"msmolens@bayccs.com","mrr":149.4},{"id":167,"name":"DSEC","email":"surbanczyk@deafsmith.coop","mrr":39.0}];

app.get('/api/messagedesk/dashboard', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['md'];
    const cached = getCache('md');
    if (cached) return res.json(cached);
    const r = await axios.get(`${MD_DASHBOARD_URL}/api/dashboard`, { timeout: 55000 });
    setCache('md', r.data);
    res.json(r.data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ─── MessageDesk pipeline — DIRECT Close query (bypasses the flaky messagedesk-dashboard proxy) ───
// Lean: only the 3 active sales stages in the Parasol pipeline (no per-lead activity history —
// that history fan-out is what makes the upstream /api/dashboard time out). ~6 Close calls, fast.
const MD_CLOSE_KEY    = process.env.CLOSE_API_KEY || '';
const MD_PIPE_ID      = 'pipe_1lXFBvtVQXtRgcjonTFr1Y';
const MD_ACTIVE_STATUS = {
  'Champion Confirmed': 'stat_cc0WZatlED2N35FGCESK5bZBWL1luEsAXERlr5PJjm2',
  'Active Evaluation':  'stat_FZAonpp4O5F7JHwY5WZPojQbWBL1s13ixQQ77ReHW2T',
  'Meeting Scheduled':  'stat_5AloLXmKYNbiif1UWs8BQ0jEeNEKGQIwfIeXfYLmB1w',
};
const MD_WON_STATUS   = 'stat_9HUWswm1Dssw9Ar6PJ4EfgrBpKw3TIyQNpuQnb33W9a';
const MD_QLOST_STATUS = ['stat_RL2VdsX6p6GYjvTIwxDrAmR8YKcO5LqTn63qGZwvQVf','stat_cxzzhNwX0SyLkKS89Yn8ntR27dIJ9jZhmyX47CapPVe'];
const MD_STAGE_WEIGHT = { 'Champion Confirmed':0.70, 'Active Evaluation':0.40, 'Meeting Scheduled':0.25 };

function mdCloseAuth() { return { Authorization: `Basic ${Buffer.from(MD_CLOSE_KEY + ':').toString('base64')}` }; }
function mdToMonthly(o) {
  if (o.value === null || o.value === undefined || o.value === '') return 0;
  const d = parseFloat(o.value) / 100, f = (o.value_period || '').toLowerCase();
  if (f === 'annual') return d / 12;
  if (f === 'one_time' || f === 'one-time') return 0;
  return d;
}
async function mdCloseCount(statusId) {
  const r = await axios.get(`https://api.close.com/api/v1/opportunity/?pipeline_id=${MD_PIPE_ID}&status_id=${statusId}&_limit=1&_fields=id`, { headers: mdCloseAuth(), timeout: 20000 });
  return r.data.total_results || 0;
}
async function buildMdPipeline() {
  const FIELDS = 'id,lead_name,status_label,value,value_period,date_created';
  const deals = [];
  for (const [label, id] of Object.entries(MD_ACTIVE_STATUS)) {
    let skip = 0;
    while (true) {
      const r = await axios.get(`https://api.close.com/api/v1/opportunity/?pipeline_id=${MD_PIPE_ID}&status_id=${id}&_limit=100&_skip=${skip}&_fields=${FIELDS}`, { headers: mdCloseAuth(), timeout: 20000 });
      for (const o of (r.data.data || [])) deals.push({ company: o.lead_name || '', stage: label, monthly_value: mdToMonthly(o), date_created: o.date_created || '' });
      if (!r.data.has_more) break;
      skip += 100;
    }
  }
  deals.sort((a,b) => b.monthly_value - a.monthly_value);
  const active_mrr   = deals.reduce((s,d) => s + d.monthly_value, 0);
  const champion     = deals.filter(d => d.stage === 'Champion Confirmed');
  const weighted_mrr = deals.reduce((s,d) => s + d.monthly_value * (MD_STAGE_WEIGHT[d.stage] || 0.2), 0);
  const by_stage = {};
  for (const d of deals) { by_stage[d.stage] = by_stage[d.stage] || { count:0, mrr:0 }; by_stage[d.stage].count++; by_stage[d.stage].mrr += d.monthly_value; }
  const counts = await Promise.all([ mdCloseCount(MD_WON_STATUS), ...MD_QLOST_STATUS.map(mdCloseCount) ]);
  const won_count = counts[0], qlost = counts.slice(1).reduce((s,n) => s + n, 0);
  const win_rate  = (won_count + qlost) > 0 ? Math.round(won_count / (won_count + qlost) * 100) : 0;
  return {
    kpis: { active_count: deals.length, active_mrr, won_count, win_rate, win_rate_denom: won_count + qlost,
            champion_count: champion.length, champion_mrr: champion.reduce((s,d)=>s+d.monthly_value,0), weighted_mrr },
    deals, by_stage,
    source: 'Close.io direct (Parasol pipeline, active stages)',
    updated_at: new Date().toISOString(),
  };
}
app.get('/api/messagedesk/pipeline', async (req, res) => {
  try {
    if (!MD_CLOSE_KEY) return res.status(503).json({ error: 'CLOSE_API_KEY not set' });
    if (req.query.refresh === '1') delete cache['md_pipeline'];
    const cached = getCache('md_pipeline'); if (cached) return res.json(cached);
    const data = await buildMdPipeline();
    setCache('md_pipeline', data);
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/messagedesk/activity', async (req, res) => {
  try {
    const r = await axios.get(`${MD_NINE_URL}/api/activity`, { timeout: 55000 });
    res.json(r.data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Instantly email send counts — per-campaign queries for full historical coverage
// Workspace is dedicated to MessageDesk; query all campaigns individually so we get complete history
app.get('/api/messagedesk/instantly', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['md_instantly'];
    const cached = getCache('md_instantly');
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);

    // Build 13-week buckets (index 0 = oldest week)
    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }
    const windowStart = weeks[0].ms;
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    const headers = { Authorization: `Bearer ${MD_INSTANTLY_KEY}` };

    // Workspace-wide daily send analytics — ONE call, no per-email pagination.
    // PRIOR APPROACH (removed): paginated /api/v2/emails per campaign via Promise.all across
    // all ~37 campaigns concurrently. That hammered Instantly into 429 rate-limiting, and the
    // catch did `break` on 429 — silently truncating each campaign after its first page or two.
    // Since emails come newest-first, only ~today's first page survived; deeper recent-week pages
    // were never reached. Result: severe, UNSTABLE undercounts (e.g. 130 WTD / 0 last-week, while
    // the true figures were ~510 / ~651). analytics/daily returns exact per-day `sent` totals for
    // the whole MD-only workspace in a single request — accurate and immune to rate limiting.
    const fmtDate = (ms) => new Date(ms).toISOString().slice(0, 10);
    const url = new URL('https://api.instantly.ai/api/v2/campaigns/analytics/daily');
    url.searchParams.set('start_date', fmtDate(windowStart));
    url.searchParams.set('end_date', fmtDate(now.getTime()));
    const r = await axios.get(url.toString(), { headers, timeout: 20000 });
    const rows = Array.isArray(r.data) ? r.data : [];

    let wtdCount = 0, lwCount = 0;
    for (const row of rows) {
      const tsMs = new Date(row.date + 'T00:00:00Z').getTime();
      const sent = row.sent || 0;
      if (!tsMs || isNaN(tsMs) || !sent) continue;
      if (tsMs >= windowStart) {
        for (const w of weeks) { if (tsMs >= w.ms && tsMs < w.end) { w.count += sent; break; } }
      }
      if (tsMs >= thisMon.getTime()) wtdCount += sent;
      else if (tsMs >= lastWkStart) lwCount += sent;
    }

    const result = { emails_wtd: wtdCount, emails_lw: lwCount, weeks, updated_at: now.toISOString() };
    setCache('md_instantly', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Accoil customer health — score → green/yellow/red, cached 60 min
app.get('/api/messagedesk/health', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['md_health'];
    const cached = getCache('md_health');
    if (cached) return res.json(cached);

    const ACCOIL_BASE = `https://api.accoil.com/v1/accounts?workspace_id=${ACCOIL_WORKSPACE}`;
    const BATCH = 10;
    const results = [];

    for (let i = 0; i < MD_V2_ACCOIL.length; i += BATCH) {
      const batch = MD_V2_ACCOIL.slice(i, i + BATCH);
      const batchResults = await Promise.allSettled(batch.map(async (acct) => {
        const r = await axios.get(`${ACCOIL_BASE}&id=${acct.id}`, {
          headers: { Authorization: `Bearer ${ACCOIL_TOKEN}` }, timeout: 10000
        });
        const m = r.data.metrics || {};
        const score = m.engagement_score || 0;
        const lastSeen = m.last_seen ? new Date(m.last_seen) : null;
        const daysSince = lastSeen ? Math.floor((Date.now() - lastSeen) / 86400000) : 999;
        // Health: red < 0.25 or unseen 60+ days; yellow 0.25–0.5; green >= 0.5
        const health = (score < 0.25 || daysSince >= 60) ? 'red'
                     : score < 0.5 ? 'yellow' : 'green';
        return { ...acct, score: Math.round(score * 100), health, last_seen: m.last_seen || null };
      }));
      for (const r of batchResults) {
        if (r.status === 'fulfilled') results.push(r.value);
      }
    }

    const green  = results.filter(a => a.health === 'green');
    const yellow = results.filter(a => a.health === 'yellow');
    const red    = results.filter(a => a.health === 'red').sort((a,b) => a.score - b.score || b.mrr - a.mrr);

    const result = { green: green.length, yellow: yellow.length, red: red.length,
      total: results.length, red_accounts: red, updated_at: new Date().toISOString() };
    setCache('md_health', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// HeyReach LinkedIn stats — sums outreach actions per day, bucketed into 13 weekly bins
app.get('/api/messagedesk/heyreach', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['md_heyreach'];
    const cached = getCache('md_heyreach');
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    // Build 13-week buckets
    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }

    const hrHeaders = { 'X-API-KEY': MD_HEYREACH_KEY, 'Content-Type': 'application/json' };
    const campsR = await axios.post('https://api.heyreach.io/api/public/campaign/GetAll',
      { pageNumber: 0, pageSize: 100 }, { headers: hrHeaders, timeout: 15000 });
    const campaigns = campsR.data.items || [];

    let liWtd = 0, liLw = 0;

    await Promise.all(campaigns.map(async (camp) => {
      try {
        const accountIds = camp.campaignAccountIds || [];
        if (!accountIds.length) return;
        const statsR = await axios.post('https://api.heyreach.io/api/public/stats/GetOverallStats',
          { AccountIds: accountIds, CampaignIds: [camp.id] },
          { headers: hrHeaders, timeout: 15000 });
        const byDay = statsR.data.byDayStats || {};
        for (const [dateStr, day] of Object.entries(byDay)) {
          const tsMs = new Date(dateStr).getTime();
          const sent = (day.connectionsSent || 0) + (day.totalMessageStarted || 0) + (day.totalInmailStarted || 0);
          if (!sent) continue;
          for (const w of weeks) { if (tsMs >= w.ms && tsMs < w.end) { w.count += sent; break; } }
          if (tsMs >= thisMon.getTime()) liWtd += sent;
          if (tsMs >= lastWkStart && tsMs < thisMon.getTime()) liLw += sent;
        }
      } catch { /* skip failed campaigns */ }
    }));

    const result = { li_wtd: liWtd, li_lw: liLw, weeks, updated_at: now.toISOString() };
    setCache('md_heyreach', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Fathom meetings — paginate /external/v1/meetings, bucket into 13 weekly bins
// Auth: X-Api-Key header. Filter: created_after=windowStart ISO string.
// Use recording_start_time (actual start) as canonical meeting timestamp.
app.get('/api/messagedesk/fathom', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['md_fathom'];
    const cached = getCache('md_fathom', 30 * 60 * 1000); // 30-min TTL — Fathom API is slow
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);

    // Build 13-week buckets
    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }
    const windowStart = weeks[0].ms;
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    const headers = { 'X-Api-Key': MD_FATHOM_KEY };
    let meetingsWtd = 0, meetingsLw = 0, cursor = null;

    // Fathom's created_after filter causes a 500 on their end — paginate newest-first instead
    // and stop once we've gone past the 13-week window
    for (let page = 0; page < 50; page++) {
      const url = new URL('https://api.fathom.ai/external/v1/meetings');
      url.searchParams.set('limit', '20');
      if (cursor) url.searchParams.set('cursor', cursor);

      const r = await axios.get(url.toString(), { headers, timeout: 20000 });
      const items = r.data.items || [];
      if (!items.length) break;

      let anyInWindow = false, anyValidTs = false;
      for (const mtg of items) {
        const tsMs = new Date(mtg.recording_start_time || mtg.created_at).getTime();
        if (isNaN(tsMs)) continue;
        anyValidTs = true;
        if (tsMs >= windowStart) {
          anyInWindow = true;
          for (const w of weeks) { if (tsMs >= w.ms && tsMs < w.end) { w.count++; break; } }
          if (tsMs >= thisMon.getTime()) meetingsWtd++;
          if (tsMs >= lastWkStart && tsMs < thisMon.getTime()) meetingsLw++;
        }
      }

      cursor = r.data.next_cursor;
      // Stop when we've gone past our window or no more pages
      if (!cursor || (anyValidTs && !anyInWindow)) break;
    }

    const result = { meetings_wtd: meetingsWtd, meetings_lw: meetingsLw, weeks, updated_at: now.toISOString() };
    setCache('md_fathom', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Roebling ─────────────────────────────────────────────────────────────────

// Pipeline: fetch all Attio deals, filter to Joe+Josh, aggregate by stage
app.get('/api/roebling/pipeline', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['roe_pipeline'];
    const cached = getCache('roe_pipeline');
    if (cached) return res.json(cached);

    // Paginate all deals (workspace has 1000+)
    const allDeals = [];
    let offset = 0;
    while (true) {
      const data = await attioPost('/v2/objects/deals/records/query', { limit: 500, offset });
      const page = data.data || [];
      allDeals.push(...page);
      if (page.length < 500) break;
      offset += 500;
    }

    // Filter to Joe / Josh owner IDs
    const deals = allDeals
      .filter(r => ROEBLING_OWNERS.has(r.values.owner?.[0]?.referenced_actor_id))
      .map(r => {
        const v = r.values;
        return {
          name:     v.name?.[0]?.value || '',
          stage:    v.stage?.[0]?.status?.title || '',
          weighted: v.weighted_deal_value_6?.[0]?.currency_value || 0,
        };
      });

    let activeDeals = 0, activeWeighted = 0, wonDeals = 0, wonWeighted = 0, committedDeals = 0, committedWeighted = 0;
    const byStage = {};
    for (const d of deals) {
      if (!byStage[d.stage]) byStage[d.stage] = { count: 0, weighted: 0, deals: [] };
      byStage[d.stage].count++;
      byStage[d.stage].weighted += d.weighted;
      byStage[d.stage].deals.push({ name: d.name, weighted: d.weighted });
      if (ROE_PIPELINE_S.includes(d.stage))  { activeDeals++;    activeWeighted    += d.weighted; }
      else if (d.stage === ROE_WON_S)        { wonDeals++;       wonWeighted       += d.weighted; }
      else if (d.stage === ROE_COMMITTED_S)  { committedDeals++; committedWeighted += d.weighted; }
    }

    const stages = ROE_PIPELINE_S.map(s => ({
      stage: s, count: byStage[s]?.count || 0, weighted: byStage[s]?.weighted || 0,
      deals: byStage[s]?.deals || []
    }));

    const result = {
      active_deals: activeDeals, active_weighted: activeWeighted,
      won_deals: wonDeals, won_weighted: wonWeighted,
      committed_deals: committedDeals, committed_weighted: committedWeighted,
      stages, updated_at: new Date().toISOString()
    };
    setCache('roe_pipeline', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Calls: bucket Attio calls by week into 13-week bins
app.get('/api/roebling/calls', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['roe_calls'];
    const cached = getCache('roe_calls');
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);

    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }
    const windowStart = weeks[0].ms;
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    let callsWtd = 0, callsLw = 0, offset = 0;
    while (offset < 2000) {
      const data = await attioPost('/v2/objects/calls/records/query', {
        limit: 100, offset,
        sorts: [{ attribute: 'call_date', direction: 'desc' }]
      });
      const page = data.data || [];
      if (!page.length) break;
      let anyInWindow = false, anyValidTs = false;
      for (const r of page) {
        const dateStr = r.values.call_date?.[0]?.value;
        if (!dateStr) continue;
        const tsMs = new Date(dateStr + 'T00:00:00Z').getTime();
        anyValidTs = true;
        if (tsMs >= windowStart) {
          anyInWindow = true;
          for (const w of weeks) { if (tsMs >= w.ms && tsMs < w.end) { w.count++; break; } }
          if (tsMs >= thisMon.getTime()) callsWtd++;
          if (tsMs >= lastWkStart && tsMs < thisMon.getTime()) callsLw++;
        }
      }
      if (anyValidTs && !anyInWindow) break;
      offset += 100;
      if (page.length < 100) break;
    }

    const result = { calls_wtd: callsWtd, calls_lw: callsLw, weeks, updated_at: now.toISOString() };
    setCache('roe_calls', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HS_BASE = 'https://api.hubapi.com';
const DUET_PIPELINE_ID = '2168635108';
const MEETING_BOOKED_STAGE_ID = '3467751100';

const DUET_STAGE_MAP = {
  '3446819577': 'New / Not Yet Contacted',
  '3446820538': 'Attempting Contact',
  '3446820539': 'Parasol Engaged',
  '3467751100': 'Meeting Booked',
  '3446820540': 'Meeting Held',
  '3467565765': 'Interest Confirmed',
  '3477604030': 'Diagnostic',
  '3446820542': 'LOI Sent',
  '3446820543': 'Enrolled / Won',
  '3446820544': 'Not Interested / Lost',
  '3446820545': 'Come Back To',
  '3446820546': 'Not Relevant / DQ',
};

const QUALIFIED_STAGE_IDS = new Set(['3467751100','3446820540','3467565765','3477604030','3446820542','3446820543']);

const DUET_OWNER_MAP = {
  '163553901': 'Jonathan Goldberg',
  '163553854': 'Florencia Scopp',
  '83189293': 'Joe',
  '163575365': 'Alicia Ortiz',
  '163553855': 'Blair',
};

const cache = {};
function getCache(key, ttlMs = 5 * 60 * 1000) {
  const e = cache[key];
  if (!e) return null;
  if (Date.now() - e.ts > ttlMs) return null;
  return e.data;
}
function setCache(key, data) { cache[key] = { data, ts: Date.now() }; }

const ownerCache = {};
async function resolveOwner(id) {
  if (!id) return 'Unknown';
  if (DUET_OWNER_MAP[id]) return DUET_OWNER_MAP[id];
  if (ownerCache[id]) return ownerCache[id];
  try {
    const r = await axios.get(`${HS_BASE}/crm/v3/owners/${id}`, {
      headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
    });
    const name = [r.data.firstName, r.data.lastName].filter(Boolean).join(' ') || r.data.email || id;
    ownerCache[id] = name;
    return name;
  } catch { ownerCache[id] = id; return id; }
}

async function fetchStageHistory(dealIds) {
  const stageEnteredMap = {};
  const chunks = [];
  for (let i = 0; i < dealIds.length; i += 50) chunks.push(dealIds.slice(i, i + 50));
  // Fetch all chunks in parallel — much faster than sequential
  await Promise.all(chunks.map(async (chunk) => {
    try {
      const body = { inputs: chunk.map(id => ({ id })), propertiesWithHistory: ['dealstage'] };
      const r = await axios.post(`${HS_BASE}/crm/v3/objects/deals/batch/read`, body, {
        headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' }
      });
      for (const result of (r.data.results || [])) {
        const history = result.propertiesWithHistory?.dealstage || [];
        let meetingBookedAt = null;
        let qualifiedAt = null;
        const sortedHistory = history.slice().sort((a,b)=>new Date(a.timestamp)-new Date(b.timestamp));
        for (const h of sortedHistory) {
          if (h.value === MEETING_BOOKED_STAGE_ID && !meetingBookedAt) meetingBookedAt = h.timestamp;
          if (QUALIFIED_STAGE_IDS.has(h.value) && !qualifiedAt) qualifiedAt = h.timestamp;
        }
        stageEnteredMap[result.id] = { meetingBookedAt, qualifiedAt };
      }
    } catch(e) { console.error('Stage history fetch error:', e.message); }
  }));
  return stageEnteredMap;
}

async function fetchMeetingEngagements(dealIds) {
  const nowMs = Date.now();
  const hasUpcomingMap = {};
  if (!dealIds.length) return hasUpcomingMap;

  // Step 1: associations for all deals in parallel
  const assocResults = await Promise.allSettled(dealIds.map(async (dealId) => {
    const r = await axios.get(`${HS_BASE}/crm/v3/objects/deals/${dealId}/associations/meetings`, {
      headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` }
    });
    return { dealId, mtgIds: (r.data.results || []).map(m => m.id) };
  }));

  const dealToMtgIds = {};
  const allMtgIds = [];
  for (const r of assocResults) {
    if (r.status === 'fulfilled' && r.value.mtgIds.length) {
      dealToMtgIds[r.value.dealId] = r.value.mtgIds;
      allMtgIds.push(...r.value.mtgIds);
    }
  }
  if (!allMtgIds.length) return hasUpcomingMap;

  // Step 2: batch read meeting start times
  const unique = [...new Set(allMtgIds)];
  const chunks = [];
  for (let i = 0; i < unique.length; i += 100) chunks.push(unique.slice(i, i + 100));
  const mtgStartMap = {};
  await Promise.all(chunks.map(async (chunk) => {
    const r = await axios.post(`${HS_BASE}/crm/v3/objects/meetings/batch/read`,
      { inputs: chunk.map(id => ({ id })), properties: ['hs_meeting_start_time'] },
      { headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' } }
    );
    for (const m of (r.data.results || [])) {
      const ts = m.properties?.hs_meeting_start_time;
      if (ts) mtgStartMap[m.id] = new Date(ts).getTime();
    }
  }));

  // Step 3: tag each deal
  for (const [dealId, mtgIds] of Object.entries(dealToMtgIds)) {
    hasUpcomingMap[dealId] = mtgIds.some(id => (mtgStartMap[id] || 0) > nowMs);
  }
  return hasUpcomingMap;
}

async function fetchDealEligibility(dealIds) {
  const eligMap = {};
  if (!dealIds.length) return eligMap;

  // Batch fetch deal→company associations (v4 batch API, 100 per request)
  const chunks = [];
  for (let i = 0; i < dealIds.length; i += 100) chunks.push(dealIds.slice(i, i + 100));

  const dealToCompanyId = {};
  await Promise.all(chunks.map(async (chunk) => {
    try {
      const r = await axios.post(`${HS_BASE}/crm/v4/associations/deals/companies/batch/read`,
        { inputs: chunk.map(id => ({ id: String(id) })) },
        { headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' } }
      );
      for (const result of (r.data.results || [])) {
        const tos = result.to || [];
        if (tos.length) dealToCompanyId[result.from.id] = String(tos[0].toObjectId);
      }
    } catch(e) { console.error('Eligibility assoc error:', e.message); }
  }));

  // Batch read company aco_lead_eligibility
  const companyIds = [...new Set(Object.values(dealToCompanyId))];
  if (!companyIds.length) return eligMap;

  const compChunks = [];
  for (let i = 0; i < companyIds.length; i += 100) compChunks.push(companyIds.slice(i, i + 100));

  const compEligMap = {};
  await Promise.all(compChunks.map(async (chunk) => {
    try {
      const r = await axios.post(`${HS_BASE}/crm/v3/objects/companies/batch/read`,
        { inputs: chunk.map(id => ({ id })), properties: ['aco_lead_eligibility'] },
        { headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' } }
      );
      for (const comp of (r.data.results || [])) {
        compEligMap[comp.id] = comp.properties?.aco_lead_eligibility || null;
      }
    } catch(e) { console.error('Eligibility company fetch error:', e.message); }
  }));

  for (const [dealId, compId] of Object.entries(dealToCompanyId)) {
    eligMap[dealId] = compEligMap[compId] || null;
  }
  return eligMap;
}

async function fetchDuetDeals() {
  const cached = getCache('duet');
  if (cached) return cached;

  const deals = [];
  let after = null;
  const PROPS = 'dealname,dealstage,pipeline,hubspot_owner_id,closedate,hs_lastmodifieddate,attribution_2025,gross_savings_2025_deal,outreach_attempt_count,last_outreach_date,meeting_date,loi_sent_date,loi_signed_date,enrollment_date,enrollment_deadline,champion_name,champion_role,lost_reason,deal_source,duet_engaged_owner,secondary_owner,meeting_set,np_intro_made,hs_date_entered_3467751100';

  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'pipeline', operator: 'EQ', value: DUET_PIPELINE_ID }] }],
      properties: PROPS.split(','),
      limit: 100,
    };
    if (after) body.after = after;
    const r = await axios.post(`${HS_BASE}/crm/v3/objects/deals/search`, body, {
      headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' }
    });
    deals.push(...(r.data.results || []));
    if (r.data.paging?.next?.after) after = r.data.paging.next.after;
    else break;
  }

  const ownerIds = [...new Set(deals.map(d => d.properties?.hubspot_owner_id).filter(Boolean))];
  await Promise.all(ownerIds.map(id => resolveOwner(id)));

  // Fetch history for all Parasol-owned deals (not just currently-qualified ones)
  // so we catch deals that were Meeting Booked and later bounced back to earlier stages
  const PARASOL_OWNER_IDS = new Set(['163553854','83189293','164358712']); // Florencia, Joe, Lauren
  const parasolDeals = deals.filter(d => PARASOL_OWNER_IDS.has(d.properties?.hubspot_owner_id || ''));
  const stageHistoryMap = await fetchStageHistory(parasolDeals.map(d => d.id));

  const mapped = deals.map(d => {
    const p = d.properties || {};
    const stageId = p.dealstage || '';
    const ownerId = p.hubspot_owner_id || '';
    const history = stageHistoryMap[d.id] || {};
    return {
      id: d.id,
      dealname: p.dealname || '',
      stage_id: stageId,
      stage: DUET_STAGE_MAP[stageId] || stageId,
      owner: DUET_OWNER_MAP[ownerId] || ownerCache[ownerId] || ownerId,
      ownerId: ownerId,
      lives: parseFloat(p.attribution_2025) || 0,
      gross_savings: parseFloat(p.gross_savings_2025_deal) || 0,
      outreach_attempts: parseInt(p.outreach_attempt_count) || 0,
      last_outreach: p.last_outreach_date || null,
      meeting_date: p.meeting_date || null,
      loi_sent_date: p.loi_sent_date || null,
      closedate: p.closedate || null,
      last_modified: p.hs_lastmodifieddate || null,
      champion_name: p.champion_name || '',
      champion_role: p.champion_role || '',
      lost_reason: p.lost_reason || '',
      meeting_set: p.meeting_set || '',
      meetingBookedAt: history.meetingBookedAt || null,
      qualifiedAt:     history.qualifiedAt     || null,
    };
  });

  // Fetch meeting engagements + deal eligibility in parallel
  const bookedIds = mapped
    .filter(d => d.stage_id === MEETING_BOOKED_STAGE_ID && PARASOL_OWNER_IDS.has(d.ownerId))
    .map(d => d.id);
  const [hasUpcomingMap, eligibilityMap] = await Promise.all([
    fetchMeetingEngagements(bookedIds),
    fetchDealEligibility(mapped.map(d => d.id))
  ]);
  for (const d of mapped) {
    d.hasUpcomingMeeting = hasUpcomingMap[d.id] || false;
    d.eligibility = eligibilityMap[d.id] || null;
  }

  const result = { deals: mapped, updated_at: new Date().toISOString() };
  setCache('duet', result);
  return result;
}


app.get('/api/duet/deals', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['duet'];
    res.json(await fetchDuetDeals());
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("/api/duet/team-performance", async (req, res) => {
  try {
    const r = await axios.get("https://duet-dashboard.vercel.app/api/team-performance", { timeout: 25000 });
    const d = r.data || {};
    // Ensure every consumed metric carries a provenance stamp (brief no-carry-over rule)
    res.json({ ...d, updated_at: d.updated_at || new Date().toISOString() });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── Terrabase ─────────────────────────────────────────────────────────────────

// Pipeline (Notion CRM snapshot) + Instantly ecomm email stats
app.get('/api/terrabase/pipeline', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['tb_pipeline'];
    const cached = getCache('tb_pipeline');
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);

    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }
    const windowStart = weeks[0].ms;
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    const headers = { Authorization: `Bearer ${TB_INSTANTLY_KEY}` };
    let emailsWtd = 0, emailsLw = 0, cursor = null;

    for (let page = 0; page < 50; page++) {
      const url = new URL('https://api.instantly.ai/api/v2/emails');
      url.searchParams.set('limit', '100');
      url.searchParams.set('campaign_id', TB_ECOMM_CAMP_ID);
      if (cursor) url.searchParams.set('starting_after', cursor);
      let r;
      try {
        r = await axios.get(url.toString(), { headers, timeout: 15000 });
      } catch(pageErr) {
        if (pageErr.response?.status === 429) break;
        throw pageErr;
      }
      const items = r.data.items || [];
      if (!items.length) break;
      let anyInWindow = false, anyValidTs = false;
      for (const item of items) {
        const tsMs = new Date(item.timestamp_email).getTime();
        if (!tsMs || isNaN(tsMs)) continue;
        anyValidTs = true;
        if (tsMs >= windowStart) {
          anyInWindow = true;
          for (const w of weeks) { if (tsMs >= w.ms && tsMs < w.end) { w.count++; break; } }
          if (tsMs >= thisMon.getTime()) emailsWtd++;
          if (tsMs >= lastWkStart && tsMs < thisMon.getTime()) emailsLw++;
        }
      }
      cursor = r.data.next_starting_after;
      if (!cursor || (anyValidTs && !anyInWindow)) break;
    }

    const activeDeals = TB_CRM_DEALS.filter(d => !['Closed Won','Closed Lost'].includes(d.stage));
    const wonDeals    = TB_CRM_DEALS.filter(d => d.stage === 'Closed Won');
    const totalArr    = activeDeals.reduce((sum, d) => sum + (d.arr || 0), 0);

    const result = {
      deals: TB_CRM_DEALS,
      active_deals: activeDeals.length,
      won_deals: wonDeals.length,
      total_arr: totalArr,
      emails_wtd: emailsWtd,
      emails_lw: emailsLw,
      weeks,
      updated_at: now.toISOString()
    };
    setCache('tb_pipeline', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// HeyReach LinkedIn stats — Terrabase warm + retail network campaigns
// Auth: MCP JSON-RPC (xMcpKey), NOT REST X-API-KEY (REST key not available)
// progressStats.totalUsersFinished bucketed by campaign startedAt date
app.get('/api/terrabase/heyreach', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['tb_heyreach'];
    const cached = getCache('tb_heyreach');
    if (cached) return res.json(cached);

    const now = new Date();
    const dow = now.getUTCDay();
    const daysToMon = dow === 0 ? 6 : dow - 1;
    const thisMon = new Date(now);
    thisMon.setUTCDate(now.getUTCDate() - daysToMon);
    thisMon.setUTCHours(0, 0, 0, 0);
    const lastWkStart = thisMon.getTime() - 7 * 86400000;

    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const m = new Date(thisMon);
      m.setUTCDate(thisMon.getUTCDate() - i * 7);
      weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
        lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
    }

    const MCP_URL = `https://mcp.heyreach.io/mcp?xMcpKey=${encodeURIComponent(TB_HEYREACH_KEY)}`;
    const mcpRes = await axios.post(MCP_URL, {
      jsonrpc: '2.0', id: 1, method: 'tools/call',
      params: { name: 'get_all_campaigns', arguments: { pageNumber: 0, pageSize: 100 } }
    }, { timeout: 20000, responseType: 'text' });

    const dataLine = (mcpRes.data || '').split('\n').find(l => l.startsWith('data: '));
    if (!dataLine) throw new Error('No data in MCP response');
    const mcpJson = JSON.parse(dataLine.slice(6));
    const campaigns = JSON.parse(mcpJson.result?.content?.[0]?.text || '{}').items || [];

    let liWtd = 0, liLw = 0;
    for (const camp of campaigns) {
      if (!camp.startedAt) continue;
      const finished = camp.progressStats?.totalUsersFinished || 0;
      if (!finished) continue;
      const tsMs = new Date(camp.startedAt).getTime();
      for (const w of weeks) {
        if (tsMs >= w.ms && tsMs < w.end) {
          w.count += finished;
          if (tsMs >= thisMon.getTime()) liWtd += finished;
          if (tsMs >= lastWkStart && tsMs < thisMon.getTime()) liLw += finished;
          break;
        }
      }
    }

    const result = { li_wtd: liWtd, li_lw: liLw, weeks, updated_at: now.toISOString() };
    setCache('tb_heyreach', result);
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/terrabase/advisor', (req, res) => {
  const now = new Date();
  const dow = now.getUTCDay();
  const daysToMon = dow === 0 ? 6 : dow - 1;
  const thisMon = new Date(now);
  thisMon.setUTCDate(now.getUTCDate() - daysToMon);
  thisMon.setUTCHours(0, 0, 0, 0);

  const weeks = [];
  for (let i = 12; i >= 0; i--) {
    const m = new Date(thisMon);
    m.setUTCDate(thisMon.getUTCDate() - i * 7);
    weeks.push({ ms: m.getTime(), end: m.getTime() + 7 * 86400000,
      lbl: `${m.getUTCMonth()+1}/${m.getUTCDate()}`, cur: i === 0, count: 0 });
  }

  for (const call of TB_ADVISOR_CALLS) {
    if (!call.call_date) continue;
    const tsMs = new Date(call.call_date + 'T12:00:00Z').getTime();
    for (const w of weeks) {
      if (tsMs >= w.ms && tsMs < w.end) { w.count++; break; }
    }
  }

  const completed = TB_ADVISOR_CALLS.filter(c => c.status === 'Completed').length;
  const graduated = TB_ADVISOR_CALLS.filter(c => c.graduating).length;
  res.json({ weeks, total: TB_ADVISOR_CALLS.length, completed, graduated,
    rate: completed > 0 ? Math.round(graduated / completed * 100) : null,
    updated_at: now.toISOString() });
});

// ── Consolidated brief feed ─────────────────────────────────────────────────
// One source-stamped payload for the weekly Strategic Brief. Every metric carries
// { source, updated_at, stale }. A feed that can't be fetched fresh THIS request is
// marked stale:true with its value nulled — never silently carried forward (enforces
// the no-carry-over rule structurally). Full registry of which numbers map to which
// feed: parasol-clients/internal/brief-system/metrics-source-map.md.

// Duet aggregation — MIRRORS public/index.html loadDuet() exactly. Keep the two in
// sync (TODO: unify into one shared module so they can never drift).
const DUET_BRIEF_QUAL   = new Set(['Meeting Booked','Meeting Held','Interest Confirmed','Diagnostic','LOI Sent','Enrolled / Won']);
const DUET_BRIEF_PAR_OW = new Set(['163553854','83189293','164358712']); // Florencia, Joe, Lauren
function aggregateDuetForBrief(rawDeals) {
  const isElig = d => d.eligibility !== '3 - Ineligible';
  const deals  = (rawDeals || []).filter(d => DUET_BRIEF_PAR_OW.has(String(d.ownerId || '')));
  const pipe   = deals.filter(d => DUET_BRIEF_QUAL.has(d.stage || d.stage_id) && isElig(d));
  const total_savings = pipe.reduce((s,d) => s + Math.max(0, d.gross_savings || 0), 0);
  const total_lives   = pipe.reduce((s,d) => s + (d.lives || 0), 0);
  const top_deals = [...pipe].sort((a,b) => (b.gross_savings||0) - (a.gross_savings||0))
    .slice(0,5).map(d => ({ name: d.dealname, stage: d.stage, savings: d.gross_savings||0, lives: d.lives||0 }));
  return { active_deals: pipe.length, total_savings, total_lives, top_deals };
}

app.get('/api/brief', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['brief'];
    const cached = getCache('brief');
    if (cached) return res.json(cached);

    const proto = req.headers['x-forwarded-proto'] || req.protocol || 'https';
    const base  = `${proto}://${req.headers.host}`;
    const rq    = req.query.refresh === '1' ? '?refresh=1' : '';
    const slow  = req.query.refresh === '1' ? 55000 : 45000;

    const get = async (pathname, timeout = 25000) => {
      const t0 = Date.now();
      try {
        const r = await axios.get(`${base}${pathname}${rq}`, { timeout });
        if (r.data && r.data.error) return { ok:false, error:r.data.error, ms:Date.now()-t0 };
        return { ok:true, data:r.data, ms:Date.now()-t0 };
      } catch(e) { return { ok:false, error:e.message, ms:Date.now()-t0 }; }
    };

    const [roePipe, roeCalls, duetDeals, duetTeam, mdHealth, mdInst, mdHR, mdFathom, mdDash, tbPipe, tbHR, tbAdv] =
      await Promise.all([
        get('/api/roebling/pipeline'),
        get('/api/roebling/calls'),
        get('/api/duet/deals', 35000),
        get('/api/duet/team-performance'),
        get('/api/messagedesk/health'),
        get('/api/messagedesk/instantly'),
        get('/api/messagedesk/heyreach'),
        get('/api/messagedesk/fathom', slow),
        get('/api/messagedesk/pipeline', 25000), // direct Close query — replaces the flaky messagedesk-dashboard proxy
        get('/api/terrabase/pipeline'),
        get('/api/terrabase/heyreach'),
        get('/api/terrabase/advisor'),
      ]);

    // wrap a feed into a stamped metric; stale:true means "could not fetch fresh — do not carry"
    const stamp = (feed, source, shape) => feed.ok
      ? { stale:false, source, updated_at: feed.data?.updated_at || null, ...shape(feed.data) }
      : { stale:true,  source, updated_at:null, value:null, error: feed.error };

    const duetAgg = duetDeals.ok ? aggregateDuetForBrief(duetDeals.data.deals) : null;

    const payload = {
      generated_at: new Date().toISOString(),
      base_url: base,
      no_carry_over: 'Every value below was fetched this request. stale:true means the live source could not be reached this cycle — drop or hand-source that number, never carry a prior value.',
      clients: {
        messagedesk: {
          health: stamp(mdHealth, 'Accoil via /api/messagedesk/health',
            d => ({ green:d.green, yellow:d.yellow, red:d.red, total:d.total })),
          activity: {
            emails:   stamp(mdInst,   'Instantly via /api/messagedesk/instantly', d => ({ wtd:d.emails_wtd, lw:d.emails_lw, weeks:d.weeks })),
            linkedin: stamp(mdHR,     'HeyReach via /api/messagedesk/heyreach',    d => ({ wtd:d.li_wtd, lw:d.li_lw, weeks:d.weeks })),
            meetings: stamp(mdFathom, 'Fathom via /api/messagedesk/fathom',        d => ({ wtd:d.meetings_wtd, lw:d.meetings_lw, weeks:d.weeks })),
          },
          pipeline: mdDash.ok
            ? { stale:false, source:'Close.io direct (active pipeline) via /api/messagedesk/pipeline', updated_at: mdDash.data?.updated_at || null, value: mdDash.data }
            : { stale:true,  source:'Close.io ($2k+ ARR Active Deals smart view)', updated_at:null, value:null,
                note:`Direct Close query failed (${mdDash.error||''}). Hand-pull from Close.io smart view save_1G9JYa5hgaGtxznyVFbhRfHubSo9HwSAjKWwo3k46T7. Check CLOSE_API_KEY is set in hub-v3 Vercel env.` },
        },
        duet: {
          pipeline: duetAgg
            ? { stale:false, source:'HubSpot via /api/duet/deals (Parasol-owned ∩ qualified stages ∩ eligible — mirrors dashboard)', updated_at: duetDeals.data?.updated_at || null, ...duetAgg }
            : { stale:true,  source:'HubSpot via /api/duet/deals', updated_at:null, value:null, error: duetDeals.error },
          activity: stamp(duetTeam, 'HubSpot via /api/duet/team-performance', d => ({ owners:d.owners })),
        },
        roebling: {
          pipeline: stamp(roePipe, 'Attio via /api/roebling/pipeline',
            d => ({ active_deals:d.active_deals, active_weighted:d.active_weighted, won_deals:d.won_deals, committed_deals:d.committed_deals, stages:d.stages })),
          calls: stamp(roeCalls, 'Attio via /api/roebling/calls', d => ({ wtd:d.calls_wtd, lw:d.calls_lw, weeks:d.weeks })),
          note: 'Confidence % and projected close dates are NOT sourced here (Tier C). Confirm with Joe each cycle or omit. TCV = Attio weighted_deal_value_6 (mostly std $20K).',
        },
        terrabase: {
          pipeline: tbPipe.ok
            ? { stale:false, snapshot:true, snapshot_date: TB_CRM_SNAPSHOT_DATE,
                source:'Notion CRM snapshot (TB_CRM_DEALS, hardcoded in hub-v3 server.js)',
                note:`Tier B — real freshness is the snapshot edit date (${TB_CRM_SNAPSHOT_DATE}), NOT updated_at. Verify against the Terrabase Notion Live Deals CRM before publishing.`,
                updated_at: tbPipe.data?.updated_at || null,
                active_deals: tbPipe.data.active_deals, won_deals: tbPipe.data.won_deals, total_arr: tbPipe.data.total_arr, deals: tbPipe.data.deals }
            : { stale:true, source:'Notion CRM snapshot (TB_CRM_DEALS)', updated_at:null, value:null, error: tbPipe.error },
          activity: {
            emails:   stamp(tbPipe, 'Instantly via /api/terrabase/pipeline', d => ({ wtd:d.emails_wtd, lw:d.emails_lw, weeks:d.weeks })),
            linkedin: stamp(tbHR,   'HeyReach via /api/terrabase/heyreach',  d => ({ wtd:d.li_wtd, lw:d.li_lw, weeks:d.weeks })),
            advisor:  tbAdv.ok
              ? { stale:false, snapshot:true, snapshot_date: TB_ADVISOR_SNAPSHOT_DATE,
                  source:'Notion advisor tracker snapshot (TB_ADVISOR_CALLS, hardcoded)',
                  updated_at: tbAdv.data?.updated_at || null,
                  total: tbAdv.data.total, completed: tbAdv.data.completed, graduated: tbAdv.data.graduated, rate: tbAdv.data.rate }
              : { stale:true, source:'Notion advisor tracker snapshot', updated_at:null, value:null, error: tbAdv.error },
          },
        },
      },
      tier_c_uninstrumented: [
        'Arlow (contacts / operators / calls / leads — manual)',
        'MessageDesk cold-call volume (not tracked anywhere)',
        'Roebling confidence % + projected close dates (confirm with Joe)',
        'Craniometrix (renewal %, $/mo — qualitative)',
        'BD pipeline: Nirvana / Ignitia / FraudNet / Boreal (proposals, Grain, dated Slack)',
        'Solidly / Levain / Raise (narrative only — no $ without a cited source)',
      ],
      feeds: {
        roebling_pipeline:{ ok:roePipe.ok, ms:roePipe.ms }, roebling_calls:{ ok:roeCalls.ok, ms:roeCalls.ms },
        duet_deals:{ ok:duetDeals.ok, ms:duetDeals.ms }, duet_team:{ ok:duetTeam.ok, ms:duetTeam.ms },
        md_health:{ ok:mdHealth.ok, ms:mdHealth.ms }, md_instantly:{ ok:mdInst.ok, ms:mdInst.ms },
        md_heyreach:{ ok:mdHR.ok, ms:mdHR.ms }, md_fathom:{ ok:mdFathom.ok, ms:mdFathom.ms },
        md_dashboard:{ ok:mdDash.ok, ms:mdDash.ms }, tb_pipeline:{ ok:tbPipe.ok, ms:tbPipe.ms },
        tb_heyreach:{ ok:tbHR.ok, ms:tbHR.ms }, tb_advisor:{ ok:tbAdv.ok, ms:tbAdv.ms },
      },
    };

    // Cache when every feed EXCEPT the known-down Close proxy is healthy. (md_dashboard
    // is persistently failing; gating cache on it would force a full slow fan-out every call.
    // Drop it from this exclusion list once the proxy is fixed.)
    const cacheable = Object.entries(payload.feeds)
      .filter(([k]) => k !== 'md_dashboard').every(([, f]) => f.ok);
    if (cacheable) setCache('brief', payload);
    res.json(payload);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

app.get("*", (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => console.log(`Parasol Hub running on port ${PORT}`));

// Proxy team performance from Duet dashboard
