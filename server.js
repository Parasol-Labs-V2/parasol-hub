require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Close.io setup ────────────────────────────────────────────────────────────
const CLOSE_API_KEY = process.env.CLOSE_API_KEY;
const CLOSE_BASE = 'https://api.close.com/api/v1';

const closeApi = axios.create({
  baseURL: CLOSE_BASE,
  auth: { username: CLOSE_API_KEY, password: '' },
  timeout: 30000,
});

async function fetchAllPages(endpoint, params = {}) {
  const results = [];
  let skip = 0;
  const limit = 100;
  while (true) {
    const res = await closeApi.get(endpoint, { params: { ...params, _limit: limit, _skip: skip } });
    const data = res.data;
    results.push(...(data.data || []));
    if (!data.has_more) break;
    skip += limit;
  }
  return results;
}

function toMonthly(opp) {
  const val = parseFloat(opp.value) || 0;
  const freq = (opp.value_period || '').toLowerCase();
  if (freq === 'annual') return val / 12;
  if (freq === 'one_time' || freq === 'one-time') return 0;
  return val;
}

function a2pStage(raw) {
  if (!raw) return 0;
  const m = raw.match(/^(\d+)\./);
  return m ? parseInt(m[1]) : 0;
}

// ─── HubSpot setup ─────────────────────────────────────────────────────────────
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HS_BASE = 'https://api.hubapi.com';

async function hsFetch(path) {
  const res = await axios.get(HS_BASE + path, {
    headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}` },
    timeout: 30000,
  });
  return res.data;
}

async function hsPost(path, body) {
  const res = await axios.post(HS_BASE + path, body, {
    headers: { Authorization: `Bearer ${HUBSPOT_API_KEY}`, 'Content-Type': 'application/json' },
    timeout: 30000,
  });
  return res.data;
}

// ─── Simple cache ──────────────────────────────────────────────────────────────
const cache = {};
function getCache(key) {
  const e = cache[key];
  if (!e) return null;
  if (Date.now() - e.ts > 5 * 60 * 1000) return null;
  return e.data;
}
function setCache(key, data) { cache[key] = { data, ts: Date.now() }; }

// ─── MessageDesk ───────────────────────────────────────────────────────────────
async function fetchMessageDeskDashboard() {
  const cached = getCache('messagedesk');
  if (cached) return cached;

  const leads = await fetchAllPages('/lead/', {
    query: 'has:opportunities',
    _fields: 'id,display_name,opportunities,custom,contacts,date_created',
  });

  const opps = await fetchAllPages('/opportunity/', {
    _fields: 'id,lead_id,lead_name,status_id,status_label,status_type,value,value_period,date_created,date_updated,date_won,date_lost,user_id,user_name,note,confidence,custom',
    pipeline_id: 'pipe_1lXFBvtVQXtRgcjonTFr1Y',
  });

  // Filter to Parasol pipeline only
  const PARASOL_PIPELINE = 'pipe_1lXFBvtVQXtRgcjonTFr1Y';
  const filteredOpps = opps.filter(o => o.pipeline_id === PARASOL_PIPELINE);
  const oppsToProcess = filteredOpps.length > 0 ? filteredOpps : opps;

  const statusRes = await closeApi.get('/status/opportunity/');
  const statuses = statusRes.data.data || [];

  const leadMap = {};
  for (const lead of leads) leadMap[lead.id] = lead;

  let a2pFieldId = null;
  let pipelineFieldId = null;
  for (const lead of leads) {
    if (lead.custom) {
      for (const [k, v] of Object.entries(lead.custom)) {
        if (typeof v === 'string' && v.match(/^\d+\.\s+/)) {
          if (!a2pFieldId) a2pFieldId = k;
        }
      }
    }
  }

  const activeOpps = [], wonOpps = [], lostOpps = [];
  let totalPipelineValue = 0, totalWonMRR = 0;

  const statusTypeMap = {};
  for (const s of statuses) statusTypeMap[s.id] = s.type;

  for (const opp of oppsToProcess) {
    const monthly = toMonthly(opp);
    const lead = leadMap[opp.lead_id] || {};
    const custom = lead.custom || {};

    let a2pStatus = '';
    let pipelineReview = '';
    if (!a2pFieldId) {
      for (const [k, v] of Object.entries(custom)) {
        if (typeof v === 'string' && v.match(/^\d+\.\s+/)) {
          a2pFieldId = k; a2pStatus = v; break;
        }
      }
    } else {
      a2pStatus = custom[a2pFieldId] || '';
    }
    if (!pipelineFieldId) {
      for (const [k, v] of Object.entries(custom)) {
        if (k !== a2pFieldId && typeof v === 'string' && v.length > 20) {
          pipelineFieldId = k; pipelineReview = v; break;
        }
      }
    } else {
      pipelineReview = custom[pipelineFieldId] || '';
    }

    const created = new Date(opp.date_created);
    const ageDays = Math.floor((Date.now() - created.getTime()) / 86400000);

    const enriched = {
      ...opp,
      monthly_value: monthly,
      age_days: ageDays,
      a2p_status: a2pStatus,
      a2p_stage: a2pStage(a2pStatus),
      pipeline_review: pipelineReview,
      lead_display_name: opp.lead_name || lead.display_name || '',
    };

    const sType = statusTypeMap[opp.status_id] || opp.status_type || '';
    if (sType === 'won') { wonOpps.push(enriched); totalWonMRR += monthly; }
    else if (sType === 'lost') { lostOpps.push(enriched); }
    else { activeOpps.push(enriched); totalPipelineValue += monthly; }
  }

  activeOpps.sort((a, b) => b.monthly_value - a.monthly_value);

  const wonCount = wonOpps.length, lostCount = lostOpps.length;
  const closedTotal = wonCount + lostCount;
  const winRate = closedTotal > 0 ? Math.round((wonCount / closedTotal) * 100) : 0;
  const avgDealSize = wonCount > 0 ? Math.round(totalWonMRR / wonCount) : 0;

  const a2pBreakdown = { 0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
  for (const o of activeOpps) a2pBreakdown[o.a2p_stage] = (a2pBreakdown[o.a2p_stage] || 0) + 1;

  const pipelineByStatus = {};
  for (const o of activeOpps) {
    const label = o.status_label || 'Unknown';
    if (!pipelineByStatus[label]) pipelineByStatus[label] = { count: 0, value: 0 };
    pipelineByStatus[label].count++;
    pipelineByStatus[label].value += o.monthly_value;
  }

  const mrrByMonth = {};
  for (const o of wonOpps) {
    const d = new Date(o.date_won || o.date_updated);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
    mrrByMonth[key] = (mrrByMonth[key] || 0) + o.monthly_value;
  }

  const result = {
    kpis: {
      total_leads: leads.length,
      active_opportunities: activeOpps.length,
      pipeline_mrr: Math.round(totalPipelineValue),
      won_mrr: Math.round(totalWonMRR),
      win_rate: winRate,
      avg_deal_size: avgDealSize,
      won_count: wonCount,
      lost_count: lostCount,
      closed_total: closedTotal,
    },
    active_opportunities: activeOpps,
    won_opportunities: wonOpps.slice(0, 50),
    lost_opportunities: lostOpps.slice(0, 50),
    pipeline_by_status: pipelineByStatus,
    mrr_by_month: mrrByMonth,
    a2p_breakdown: a2pBreakdown,
    statuses,
    field_ids: { a2p: a2pFieldId, pipeline_review: pipelineFieldId },
    updated_at: new Date().toISOString(),
  };

  setCache('messagedesk', result);
  return result;
}

// ─── Duet ──────────────────────────────────────────────────────────────────────
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
const DUET_OWNER_MAP = {
  '163553901': 'Jonathan Goldberg',
  '163553854': 'Florencia Scopp',
  '83189293':  'Joe',
  '163575365': 'Jonathan Goldberg',
};
const ownerCache = {};

async function resolveOwner(id) {
  if (!id) return 'Unknown';
  if (DUET_OWNER_MAP[id]) return DUET_OWNER_MAP[id];
  if (ownerCache[id]) return ownerCache[id];
  try {
    const data = await hsFetch(`/crm/v3/owners/${id}`);
    const name = [data.firstName, data.lastName].filter(Boolean).join(' ') || data.email || id;
    ownerCache[id] = name;
    return name;
  } catch { ownerCache[id] = id; return id; }
}

const DUET_PIPELINE_ID = '2168635108';
const DUET_PROPS = [
  'dealname','dealstage','pipeline','hubspot_owner_id','closedate',
  'hs_lastmodifieddate','attribution_2024_lives','gross_savings_2024_deal',
  'outreach_attempt_count','last_outreach_date','meeting_date','loi_sent_date',
  'loi_signed_date','enrollment_date','enrollment_deadline','champion_name',
  'champion_role','lost_reason','deal_source','duet_engaged_owner',
  'secondary_owner','meeting_set','np_intro_made',
].join(',');

async function fetchDuetDeals() {
  const cached = getCache('duet');
  if (cached) return cached;

  const deals = [];
  let after = null;
  while (true) {
    const body = {
      filterGroups: [{ filters: [{ propertyName: 'pipeline', operator: 'EQ', value: DUET_PIPELINE_ID }] }],
      properties: DUET_PROPS.split(','),
      limit: 100,
    };
    if (after) body.after = after;
    const data = await hsPost('/crm/v3/objects/deals/search', body);
    deals.push(...(data.results || []));
    if (data.paging && data.paging.next && data.paging.next.after) {
      after = data.paging.next.after;
    } else break;
  }

  const ownerIds = [...new Set(deals.map(d => d.properties?.hubspot_owner_id).filter(Boolean))];
  await Promise.all(ownerIds.map(id => resolveOwner(id)));

  const mapped = deals.map(d => {
    const p = d.properties || {};
    const stageId = p.dealstage || '';
    const ownerId = p.hubspot_owner_id || '';
    return {
      id: d.id,
      dealname: p.dealname || '',
      stage_id: stageId,
      stage: DUET_STAGE_MAP[stageId] || stageId,
      owner: DUET_OWNER_MAP[ownerId] || ownerCache[ownerId] || ownerId,
      owner_id: ownerId,
      closedate: p.closedate || null,
      last_modified: p.hs_lastmodifieddate || null,
      lives: parseFloat(p.attribution_2024_lives) || 0,
      gross_savings: parseFloat(p.gross_savings_2024_deal) || 0,
      outreach_attempts: parseInt(p.outreach_attempt_count) || 0,
      last_outreach: p.last_outreach_date || null,
      meeting_date: p.meeting_date || null,
      loi_sent_date: p.loi_sent_date || null,
      loi_signed_date: p.loi_signed_date || null,
      enrollment_date: p.enrollment_date || null,
      enrollment_deadline: p.enrollment_deadline || null,
      champion_name: p.champion_name || '',
      champion_role: p.champion_role || '',
      lost_reason: p.lost_reason || '',
      deal_source: p.deal_source || '',
      meeting_set: p.meeting_set || '',
      np_intro_made: p.np_intro_made || '',
    };
  });

  const result = { deals: mapped, updated_at: new Date().toISOString() };
  setCache('duet', result);
  return result;
}

// ─── Routes ────────────────────────────────────────────────────────────────────
app.get('/api/messagedesk/dashboard', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['messagedesk'];
    res.json(await fetchMessageDeskDashboard());
  } catch (e) {
    console.error('MessageDesk error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/duet/deals', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['duet'];
    res.json(await fetchDuetDeals());
  } catch (e) {
    console.error('Duet error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Parasol Hub running on http://localhost:${PORT}`));
