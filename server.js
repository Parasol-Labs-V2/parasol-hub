require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const path = require('path');

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// MessageDesk — proxy to standalone dashboard
app.get('/api/messagedesk/dashboard', async (req, res) => {
  try {
    const r = await axios.get('https://messagedesk-dashboard.vercel.app/api/dashboard', { timeout: 25000 });
    res.json(r.data);
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Duet — HubSpot
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HS_BASE = 'https://api.hubapi.com';
const DUET_PIPELINE_ID = '2168635108';

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
  '83189293': 'Joe',
  '163575365': 'Jonathan Goldberg',
  '163553855': 'Blair',
};

const cache = {};
function getCache(key) {
  const e = cache[key];
  if (!e) return null;
  if (Date.now() - e.ts > 5 * 60 * 1000) return null;
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

async function fetchDuetDeals() {
  const cached = getCache('duet');
  if (cached) return cached;

  const deals = [];
  let after = null;
  const PROPS = 'dealname,dealstage,pipeline,hubspot_owner_id,closedate,hs_lastmodifieddate,attribution_2024_lives,gross_savings_2024_deal,outreach_attempt_count,last_outreach_date,meeting_date,loi_sent_date,loi_signed_date,enrollment_date,enrollment_deadline,champion_name,champion_role,lost_reason,deal_source,duet_engaged_owner,secondary_owner,meeting_set,np_intro_made';

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
    if (r.data.paging && r.data.paging.next && r.data.paging.next.after) {
      after = r.data.paging.next.after;
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
      lives: parseFloat(p.attribution_2024_lives) || 0,
      gross_savings: parseFloat(p.gross_savings_2024_deal) || 0,
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
    };
  });

  const result = { deals: mapped, updated_at: new Date().toISOString() };
  setCache('duet', result);
  return result;
}

app.get('/api/duet/deals', async (req, res) => {
  try {
    if (req.query.refresh === '1') delete cache['duet'];
    res.json(await fetchDuetDeals());
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Parasol Hub running on port ${PORT}`));
