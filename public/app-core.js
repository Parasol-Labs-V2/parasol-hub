/* ─── Utils ─────────────────────────────────────────────────────────────────── */
function fmt$(n) {
  if (!n && n !== 0) return '$0';
  if (n >= 1000000) return '$' + (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000)    return '$' + (n / 1000).toFixed(1) + 'K';
  return '$' + Math.round(n).toLocaleString();
}
function fmtNum(n) {
  if (!n && n !== 0) return '0';
  if (n >= 1000000) return (n / 1000000).toFixed(1) + 'M';
  if (n >= 1000)    return (n / 1000).toFixed(1) + 'K';
  return Math.round(n).toLocaleString();
}
function fmtDate(d) {
  if (!d) return '—';
  const dt = new Date(d);
  if (isNaN(dt)) return '—';
  return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}
function ageDays(dateStr) {
  if (!dateStr) return '—';
  const d = Math.floor((Date.now() - new Date(dateStr).getTime()) / 86400000);
  return d + 'd';
}
function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function isNextWeek(dateStr) {
  if (!dateStr) return false;
  const d = new Date(dateStr);
  const now = Date.now();
  return d.getTime() > now && d.getTime() < now + 7 * 86400000;
}
function isPast14Days(dateStr) {
  if (!dateStr) return false;
  const d = new Date(dateStr);
  const now = Date.now();
  return d.getTime() < now && d.getTime() > now - 14 * 86400000;
}
function isLast7Days(dateStr) {
  if (!dateStr) return false;
  const d = new Date(dateStr);
  const now = Date.now();
  return d.getTime() > now - 7 * 86400000 && d.getTime() <= now;
}
function exportCsv(rows, filename) {
  const csv = rows.map(r => r.map(c => `"${String(c||'').replace(/"/g,'""')}"`).join(',')).join('\n');
  const a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
  a.download = filename;
  a.click();
}
function makeSortable(tbodyId, headers) {
  headers.forEach((col, i) => {
    col.style.cursor = 'pointer';
    col.addEventListener('click', () => {
      const tbody = document.getElementById(tbodyId);
      if (!tbody) return;
      const asc = col.dataset.asc !== 'true';
      col.dataset.asc = asc;
      const rows = Array.from(tbody.querySelectorAll('tr'));
      rows.sort((a, b) => {
        const av = a.cells[i]?.textContent.trim() || '';
        const bv = b.cells[i]?.textContent.trim() || '';
        const an = parseFloat(av.replace(/[^0-9.-]/g,'')), bn = parseFloat(bv.replace(/[^0-9.-]/g,''));
        if (!isNaN(an) && !isNaN(bn)) return asc ? an - bn : bn - an;
        return asc ? av.localeCompare(bv) : bv.localeCompare(av);
      });
      rows.forEach(r => tbody.appendChild(r));
    });
  });
}

window.ParasolUtils = { fmt$, fmtNum, fmtDate, ageDays, escHtml, isNextWeek, isPast14Days, isLast7Days, exportCsv, makeSortable };

/* ─── Duet loader ────────────────────────────────────────────────────────────── */
let duetData = null;

async function loadDuet(force = false) {
  const app = document.getElementById('duet-app');
  if (!app) return;
  
  app.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:300px;flex-direction:column;gap:12px;color:#888;">
    <div style="width:36px;height:36px;border:3px solid #eee;border-top-color:#E8231A;border-radius:50%;animation:spin 0.8s linear infinite;"></div>
    <div>Loading Duet data...</div>
    <style>@keyframes spin{to{transform:rotate(360deg)}}</style>
  </div>`;

  try {
    const url = force ? '/api/duet/deals?refresh=1' : '/api/duet/deals';
    const res = await fetch(url);
    if (!res.ok) throw new Error('API error: ' + res.status);
    duetData = await res.json();
    renderDuetDashboard(duetData);
  } catch(e) {
    app.innerHTML = `<div style="padding:20px;color:#E8231A;">Error loading Duet data: ${e.message}</div>`;
  }
}

function renderDuetDashboard(data) {
  const app = document.getElementById('duet-app');
  if (!app) return;

  app.innerHTML = `
    <div style="background:white;border-bottom:1px solid #eee;padding:0 24px;display:flex;gap:0;position:sticky;top:63px;z-index:99;">
      ${['Active Pipeline','Funnel Overview','Meetings Next Week','WoW Changes','Pipeline Review','All Deals'].map((t,i) => 
        `<div class="duet-tab ${i===0?'active':''}" onclick="switchDuetTab(${i})" data-tab="${i}" style="padding:12px 20px;cursor:pointer;border-bottom:3px solid ${i===0?'#E8231A':'transparent'};font-weight:500;color:${i===0?'#E8231A':'#888'};font-size:0.9rem;white-space:nowrap;">${t}</div>`
      ).join('')}
    </div>
    <div style="padding:20px 24px 40px;max-width:1600px;margin:0 auto;">
      <div id="duet-tab-content"></div>
    </div>
    <div style="text-align:center;padding:20px;color:#aaa;font-size:0.8rem;">Built by Parasol</div>
  `;

  switchDuetTab(0);
}

function switchDuetTab(idx) {
  document.querySelectorAll('.duet-tab').forEach((t, i) => {
    t.style.borderBottomColor = i === idx ? '#E8231A' : 'transparent';
    t.style.color = i === idx ? '#E8231A' : '#888';
  });
  const content = document.getElementById('duet-tab-content');
  if (!content || !duetData) return;
  const deals = duetData.deals || [];
  const fns = [
    () => window.renderDuetTab1(deals),
    () => window.renderDuetTab2(deals),
    () => window.renderDuetTab3(deals),
    () => window.renderDuetTab4(deals),
    () => window.renderDuetTab5(deals),
    () => window.renderDuetTab6(deals),
  ];
  content.innerHTML = fns[idx] ? fns[idx]() : '<p>Coming soon</p>';
}
