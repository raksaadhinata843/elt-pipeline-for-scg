import { MDConnection } from '@motherduck/motherduck-duckdb-wasm';
import Chart from 'chart.js/auto';

const TOKEN = import.meta.env.VITE_MOTHERDUCK_TOKEN;
const DB    = 'my_db';

// ── DOM refs ────────────────────────────────────────────────────────────────
const badge     = document.getElementById('status-badge');
const valCement = document.getElementById('val-cement');
const valCoal   = document.getElementById('val-coal');
const valOil    = document.getElementById('val-oil');
const valGap    = document.getElementById('val-gap');
const subCement = document.getElementById('sub-cement');
const subCoal   = document.getElementById('sub-coal');
const subOil    = document.getElementById('sub-oil');
const subGap    = document.getElementById('sub-gap');

function setStatus(text, state = 'loading') {
  badge.textContent = text;
  badge.className   = `badge ${state}`;
}

// ── Arrow → plain-JS rows ───────────────────────────────────────────────────
function toRows(arrowTable) {
  const rows = [];
  for (let i = 0; i < arrowTable.numRows; i++) {
    const row = {};
    for (const field of arrowTable.schema.fields) {
      const col = arrowTable.getChild(field.name);
      const raw = col.get(i);
      // Arrow timestamps come back as BigInt (ms since epoch)
      row[field.name] = typeof raw === 'bigint' ? Number(raw) : raw;
    }
    rows.push(row);
  }
  return rows;
}

// ── Date formatter ──────────────────────────────────────────────────────────
function fmtMonth(val) {
  if (val == null) return '';
  const ms   = typeof val === 'number' ? val : new Date(val).getTime();
  const d    = new Date(ms);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
}

function fmtNum(n) {
  return n != null ? n.toFixed(1) : '—';
}

// ── Chart helpers ───────────────────────────────────────────────────────────
const PALETTE = {
  cement : { line: '#4f86c6', fill: 'rgba(79,134,198,0.08)'  },
  coal   : { line: '#e8a838', fill: 'rgba(232,168,56,0.08)'  },
  oil    : { line: '#e05c5c', fill: 'rgba(224,92,92,0.08)'   },
  total  : { line: '#6c63ff', fill: 'rgba(108,99,255,0.08)'  },
};

function lineDataset(label, data, color, borderDash = []) {
  return {
    label,
    data,
    borderColor      : color.line,
    backgroundColor  : color.fill,
    borderWidth      : 2,
    borderDash,
    tension          : 0.3,
    fill             : false,
    pointRadius      : 0,
    pointHoverRadius : 4,
  };
}

const sharedOptions = {
  responsive         : true,
  maintainAspectRatio: false,
  interaction        : { mode: 'index', intersect: false },
  plugins: {
    legend: { position: 'top', labels: { boxWidth: 12, color: '#ccc' } },
    tooltip: {
      backgroundColor : 'rgba(20,22,30,0.92)',
      titleColor      : '#e0e0e0',
      bodyColor       : '#ccc',
      borderColor     : '#333',
      borderWidth     : 1,
    },
  },
  scales: {
    x: {
      ticks: { maxTicksLimit: 10, color: '#888' },
      grid : { color: 'rgba(255,255,255,0.05)' },
    },
    y: {
      ticks: { color: '#888' },
      grid : { color: 'rgba(255,255,255,0.05)' },
    },
  },
};

// ── Main ────────────────────────────────────────────────────────────────────
async function main() {
  if (!TOKEN) {
    setStatus('No token — set MOTHERDUCK_TOKEN', 'error');
    return;
  }

  try {
    setStatus('Connecting to MotherDuck…', 'loading');
    const conn = await MDConnection.create({ mdToken: TOKEN });

    setStatus('Loading gold tables…', 'loading');

    const [indexRes, gapRes] = await Promise.all([
      conn.evaluateQuery(`
        SELECT obs_month, cement_idx, coal_idx, oil_idx
        FROM ${DB}.gold_indexed
        ORDER BY obs_month
      `),
      conn.evaluateQuery(`
        SELECT obs_month, gap_coal, gap_oil, gap_total_energy
        FROM ${DB}.gold_gap
        ORDER BY obs_month
      `),
    ]);

    const indexRows = toRows(indexRes.data);
    const gapRows   = toRows(gapRes.data);

    // ── Stat cards ───────────────────────────────────────────────────────
    const latest    = indexRows.at(-1);
    const latestGap = gapRows.at(-1);

    valCement.textContent = fmtNum(latest?.cement_idx);
    valCoal.textContent   = fmtNum(latest?.coal_idx);
    valOil.textContent    = fmtNum(latest?.oil_idx);
    valGap.textContent    = fmtNum(latestGap?.gap_total_energy);

    const latestMonth = fmtMonth(latest?.obs_month);
    subCement.textContent = latestMonth;
    subCoal.textContent   = latestMonth;
    subOil.textContent    = latestMonth;
    subGap.textContent    = `as of ${latestMonth}`;

    // ── Index chart ──────────────────────────────────────────────────────
    const labels = indexRows.map(r => fmtMonth(r.obs_month));
    new Chart(document.getElementById('indexChart'), {
      type: 'line',
      data: {
        labels,
        datasets: [
          lineDataset('Cement', indexRows.map(r => r.cement_idx), PALETTE.cement),
          lineDataset('Coal',   indexRows.map(r => r.coal_idx),   PALETTE.coal),
          lineDataset('Oil',    indexRows.map(r => r.oil_idx),    PALETTE.oil),
        ],
      },
      options: {
        ...sharedOptions,
        scales: {
          ...sharedOptions.scales,
          y: {
            ...sharedOptions.scales.y,
            title: { display: true, text: 'Index (Jan 2010 = 100)', color: '#888' },
          },
        },
      },
    });

    // ── Gap chart ─────────────────────────────────────────────────────────
    const gapLabels = gapRows.map(r => fmtMonth(r.obs_month));
    new Chart(document.getElementById('gapChart'), {
      type: 'line',
      data: {
        labels: gapLabels,
        datasets: [
          lineDataset('Coal Gap',         gapRows.map(r => r.gap_coal),         PALETTE.coal),
          lineDataset('Oil Gap',          gapRows.map(r => r.gap_oil),          PALETTE.oil),
          lineDataset('Total Energy Gap', gapRows.map(r => r.gap_total_energy), PALETTE.total),
        ],
      },
      options: {
        ...sharedOptions,
        scales: {
          ...sharedOptions.scales,
          y: {
            ...sharedOptions.scales.y,
            title: { display: true, text: 'Index Difference', color: '#888' },
          },
        },
      },
    });

    setStatus(`✓ ${indexRows.length} monthly records`, 'ok');
  } catch (err) {
    console.error(err);
    setStatus(`Error: ${err.message}`, 'error');
  }
}

main();
