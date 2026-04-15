/* app.js — 결제 데모 프론트엔드 (규태) */

const $ = (sel) => document.querySelector(sel);
const log = (msg) => {
  const el = $("#log");
  const ts = new Date().toLocaleTimeString();
  el.textContent += `[${ts}] ${msg}\n`;
  el.scrollTop = el.scrollHeight;
};

/* ── API 호출 ─────────────────────────────────────────────── */

async function api(path, body = {}) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}

/* ── 1) 더미 주입 ─────────────────────────────────────────── */

$("#btn-inject").addEventListener("click", async () => {
  const btn = $("#btn-inject");
  const count = parseInt($("#inject-count").value) || 100000;
  btn.disabled = true;
  btn.textContent = "주입 중...";
  log(`더미 결제 데이터 ${count.toLocaleString()}건 주입 시작...`);

  try {
    const data = await api("/api/inject", { count });
    if (data.error) {
      log(`오류: ${data.error}`);
    } else {
      log(`완료: ${data.count.toLocaleString()}건 주입 (${data.elapsed_ms}ms)`);
      const st = $("#inject-status");
      st.style.display = "inline-flex";
      st.textContent = `${data.count.toLocaleString()}건 주입 완료 — ${data.elapsed_ms}ms`;
    }
  } catch (e) {
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "더미 주입";
  }
});

/* ── 2) 장애 구간 조회 ────────────────────────────────────── */

$("#btn-range").addEventListener("click", async () => {
  const btn = $("#btn-range");
  const lo = parseInt($("#range-lo").value) || 30000;
  const hi = parseInt($("#range-hi").value) || 31500;
  btn.disabled = true;
  btn.textContent = "조회 중...";
  log(`범위 조회: id ${lo} ~ ${hi}`);

  try {
    const data = await api("/api/range", { lo, hi });
    if (data.error) {
      log(`오류: ${data.error}`);
    } else {
      log(`조회 완료: ${data.row_count}건 (${data.elapsed_ms}ms)`);
      renderRangeResult(data);
    }
  } catch (e) {
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "장애 구간 조회";
  }
});

function renderRangeResult(data) {
  const el = $("#range-result");
  const badge = data.elapsed_ms < 1
    ? `<span class="badge-fast">${data.elapsed_ms}ms</span>`
    : `<span class="badge-ok">${data.elapsed_ms}ms</span>`;

  let html = `<div class="result-header">
    id ${data.lo}~${data.hi} | ${data.row_count}건 | ${badge}
  </div>`;

  if (data.rows && data.rows.length > 0) {
    const cols = Object.keys(data.rows[0]);
    html += '<div class="table-wrap"><table><thead><tr>';
    for (const c of cols) html += `<th>${c}</th>`;
    html += '</tr></thead><tbody>';
    for (const row of data.rows.slice(0, 50)) {
      html += '<tr>';
      for (const c of cols) html += `<td>${row[c] || ""}</td>`;
      html += '</tr>';
    }
    html += '</tbody></table></div>';
    if (data.row_count > 50) {
      html += `<div class="more">... 외 ${data.row_count - 50}건</div>`;
    }
  }
  el.innerHTML = html;
}

/* ── 3) 선형 vs 인덱스 비교 ──────────────────────────────── */

let chart = null;

$("#btn-compare").addEventListener("click", async () => {
  const btn = $("#btn-compare");
  const lo = parseInt($("#range-lo").value) || 30000;
  const hi = parseInt($("#range-hi").value) || 31500;
  btn.disabled = true;
  btn.textContent = "비교 중...";
  log(`선형 vs 인덱스 비교: id ${lo} ~ ${hi}`);

  try {
    const data = await api("/api/compare", { lo, hi });
    if (data.index_error || data.linear_error) {
      log(`오류: ${data.index_error || data.linear_error}`);
    } else {
      log(`인덱스: ${data.index_ms}ms / 선형: ${data.linear_ms}ms / ${data.speedup}x 빠름`);
      renderChart(data);
    }
  } catch (e) {
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "선형 vs 인덱스 비교";
  }
});

function renderChart(data) {
  const ctx = $("#chart").getContext("2d");
  if (chart) chart.destroy();
  const area = $("#chart-area");
  if (area) area.style.display = "block";

  $("#compare-summary").innerHTML =
    `<div style="margin-top:24px">` +
    `<div class="big-number">${data.speedup}x</div>` +
    `<div class="big-label">` +
    `인덱스 ${data.index_ms}ms · 선형 ${data.linear_ms}ms</div></div>`;

  chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: ["선형 탐색 (status)", "인덱스 검색 (id range)"],
      datasets: [{
        label: "소요 시간 (ms)",
        data: [data.linear_ms, data.index_ms],
        backgroundColor: ["#d5001c", "#0e0e12"],
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      plugins: {
        legend: { display: false },
        title: {
          display: true,
          text: `선형 vs 인덱스 — ${data.speedup}x 단축`,
          color: "#fff",
          font: { size: 14, weight: 600, family: "'Inter', sans-serif" },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: "ms", color: "#7c7d82" },
          ticks: { color: "#7c7d82" },
          grid: { color: "rgba(255,255,255,0.06)" },
        },
        x: {
          ticks: { color: "#eeeff2", font: { weight: 500 } },
          grid: { display: false },
        },
      },
    },
  });
}
