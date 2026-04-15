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
  const note = $("#inject-note");
  const count = parseInt($("#inject-count").value) || 100000;
  const t0 = performance.now();

  // 기대 소요 힌트: 100k 는 ~1-2s, 1M 는 ~3-5s. 사용자가 멈춘 게 아님을 알도록.
  const expect = count <= 200000 ? "예상 1–2초" : "예상 3–6초";

  btn.disabled = true;
  btn.innerHTML = `<span class="btn-loading">주입 중...<span class="spinner"></span></span>`;
  note.className = "inline-note is-active";
  note.innerHTML = `<span class="spinner"></span> ${count.toLocaleString()}건 생성 중 · ${expect}`;
  log(`더미 결제 데이터 ${count.toLocaleString()}건 주입 시작 (${expect})...`);

  // 경과 시간 실시간 업데이트 (1 tick = 200ms)
  const tick = setInterval(() => {
    const ms = Math.round(performance.now() - t0);
    note.innerHTML = `<span class="spinner"></span> ${count.toLocaleString()}건 생성 중 · 경과 ${ms.toLocaleString()}ms`;
  }, 200);

  try {
    const data = await api("/api/inject", { count });
    if (data.error) {
      log(`오류: ${data.error}`);
      note.innerHTML = `⚠ 오류`;
    } else {
      const mode = data.mode ? ` [${data.mode}]` : "";
      log(`완료: ${data.count.toLocaleString()}건 주입 (${data.elapsed_ms}ms)${mode}`);
      note.innerHTML = `✓ ${data.count.toLocaleString()}건 · ${data.elapsed_ms}ms${mode}`;
      const st = $("#inject-status");
      st.style.display = "inline-flex";
      st.textContent = `${data.count.toLocaleString()}건 주입 완료 — ${data.elapsed_ms}ms`;
    }
  } catch (e) {
    log(`네트워크 오류: ${e.message}`);
    note.innerHTML = `⚠ 네트워크 오류`;
  } finally {
    clearInterval(tick);
    btn.disabled = false;
    btn.textContent = "Inject data";
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

  // 장애 건수 집계 (status = FAIL / TIMEOUT) — 발표 시연 핵심 포인트
  let failCount = 0, timeoutCount = 0;
  if (data.rows) {
    for (const r of data.rows) {
      if (r.status === "FAIL") failCount++;
      else if (r.status === "TIMEOUT") timeoutCount++;
    }
  }

  let failSummary = "";
  if (failCount || timeoutCount) {
    failSummary = ` &nbsp;·&nbsp; <span style="color:var(--red);font-weight:600">⚠ 장애 ${failCount + timeoutCount}건</span>` +
      (failCount ? ` <span style="color:var(--grey-20)">FAIL ${failCount}</span>` : "") +
      (timeoutCount ? ` <span style="color:var(--grey-20)">TIMEOUT ${timeoutCount}</span>` : "");
  }

  let html = `<div class="result-header">
    id ${data.lo}~${data.hi} &nbsp;·&nbsp; ${data.row_count}건 &nbsp;·&nbsp; ${badge}${failSummary}
  </div>`;

  if (data.rows && data.rows.length > 0) {
    const cols = Object.keys(data.rows[0]);
    // light 테이블 (table-wrap): 일반 행은 검은 글씨로 가독성 확보.
    // 장애 행만 .row-fail / .row-timeout 로 대비를 줌.
    html += '<div class="table-wrap"><table><thead><tr>';
    for (const c of cols) html += `<th>${c}</th>`;
    html += '</tr></thead><tbody>';
    for (const row of data.rows.slice(0, 50)) {
      let cls = "";
      if (row.status === "FAIL") cls = "row-fail";
      else if (row.status === "TIMEOUT") cls = "row-timeout";
      html += cls ? `<tr class="${cls}">` : "<tr>";
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
  btn.innerHTML = `<span class="btn-loading">비교 중...<span class="spinner"></span></span>`;
  log(`선형 vs 인덱스 비교: id ${lo} ~ ${hi}`);

  try {
    const data = await api("/api/compare", { lo, hi });
    if (data.index_error || data.linear_error) {
      log(`오류: ${data.index_error || data.linear_error}`);
    } else {
      log(`인덱스: ${data.index_ms}ms / 선형: ${data.linear_ms}ms / ${data.speedup}x 빠름`);
      data.lo = lo; data.hi = hi;
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

  const K = data.hi - data.lo + 1;
  const saved = Math.round(data.linear_ms - data.index_ms);

  $("#compare-summary").innerHTML =
    `<div style="margin-top:24px">
       <div class="big-number">${data.speedup}x</div>
       <div class="big-label">인덱스 ${data.index_ms}ms &middot; 선형 ${data.linear_ms}ms</div>
       <div class="chart-caption">
         <b>연산:</b>
         <span class="formula">${data.linear_ms}ms</span> ÷
         <span class="formula">${data.index_ms}ms</span> =
         <span class="formula">${data.speedup}×</span>
         &nbsp;→&nbsp; 장애 구간 <b>${K.toLocaleString()}건</b> 뽑는 데 <b>${saved.toLocaleString()}ms</b> 절약.
         <br/>
         <span style="color:var(--grey-40)">선형은 CSV 전체를 스캔하지만, B+ 트리는
         <span class="formula">O(log n + k)</span> 로 필요한 행만 fseek 해 읽는다.
         K (반환 행 수) 가 클수록 배율은 줄고, 전체의 ~50%를 넘으면 역전된다.</span>
       </div>
     </div>`;

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
