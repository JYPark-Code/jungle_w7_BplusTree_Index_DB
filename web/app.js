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
      (failCount ? ` <span style="color:var(--black)">FAIL ${failCount}</span>` : "") +
      (timeoutCount ? ` <span style="color:var(--black)">TIMEOUT ${timeoutCount}</span>` : "");
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

  // SQL end-to-end + 자료구조 순수 (bench) 를 병렬로 요청.
  // bench 는 ~1s 소요라 UX 상 함께 돌리는 게 자연스러움.
  try {
    const [sqlData, benchData] = await Promise.all([
      api("/api/compare", { lo, hi }),
      api("/api/bench_compare", {}),
    ]);

    if (sqlData.index_error || sqlData.linear_error) {
      log(`SQL 오류: ${sqlData.index_error || sqlData.linear_error}`);
    } else {
      log(`SQL 인덱스: ${sqlData.index_ms}ms / 선형: ${sqlData.linear_ms}ms / ${sqlData.speedup}x`);
      sqlData.lo = lo; sqlData.hi = hi;
      renderChart(sqlData);
    }

    if (benchData && benchData.error) {
      log(`Bench 오류: ${benchData.error}`);
    } else if (benchData) {
      log(`Bench 인덱스: ${benchData.index_s?.toFixed(3)}s / 선형: ${benchData.linear_s?.toFixed(3)}s / ${benchData.speedup?.toFixed(1)}x`);
      renderBenchChart(benchData);
    }

    if (sqlData && benchData && !sqlData.index_error && !benchData.error) {
      renderCompareDelta(sqlData, benchData, lo, hi);
    }
  } catch (e) {
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "선형 vs 인덱스 비교";
  }
});

let benchChart = null;

const _chartOpts = (title, unit) => ({
  responsive: true,
  plugins: {
    legend: { display: false },
    title: {
      display: true, text: title, color: "#fff",
      font: { size: 13, weight: 600, family: "'Inter', sans-serif" },
    },
  },
  scales: {
    y: {
      beginAtZero: true,
      title: { display: true, text: unit, color: "#7c7d82" },
      ticks: { color: "#7c7d82" },
      grid: { color: "rgba(255,255,255,0.06)" },
    },
    x: {
      ticks: { color: "#eeeff2", font: { weight: 500 } },
      grid: { display: false },
    },
  },
});

function renderChart(data) {
  const ctx = $("#chart").getContext("2d");
  if (chart) chart.destroy();
  const area = $("#chart-area");
  if (area) area.style.display = "block";

  $("#sql-summary").innerHTML = `
    <div class="big-number">${data.speedup}×</div>
    <div class="big-label">인덱스 ${data.index_ms}ms · 선형 ${data.linear_ms}ms</div>
  `;

  chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: ["선형 (status)", "인덱스 (BETWEEN)"],
      datasets: [{
        label: "ms",
        data: [data.linear_ms, data.index_ms],
        backgroundColor: ["#d5001c", "#0e0e12"],
        borderRadius: 4,
      }],
    },
    options: _chartOpts(`SQL 레벨 — ${data.speedup}× 단축`, "ms"),
  });
}

function renderBenchChart(b) {
  const ctx = $("#chart-bench").getContext("2d");
  if (benchChart) benchChart.destroy();

  const linear_ms = (b.linear_s ?? 0) * 1000;
  const index_ms  = (b.index_s  ?? 0) * 1000;
  const speedup   = b.speedup ?? 0;

  $("#bench-summary").innerHTML = `
    <div class="big-number">${speedup.toFixed(0)}×</div>
    <div class="big-label">인덱스 ${index_ms.toFixed(1)}ms · 선형 ${linear_ms.toFixed(1)}ms
      <span style="color:var(--grey-40);font-size:12px">· N=${(b.n||0).toLocaleString()}, M=${(b.m||0).toLocaleString()}</span></div>
  `;

  benchChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels: ["선형 flat array", "bptree_search"],
      datasets: [{
        label: "ms",
        data: [linear_ms, index_ms],
        backgroundColor: ["#d5001c", "#0e0e12"],
        borderRadius: 4,
      }],
    },
    options: _chartOpts(`자료구조 레벨 — ${speedup.toFixed(0)}× 단축`, "ms"),
  });
}

function renderCompareDelta(sql, bench, lo, hi) {
  const K = hi - lo + 1;
  const saved = Math.round(sql.linear_ms - sql.index_ms);
  $("#compare-delta").innerHTML = `
    <b>두 배율이 다른 이유.</b>
    왼쪽 <b>${sql.speedup}×</b> 는 SQL 한 번의 왕복 시간 — 매 질의마다
    <span class="formula">subprocess spawn</span> +
    <span class="formula">ensure_index rebuild (~1.8s)</span> +
    <span class="formula">파일 I/O</span> 를 다시 지불하기 때문에
    자료구조 이득이 희석된다.<br/>
    오른쪽 <b>${(bench.speedup ?? 0).toFixed(0)}×</b> 는 <span class="formula">make bench</span>
    의 <span class="formula">bptree_search()</span> 를 인-프로세스에서 N=${(bench.n||0).toLocaleString()}건에 대해
    M=${(bench.m||0).toLocaleString()}회 호출한 결과 — 알고리즘 순수 경쟁.
    PostgreSQL 같은 영속 데몬이면 SQL 레벨도 이 숫자에 수렴한다.<br/>
    <span style="color:var(--grey-40)">현재 질의: 장애 구간 <b style="color:var(--white)">${K.toLocaleString()}건</b> 추출, SQL 레벨에서 <b style="color:var(--white)">${saved.toLocaleString()}ms</b> 절약.</span>
  `;
}

/* ── 4) B+ 트리 해부 — bptree_print 라이브 ────────────────── */

$("#btn-tree").addEventListener("click", async () => {
  const btn = $("#btn-tree");
  const n = parseInt($("#tree-n").value) || 20;
  const order = parseInt($("#tree-order").value) || 4;
  const out = $("#tree-output");
  btn.disabled = true;
  btn.innerHTML = `<span class="btn-loading">실행 중...<span class="spinner"></span></span>`;
  out.classList.remove("placeholder");
  out.textContent = "./tree_shape 실행 중...";
  log(`tree_shape 실행: N=${n}, order=${order}`);

  try {
    const data = await api("/api/tree_shape", { n, order });
    if (data.error) {
      out.textContent = `오류: ${data.error}\n${data.stderr || ""}`;
      log(`오류: ${data.error}`);
    } else {
      out.textContent = data.output || "(출력 없음)";
      log(`tree_shape 완료: N=${data.n}, order=${data.order}`);
    }
  } catch (e) {
    out.textContent = `네트워크 오류: ${e.message}`;
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "Print tree";
  }
});
