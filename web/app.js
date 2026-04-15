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
    btn.textContent = "데이터 주입";
  }
});

/* ── Range 입력 → Compare 섹션 연동 표시 ──────────────────── */

function updateCompareRangeBadge() {
  const lo = parseInt($("#range-lo").value) || 0;
  const hi = parseInt($("#range-hi").value) || 0;
  const k = Math.max(0, hi - lo + 1);
  const disp = $("#compare-range-display");
  const kd = $("#compare-k-display");
  if (disp) disp.textContent = `${lo.toLocaleString()} ~ ${hi.toLocaleString()}`;
  if (kd) kd.textContent = k.toLocaleString();
}
$("#range-lo").addEventListener("input", updateCompareRangeBadge);
$("#range-hi").addEventListener("input", updateCompareRangeBadge);
document.addEventListener("DOMContentLoaded", updateCompareRangeBadge);
updateCompareRangeBadge();

/* ── 2) 장애 구간 조회 ────────────────────────────────────── */

$("#btn-range").addEventListener("click", async () => {
  const btn = $("#btn-range");
  const lo = parseInt($("#range-lo").value) || 30000;
  const hi = parseInt($("#range-hi").value) || 31500;
  btn.disabled = true;
  btn.innerHTML = `<span class="btn-loading">조회 중<span class="spinner"></span></span>`;
  log(`범위 조회: id ${lo} ~ ${hi}`);

  // β — 옆 트리 일러스트에서 루트 → 내부 → 리프 경로 하이라이트
  animateRangeTreePath();

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

/* β — Range Query 카드 우측의 고정 트리 일러스트 path highlight */
function animateRangeTreePath() {
  const svg = document.querySelector("#range-tree-svg");
  if (!svg) return;
  // 대상: 루트 → 왼쪽 internal → 중간 leaf (예시 경로)
  const targets = [
    { node: "#rn-root", edge: null,           delay: 0 },
    { node: "#rn-i1",   edge: "#edge-R-I1",   delay: 350 },
    { node: "#rn-l2",   edge: "#edge-I1-L2",  delay: 700 },
    // 이어서 linked list 따라가기: l2 → l3
    { node: "#rn-l3",   edge: null,           delay: 1050 },
  ];
  // 초기화
  svg.querySelectorAll(".ping-on").forEach(el => el.classList.remove("ping-on"));
  svg.querySelectorAll(".path-active").forEach(el => el.classList.remove("path-active"));
  for (const t of targets) {
    setTimeout(() => {
      const n = svg.querySelector(t.node);
      if (n) n.classList.add("ping-on");
      if (t.edge) {
        const e = svg.querySelector(t.edge);
        if (e) e.classList.add("path-active");
      }
    }, t.delay);
  }
  // 2초 후 희미하게 유지, 4초 후 원복
  setTimeout(() => {
    svg.querySelectorAll(".ping-on").forEach(el => el.classList.remove("ping-on"));
    svg.querySelectorAll(".path-active").forEach(el => el.classList.remove("path-active"));
  }, 4000);
}

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
  // 부모 컨테이너를 꽉 채우도록 비율 유지 해제 (카드가 왼쪽으로 압축돼 보이던 원인)
  maintainAspectRatio: false,
  plugins: {
    legend: { display: false },
    title: {
      display: true, text: title, color: "#fff",
      font: { size: 14, weight: 600, family: "'Inter', sans-serif" },
      padding: { top: 2, bottom: 14 },
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
  // max-width 클램프 제거 후 is-open 클래스로 표시 제어
  if (area) { area.classList.add("is-open"); area.style.display = "block"; }

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
    <p style="margin-bottom:12px">
      ① <b>${sql.speedup}×</b> 는 SQL 한 번의 왕복 시간. 매 질의마다
      <span class="formula">subprocess</span>
      <span class="formula">ensure_index rebuild ~1.8s</span>
      <span class="formula">파일 I/O</span>
      를 다시 지불해서 자료구조 이득이 희석된다.
    </p>
    <p style="margin-bottom:12px">
      ② <b>${(bench.speedup ?? 0).toFixed(0)}×</b> 는 <span class="formula">make bench</span>
      의 <span class="formula">bptree_search()</span> 를 인-프로세스에서
      N=${(bench.n||0).toLocaleString()} · M=${(bench.m||0).toLocaleString()}회 호출한 결과.
      알고리즘 순수 경쟁.
    </p>
    <p style="color:var(--grey-40); font-size:12.5px">
      PostgreSQL 같은 영속 데몬이면 SQL 레벨도 ②에 수렴.<br/>
      <span style="color:var(--grey-20)">이번 질의: ID ${lo.toLocaleString()}~${hi.toLocaleString()},
      장애 구간 <b style="color:var(--white)">${K.toLocaleString()}건</b> 추출 시
      SQL 경로가 <b style="color:var(--white)">${saved.toLocaleString()}ms</b> 절약.</span>
    </p>
  `;
}

/* ── 4) B+ 트리 해부 — bptree_print 출력 → SVG 파싱 렌더 ──── */

/* bptree_print 텍스트를 파싱해 트리 구조로 변환.
 * 예시 입력:
 *   BPTree(order=4):
 *     INT[11]
 *       INT[3, 5, 8]
 *         LEAF[1->12, 2->8] ->
 *         LEAF[3->13, 4->3] ->
 *         ...
 *       INT[13]
 *         LEAF[11->0, 12->10] ->
 *         ...
 *
 * 반환: { type, keys: [...], children: [...] } 재귀 구조
 */
function parseBptree(text) {
  const lines = text.split("\n").filter(l => l.trim().length > 0);
  const nodes = [];  // {depth, type, keys}
  for (const raw of lines) {
    if (raw.startsWith("BPTree(") || raw.startsWith("==")) continue;
    const m = raw.match(/^(\s*)(INT|LEAF)\[(.*?)\](\s*->)?$/);
    if (!m) continue;
    const depth = Math.floor(m[1].length / 2);  // 2-space indent
    const type = m[2];
    const inner = m[3];
    let keys;
    if (type === "LEAF") {
      keys = inner.split(",").map(s => {
        const kv = s.trim().split("->");
        return kv[0].trim();
      });
    } else {
      keys = inner.split(",").map(s => s.trim());
    }
    nodes.push({ depth, type, keys });
  }
  // depth 기반으로 트리 재구성 (stack)
  // 주의: pop 조건은 "len >= depth" 여야 형제/조카 관계가 올바름.
  //   (같은 depth 가 이미 stack 에 있으면 그것부터 pop 해야 함)
  const stack = [];
  let root = null;
  for (const n of nodes) {
    const node = { type: n.type, keys: n.keys, children: [] };
    while (stack.length >= n.depth) stack.pop();
    if (stack.length === 0) {
      root = node;
    } else {
      stack[stack.length - 1].children.push(node);
    }
    stack.push(node);
  }
  return root;
}

/* 트리 구조 → SVG. 레벨별로 가로 배치, 같은 레벨 형제는 균등 분포. */
function renderTreeSvg(tree) {
  if (!tree) return "<div style='color:var(--grey-60)'>빈 트리</div>";

  // 1 pass: leaf 좌표 계산 + 노드 좌표
  const NODE_H = 48;
  const NODE_GAP_X = 18;
  const LEVEL_GAP_Y = 80;
  const KEY_W = 28;  // 키 1개당 너비 추정
  const PAD_X = 40, PAD_Y = 40;

  const leaves = [];
  const allNodes = [];
  function collect(node, depth) {
    node.depth = depth;
    node.width = Math.max(60, node.keys.length * KEY_W + 16);
    allNodes.push(node);
    if (node.children.length === 0) leaves.push(node);
    for (const c of node.children) collect(c, depth + 1);
  }
  collect(tree, 0);

  // leaf 들을 좌에서 우로 배치
  let x = PAD_X;
  for (const lf of leaves) {
    lf.x = x;
    x += lf.width + NODE_GAP_X;
  }
  const totalW = x - NODE_GAP_X + PAD_X;

  // 내부 노드: 자식들의 중앙
  function placeInner(node) {
    if (node.children.length === 0) return;
    for (const c of node.children) placeInner(c);
    const first = node.children[0];
    const last = node.children[node.children.length - 1];
    node.x = (first.x + last.x + last.width - node.width) / 2;
  }
  placeInner(tree);

  const maxDepth = Math.max(...allNodes.map(n => n.depth));
  const totalH = PAD_Y + (maxDepth + 1) * LEVEL_GAP_Y + NODE_H + PAD_Y;

  // 엣지 생성 (부모→자식)
  let edges = "";
  for (const n of allNodes) {
    if (!n.children.length) continue;
    const px = n.x + n.width / 2;
    const py = PAD_Y + n.depth * LEVEL_GAP_Y + NODE_H;
    for (const c of n.children) {
      const cx = c.x + c.width / 2;
      const cy = PAD_Y + c.depth * LEVEL_GAP_Y;
      edges += `<path class="tree-edge" d="M${px},${py} C${px},${(py + cy) / 2} ${cx},${(py + cy) / 2} ${cx},${cy}"/>`;
    }
  }
  // 리프 linked list
  let leafLinks = "";
  for (let i = 0; i < leaves.length - 1; i++) {
    const a = leaves[i], b = leaves[i + 1];
    const ay = PAD_Y + a.depth * LEVEL_GAP_Y + NODE_H / 2;
    leafLinks += `<path class="tree-leaf-link" d="M${a.x + a.width},${ay} L${b.x},${ay}"/>`;
  }

  // 노드 렌더 (애니메이션 stagger + data-nid 로 노드 ↔ SVG 매핑)
  let nodeG = "";
  allNodes.forEach((n, i) => {
    n._id = i;
    const y = PAD_Y + n.depth * LEVEL_GAP_Y;
    const cls = n.depth === 0 ? "tree-node-root"
      : n.type === "LEAF" ? "tree-node-leaf"
      : "tree-node-internal";
    const label = n.keys.join(", ");
    nodeG += `<g class="${cls} tree-anim-in" data-nid="${i}" style="animation-delay:${(i * 60)}ms">
      <rect x="${n.x}" y="${y}" width="${n.width}" height="${NODE_H}" rx="6"/>
      <text x="${n.x + n.width / 2}" y="${y + NODE_H / 2 + 5}"
            text-anchor="middle" font-family="'JetBrains Mono',monospace" font-size="12">${label}</text>
    </g>`;
  });

  // search 가 사용할 수 있도록 전역 상태 업데이트 — 렌더 후 DOM 에서 실제 요소를 찾아 Map 생성
  setTimeout(() => {
    _lastRenderedTree = tree;
    const svg = document.querySelector("#tree-svg-wrap svg");
    if (!svg) return;
    _nodeIndexById = new Map();
    function walk(n) {
      const el = svg.querySelector(`g[data-nid="${n._id}"]`);
      if (el) _nodeIndexById.set(n, el);
      for (const c of n.children) walk(c);
    }
    walk(tree);
  }, 10);

  // 줌/팬 위해 전체를 #tree-root g 로 감싸 transform 의 단일 진실원 으로 둔다.
  const w = Math.max(totalW, 600);
  return `<svg id="tree-svg" viewBox="0 0 ${w} ${totalH}"
               preserveAspectRatio="xMidYMid meet"
               width="100%" height="100%">
    <g id="tree-root" transform="translate(0,0) scale(1)">
      ${edges}${leafLinks}${nodeG}
    </g>
  </svg>`;
}

async function buildAndRenderTree({ animate = false } = {}) {
  const btn = $("#btn-tree");
  const playBtn = $("#btn-tree-play");
  const n = parseInt($("#tree-n").value) || 20;
  const order = parseInt($("#tree-order").value) || 4;
  const wrap = $("#tree-svg-wrap");
  const insightOrder = $("#tree-insight-order");
  if (insightOrder) insightOrder.textContent = order;

  btn.disabled = true;
  if (animate && playBtn) playBtn.disabled = true;
  btn.innerHTML = `<span class="btn-loading">생성 중<span class="spinner"></span></span>`;
  wrap.classList.remove("placeholder");
  wrap.innerHTML = `<span style="color:var(--grey-60)">./tree_shape 실행 중 <span class="spinner"></span></span>`;
  log(`tree_shape 실행: N=${n}, order=${order}${animate ? " (애니메이션)" : ""}`);

  try {
    const snapshotsCount = animate ? Math.min(12, Math.max(4, Math.floor(n / 3))) : 0;
    const data = await api("/api/tree_shape", { n, order, snapshots: snapshotsCount });
    if (data.error) {
      wrap.innerHTML = `<pre style="color:#ff6e6e;font-family:var(--mono)">오류: ${data.error}\n${data.stderr || ""}</pre>`;
      wrap.classList.add("placeholder");
      log(`오류: ${data.error}`);
      return;
    }

    if (animate) {
      await playInsertAnimation(data.output || "", wrap);
    } else {
      const tree = parseBptree(data.output || "");
      wrap.innerHTML = renderTreeSvg(tree);
    }
    log(`트리 완성: N=${data.n}, order=${data.order}`);
  } catch (e) {
    wrap.innerHTML = `<span style="color:#ff6e6e">네트워크 오류: ${e.message}</span>`;
    log(`네트워크 오류: ${e.message}`);
  } finally {
    btn.disabled = false;
    if (playBtn) playBtn.disabled = false;
    btn.textContent = "트리 그리기";
  }
}

$("#btn-tree").addEventListener("click", () => buildAndRenderTree({ animate: false }));
$("#btn-tree-play").addEventListener("click", () => buildAndRenderTree({ animate: true }));

/* γ — N 슬라이더 실시간 재생성 (change 이벤트 = 드래그 release) */
$("#tree-n").addEventListener("input", () => {
  $("#tree-n-val").textContent = $("#tree-n").value;
});
$("#tree-n").addEventListener("change", () => buildAndRenderTree({ animate: false }));
$("#tree-order").addEventListener("change", () => {
  const v = parseInt($("#tree-order").value) || 4;
  const insightOrder = $("#tree-insight-order");
  if (insightOrder) insightOrder.textContent = v;
  buildAndRenderTree({ animate: false });
});

/* α — 삽입 애니메이션: snapshots 를 순차 렌더 */
async function playInsertAnimation(raw, wrap) {
  // "=== SNAPSHOT step=K inserted=X ===" 로 split
  const chunks = raw.split(/^===\s*SNAPSHOT\s+step=\d+\s+inserted=(\d+)\s*===\s*$/m);
  // split: [ header, inserted1, snap1, inserted2, snap2, ... ]
  const frames = [];
  for (let i = 1; i < chunks.length; i += 2) {
    const inserted = parseInt(chunks[i]);
    const body = chunks[i + 1] || "";
    frames.push({ inserted, body });
  }
  if (!frames.length) {
    // 스냅샷 없음 → 최종만
    const tree = parseBptree(raw);
    wrap.innerHTML = renderTreeSvg(tree);
    return;
  }
  for (let i = 0; i < frames.length; i++) {
    const f = frames[i];
    const tree = parseBptree(f.body);
    wrap.innerHTML =
      `<div style="width:100%">
        <div style="text-align:center;font-size:12px;color:var(--grey-40);margin-bottom:10px;
                    font-family:var(--mono);letter-spacing:0.05em">
          <b style="color:var(--red)">▶ 삽입 ${f.inserted}건</b>
          <span style="color:var(--grey-60)">· step ${i + 1}/${frames.length}</span>
        </div>
        ${renderTreeSvg(tree)}
      </div>`;
    await new Promise(r => setTimeout(r, i === frames.length - 1 ? 800 : 600));
  }
}

/* 트리 내 특정 key 경로 시각화 — 실제 B+ 트리 탐색 로직 */
let _lastRenderedTree = null;
let _nodeIndexById = null;  // 노드 → SVG g 요소 인덱스

function searchPath(tree, target) {
  /* B+ 트리 탐색 규칙:
   * INT[k0, k1, ..., k_{m-1}] 는 m+1 개 자식을 가리킴.
   * keys[i] 는 "child[i+1] 의 최소 키". 즉:
   *   target <  keys[0] → child[0]
   *   keys[0] <= target < keys[1] → child[1]
   *   ...
   *   target >= keys[m-1] → child[m]
   * 따라서 "target < keys[i] 인 가장 작은 i" 가 목적지. 없으면 last child. */
  const path = [];  // [{node, why}]
  function descend(node) {
    path.push({ node, why: null });
    if (!node.children.length) return;
    const keys = node.keys.map(Number);
    let idx = keys.findIndex(k => target < k);
    if (idx < 0) idx = node.children.length - 1;
    path[path.length - 1].why = idx < keys.length
      ? `${target} < ${keys[idx]} → child[${idx}]`
      : `${target} ≥ ${keys[keys.length - 1]} → child[${idx}] (끝)`;
    descend(node.children[idx]);
  }
  descend(tree);
  const leaf = path[path.length - 1].node;
  const found = leaf.keys.map(Number).includes(target);
  return { path, found, leaf };
}

function formatTrace(target, result) {
  const lines = [];
  lines.push(`<b style="color:var(--red)">bptree_search(${target})</b> 경로:`);
  result.path.forEach((p, i) => {
    const pad = "&nbsp;".repeat(i * 3);
    const typ = p.node.children.length === 0 ? "LEAF" : (i === 0 ? "루트" : "INT");
    const keysStr = p.node.keys.join(", ");
    const arrow = p.why ? `<span style="color:var(--grey-40)"> — ${p.why}</span>` : "";
    lines.push(`${pad}→ ${typ}[${keysStr}]${arrow}`);
  });
  lines.push(`<span style="color:${result.found ? "#00b450" : "var(--red)"};font-weight:600">
    ${result.found ? "✓" : "✗"} id=${target} ${result.found ? "발견" : "없음"}
  </span>`);
  return lines.join("<br/>");
}

$("#btn-tree-search").addEventListener("click", () => {
  const svg = document.querySelector("#tree-svg-wrap svg");
  const trace = $("#search-trace");
  if (!svg || !_lastRenderedTree) {
    if (trace) trace.innerHTML = `<span style="color:var(--grey-60)">먼저 '트리 그리기' 로 트리를 생성해주세요.</span>`;
    log("먼저 트리를 그려주세요");
    return;
  }
  const target = parseInt($("#tree-search-target").value);
  if (!Number.isFinite(target)) {
    if (trace) trace.innerHTML = `<span style="color:var(--red)">유효한 id 를 입력하세요.</span>`;
    return;
  }
  const result = searchPath(_lastRenderedTree, target);

  // 초기화
  svg.querySelectorAll(".tree-highlight, .is-path").forEach(el => {
    el.classList.remove("tree-highlight");
    el.classList.remove("is-path");
  });

  // 노드 및 엣지 stagger 하이라이트
  result.path.forEach((p, i) => {
    setTimeout(() => {
      const el = _nodeIndexById?.get(p.node);
      if (el) el.classList.add("tree-highlight");
      if (i > 0) {
        const prev = _nodeIndexById?.get(result.path[i - 1].node);
        // 엣지는 ID 부여 안 함 — highlight 는 노드 단에서만
      }
    }, i * 450);
  });

  if (trace) trace.innerHTML = formatTrace(target, result);
  log(`탐색: id=${target} → ${result.path.length} 레벨 → ${result.found ? "발견" : "없음"}`);
});

function countLeaves(t) {
  if (!t) return 0;
  if (!t.children.length) return 1;
  return t.children.reduce((s, c) => s + countLeaves(c), 0);
}

/* ── 5) Tree SVG 줌 / 팬 / 카메라 이동 ─────────────────────── */

const _zoom = { s: 1, tx: 0, ty: 0 };

function _treeRoot()    { return document.getElementById("tree-root"); }
function _treeWrap()    { return document.getElementById("tree-svg-wrap"); }
function _treeSvg()     { return document.getElementById("tree-svg"); }

function applyTreeTransform() {
  const root = _treeRoot();
  if (!root) return;
  root.setAttribute("transform",
    `translate(${_zoom.tx},${_zoom.ty}) scale(${_zoom.s})`);
}

/* fit: 트리 BBox 를 컨테이너에 맞춰 90% 영역에 배치 */
function fitTreeView() {
  const root = _treeRoot();
  const wrap = _treeWrap();
  const svg  = _treeSvg();
  if (!root || !wrap || !svg) return;
  // SVG viewBox 좌표계로 BBox 를 받기 위해 transform 임시 reset
  const prev = root.getAttribute("transform");
  root.setAttribute("transform", "translate(0,0) scale(1)");
  let bbox;
  try { bbox = root.getBBox(); }
  catch (e) { root.setAttribute("transform", prev || ""); return; }
  // SVG viewBox 가 차지한 픽셀 폭/높이 계산 (svg.clientWidth = wrap 폭)
  const cw = wrap.clientWidth;
  const ch = wrap.clientHeight;
  if (bbox.width <= 0 || bbox.height <= 0) {
    root.setAttribute("transform", prev || "");
    return;
  }
  // viewBox 좌표는 svg 내부 단위. transform 도 같은 단위.
  // 컨테이너에 맞추려면 SVG 의 viewBox 와 동일 단위에서 scale 만 조정해도 됨
  // (preserveAspectRatio meet 은 svg 자체가 이미 영역에 맞게 그려짐)
  // → 여기서는 1.0 부근에서 살짝 키우는 정도가 자연스럽다.
  const vb = svg.viewBox.baseVal;
  const sx = (vb.width  * 0.95) / bbox.width;
  const sy = (vb.height * 0.95) / bbox.height;
  _zoom.s  = Math.max(0.2, Math.min(sx, sy));
  _zoom.tx = (vb.width  - bbox.width  * _zoom.s) / 2 - bbox.x * _zoom.s;
  _zoom.ty = (vb.height - bbox.height * _zoom.s) / 2 - bbox.y * _zoom.s;
  applyTreeTransform();
}

function zoomTreeIn()  { _zoom.s = Math.min(_zoom.s * 1.3, 20); applyTreeTransform(); }
function zoomTreeOut() { _zoom.s = Math.max(_zoom.s / 1.3, 0.05); applyTreeTransform(); }

/* 노드 SVG element 를 화면 중앙으로 이동 + 자동 확대 */
function panToTreeNode(nodeEl) {
  if (!nodeEl) return;
  const svg = _treeSvg();
  if (!svg) return;
  let bbox;
  try { bbox = nodeEl.getBBox(); } catch (e) { return; }
  const cx = bbox.x + bbox.width / 2;
  const cy = bbox.y + bbox.height / 2;
  const vb = svg.viewBox.baseVal;
  const targetScale = Math.max(2.5, _zoom.s);
  _zoom.s  = targetScale;
  _zoom.tx = vb.width  / 2 - cx * targetScale;
  _zoom.ty = vb.height / 2 - cy * targetScale;
  const root = _treeRoot();
  if (root) {
    /* 노드 펄스 하이라이트(1.6s)와 호흡을 맞추기 위해 ~900ms ease-in-out 으로 천천히 */
    root.style.transition = "transform 900ms cubic-bezier(.4,.0,.2,1)";
    applyTreeTransform();
    setTimeout(() => { root.style.transition = ""; }, 950);
  }
}

/* 트리 그려진 직후 호출 — 자동 fit + 줌/팬 핸들러 활성화 */
function setupTreeZoomHandlers() {
  const wrap = _treeWrap();
  if (!wrap || wrap.dataset.zoomBound === "1") {
    // 이미 바인딩됨 — 새 SVG 가 생겨도 상태만 reset 하면 됨
    return;
  }
  wrap.dataset.zoomBound = "1";

  // 휠 — 마우스 위치 기준 확대
  wrap.addEventListener("wheel", (e) => {
    if (!_treeRoot()) return;
    e.preventDefault();
    const svg = _treeSvg();
    const rect = wrap.getBoundingClientRect();
    const vb = svg.viewBox.baseVal;
    // 마우스 위치를 SVG viewBox 좌표로 변환
    const mx = (e.clientX - rect.left) / rect.width  * vb.width;
    const my = (e.clientY - rect.top)  / rect.height * vb.height;
    const delta = e.deltaY < 0 ? 1.15 : 0.87;
    _zoom.tx = mx - (mx - _zoom.tx) * delta;
    _zoom.ty = my - (my - _zoom.ty) * delta;
    _zoom.s  = Math.min(Math.max(_zoom.s * delta, 0.05), 20);
    applyTreeTransform();
  }, { passive: false });

  // 드래그 팬
  let drag = { active: false, sx: 0, sy: 0, tx0: 0, ty0: 0 };
  wrap.addEventListener("mousedown", (e) => {
    if (!_treeRoot()) return;
    drag.active = true;
    drag.sx = e.clientX; drag.sy = e.clientY;
    drag.tx0 = _zoom.tx; drag.ty0 = _zoom.ty;
  });
  window.addEventListener("mousemove", (e) => {
    if (!drag.active) return;
    const svg = _treeSvg();
    if (!svg) return;
    const rect = wrap.getBoundingClientRect();
    const vb = svg.viewBox.baseVal;
    const dx = (e.clientX - drag.sx) / rect.width  * vb.width;
    const dy = (e.clientY - drag.sy) / rect.height * vb.height;
    _zoom.tx = drag.tx0 + dx;
    _zoom.ty = drag.ty0 + dy;
    applyTreeTransform();
  });
  window.addEventListener("mouseup", () => { drag.active = false; });
  wrap.addEventListener("mouseleave", () => { drag.active = false; });
}

// 버튼 와이어
const _btnFit  = document.getElementById("btn-tree-fit");
const _btnZin  = document.getElementById("btn-tree-zoomin");
const _btnZout = document.getElementById("btn-tree-zoomout");
if (_btnFit)  _btnFit.addEventListener("click", fitTreeView);
if (_btnZin)  _btnZin.addEventListener("click", zoomTreeIn);
if (_btnZout) _btnZout.addEventListener("click", zoomTreeOut);

/* renderTreeSvg 가 호출된 후 자동 fit + 핸들러 바인딩.
 * MutationObserver 로 #tree-svg-wrap 안에 svg 가 새로 들어올 때마다 트리거. */
const _treeWrapObserverInit = () => {
  const wrap = _treeWrap();
  if (!wrap) return;
  setupTreeZoomHandlers();
  const obs = new MutationObserver(() => {
    const svg = _treeSvg();
    if (!svg) return;
    // 새 SVG 가 들어왔으면 줌 상태 리셋 + 다음 tick 에 fit
    _zoom.s = 1; _zoom.tx = 0; _zoom.ty = 0;
    applyTreeTransform();
    setTimeout(fitTreeView, 30);
  });
  obs.observe(wrap, { childList: true, subtree: false });
};
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", _treeWrapObserverInit);
} else {
  _treeWrapObserverInit();
}

/* btn-tree-search 내부 stagger 에 panToTreeNode 추가용 — 기존 핸들러는 그대로
 * 두고, search trace 로그 시 노드 element 를 잡아 카메라 이동만 wrap. */
(function wrapSearchPan() {
  const orig = document.getElementById("btn-tree-search");
  if (!orig) return;
  // 기존 click 리스너에 더해서 동일 input 으로 이어가는 'after' 훅 등록.
  orig.addEventListener("click", () => {
    const target = parseInt(document.getElementById("tree-search-target")?.value);
    if (!Number.isFinite(target) || !_lastRenderedTree) return;
    const result = searchPath(_lastRenderedTree, target);
    // 노드 stagger 와 동일한 타이밍으로 카메라 이동
    result.path.forEach((p, i) => {
      setTimeout(() => {
        const el = _nodeIndexById?.get(p.node);
        if (el) panToTreeNode(el);
      }, i * 450 + 80);  // 하이라이트 살짝 뒤 80ms 시점
    });
  });
})();
