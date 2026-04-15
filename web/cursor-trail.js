/* cursor-trail.js — single continuous elegant curve following mouse
 *
 * 핵심: 끊어진 세그먼트가 아니라 하나의 연속 곡선.
 * 얇고, 우아하고, 천천히 사라진다.
 */

(function () {
  var canvas = document.createElement("canvas");
  canvas.id = "cursor-trail";
  canvas.style.cssText =
    "position:fixed;inset:0;z-index:9998;pointer-events:none;";
  document.body.appendChild(canvas);

  var ctx = canvas.getContext("2d");
  var w, h, dpr;

  /* trail history — smoothed positions over time */
  var trail = [];
  var maxLen = 120;
  var mouse = { x: -999, y: -999 };

  /* smoothed cursor (lerp) for organic feel */
  var cursor = { x: -999, y: -999 };
  var speed = 0.35;

  function resize() {
    dpr = window.devicePixelRatio || 1;
    w = window.innerWidth;
    h = window.innerHeight;
    canvas.width = w * dpr;
    canvas.height = h * dpr;
    canvas.style.width = w + "px";
    canvas.style.height = h + "px";
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }
  resize();
  window.addEventListener("resize", resize);

  document.addEventListener("mousemove", function (e) {
    mouse.x = e.clientX;
    mouse.y = e.clientY;
  });

  function tick() {
    /* lerp cursor toward mouse */
    cursor.x += (mouse.x - cursor.x) * speed;
    cursor.y += (mouse.y - cursor.y) * speed;

    /* only add if moved enough */
    var last = trail[trail.length - 1];
    if (!last ||
      Math.abs(cursor.x - last.x) > 0.5 ||
      Math.abs(cursor.y - last.y) > 0.5) {
      trail.push({ x: cursor.x, y: cursor.y });
    }

    /* trim old points */
    while (trail.length > maxLen) trail.shift();

    draw();
    requestAnimationFrame(tick);
  }

  function draw() {
    ctx.clearRect(0, 0, w, h);

    var len = trail.length;
    if (len < 4) return;

    /* Draw one continuous path with varying opacity via gradient segments.
     * We split into small sub-paths so we can vary alpha along the curve. */

    for (var i = 2; i < len - 1; i++) {
      var t = i / (len - 1); /* 0 = oldest, 1 = newest */

      /* alpha: old = transparent, new = opaque */
      var alpha = t * t * 0.45;

      /* line width: slightly tapers — thinner at tail, thicker at head */
      var lw = 0.4 + t * 0.8;

      if (alpha < 0.005) continue;

      var p0 = trail[i - 2];
      var p1 = trail[i - 1];
      var p2 = trail[i];
      var p3 = trail[Math.min(i + 1, len - 1)];

      /* Catmull-Rom → cubic bezier control points */
      var tension = 0.25;
      var cp1x = p1.x + (p2.x - p0.x) * tension;
      var cp1y = p1.y + (p2.y - p0.y) * tension;
      var cp2x = p2.x - (p3.x - p1.x) * tension;
      var cp2y = p2.y - (p3.y - p1.y) * tension;

      ctx.beginPath();
      ctx.moveTo(p1.x, p1.y);
      ctx.bezierCurveTo(cp1x, cp1y, cp2x, cp2y, p2.x, p2.y);

      /* warm copper/brown stroke */
      ctx.strokeStyle = "rgba(160,112,80," + alpha + ")";
      ctx.lineWidth = lw;
      ctx.lineCap = "round";
      ctx.stroke();
    }
  }

  /* init cursor position */
  cursor.x = mouse.x;
  cursor.y = mouse.y;
  requestAnimationFrame(tick);
})();
