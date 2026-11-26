/* Pulso del transporte — panel en vivo (JS sin dependencias salvo Leaflet). */
(() => {
  "use strict";

  const $ = (s) => document.querySelector(s);
  const el = (tag, attrs = {}, ...kids) => {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (k === "class") n.className = v;
      else if (k === "style") n.style.cssText = v;
      else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else if (v !== null && v !== undefined && v !== false) n.setAttribute(k, v);
    }
    for (const kid of kids.flat()) if (kid !== null && kid !== undefined) n.append(kid);
    return n;
  };
  const SVGNS = "http://www.w3.org/2000/svg";
  const svg = (tag, attrs = {}) => {
    const n = document.createElementNS(SVGNS, tag);
    for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, v);
    return n;
  };
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  const NET_LABEL = { cercanias: "Cercanías", ld: "AV/LD" };
  const NET_SERIES = { cercanias: "--series-1", ld: "--series-2" };
  const ON_TIME = { cercanias: 180, ld: 300 };
  const FEED_LABEL = {
    cercanias_trip_updates: "Cercanías · retrasos",
    cercanias_vehicle_positions: "Cercanías · posiciones",
    cercanias_alerts: "Cercanías · avisos",
    ld_trip_updates: "AV/LD · retrasos",
    ld_vehicle_positions: "AV/LD · posiciones",
  };
  const ANOM = {
    pico_retraso: { icon: "↑", label: "Pico de retraso", color: "--serious" },
    retraso_alto: { icon: "!", label: "Retraso alto", color: "--critical" },
    cancelaciones: { icon: "✕", label: "Cancelaciones", color: "--critical" },
    tren_detenido: { icon: "‖", label: "Tren detenido", color: "--warning" },
  };
  const hhmm = new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", hour: "2-digit", minute: "2-digit" });
  const dayHhmm = new Intl.DateTimeFormat("es-ES", { timeZone: "Europe/Madrid", day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
  const nf = new Intl.NumberFormat("es-ES");
  const nf1 = new Intl.NumberFormat("es-ES", { maximumFractionDigits: 1 });

  const state = { net: "", hours: 6, line: null, live: null, search: "" };

  // ---------------------------------------------------------------- utilidades
  function fmtDelay(s) {
    if (s === null || s === undefined) return "—";
    const m = Math.round(s / 60);
    if (m === 0) return "0 min";
    return (m > 0 ? "+" : "−") + Math.abs(m) + " min";
  }
  function status(delay, net) {
    if (delay === null || delay === undefined) return { key: "none", label: "Sin dato", color: css("--neutral") };
    if (delay < (ON_TIME[net] || 180)) return { key: "good", label: "Puntual", color: css("--good") };
    if (delay < 600) return { key: "warning", label: "3–10 min", color: css("--warning") };
    if (delay < 1200) return { key: "serious", label: "10–20 min", color: css("--serious") };
    return { key: "critical", label: "20 min o más", color: css("--critical") };
  }
  function textOn(hex) {
    const h = (hex || "").replace("#", "");
    if (h.length !== 6) return "#fff";
    const [r, g, b] = [0, 2, 4].map((i) => parseInt(h.slice(i, i + 2), 16) / 255);
    const lum = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    return lum > 0.6 ? "#0b0b0b" : "#ffffff";
  }
  function ago(iso) {
    if (!iso) return "";
    const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return `hace ${Math.round(s)} s`;
    if (s < 3600) return `hace ${Math.round(s / 60)} min`;
    return `hace ${nf1.format(s / 3600)} h`;
  }
  async function getJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`${url}: ${r.status}`);
    return r.json();
  }
  const byNet = (x) => !state.net || x.network === state.net;

  // ----------------------------------------------------------------- tooltip
  const tip = $("#tip");
  function showTip(html, x, y) {
    tip.innerHTML = html;
    tip.hidden = false;
    const r = tip.getBoundingClientRect();
    const left = Math.min(window.innerWidth - r.width - 8, x + 14);
    const top = y + r.height + 18 > window.innerHeight ? y - r.height - 12 : y + 14;
    tip.style.left = `${Math.max(8, left)}px`;
    tip.style.top = `${Math.max(8, top)}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  // -------------------------------------------------------------------- tema
  function applyTheme(t) {
    if (t) document.documentElement.dataset.theme = t;
    else delete document.documentElement.dataset.theme;
    setTiles();
    if (state.live) renderAll();
    loadHistory();
  }
  function isDark() {
    const t = document.documentElement.dataset.theme;
    return t ? t === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  }
  $("#theme").addEventListener("click", () => {
    const next = isDark() ? "light" : "dark";
    try { localStorage.setItem("pulso-theme", next); } catch (_) { /* sin almacenamiento */ }
    applyTheme(next);
  });
  try { const t = localStorage.getItem("pulso-theme"); if (t) document.documentElement.dataset.theme = t; } catch (_) { /* nada */ }

  // --------------------------------------------------------------------- mapa
  const map = L.map("map", { zoomControl: true, attributionControl: true, preferCanvas: true })
    .fitBounds([[36.0, -9.4], [43.8, 3.4]]);
  let tiles = null;
  function setTiles() {
    // Teselas de OpenStreetMap sin clave; en gris (y en oscuro, invertidas) por CSS para que
    // el color quede reservado a los trenes.
    if (tiles) return;
    tiles = L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    }).addTo(map);
  }
  setTiles();
  const trainLayer = L.layerGroup().addTo(map);

  function renderLegend() {
    const items = ["good", "warning", "serious", "critical"].map((k) => {
      const s = status({ good: 0, warning: 300, serious: 700, critical: 1300 }[k], "cercanias");
      return el("span", {}, el("i", { class: "dot", style: `background:${s.color}` }), s.label);
    });
    $("#map-legend").replaceChildren(...items);
  }

  function renderMap(trains) {
    trainLayer.clearLayers();
    const surface = css("--surface-1");
    let shown = 0;
    for (const t of trains) {
      if (t.lat === null || t.lon === null || t.status === "CANCELED") continue;
      const sel = state.line && (t.line !== state.line.line || t.area !== state.line.area);
      const s = status(t.delay_s, t.network);
      const m = L.circleMarker([t.lat, t.lon], {
        radius: sel ? 4 : 6, weight: 2, color: surface, fillColor: sel ? css("--neutral") : s.color,
        fillOpacity: sel ? 0.45 : 1, opacity: sel ? 0.5 : 1,
      });
      m.bindTooltip(
        `<b>${t.line}</b> · ${t.area}<br>${t.status === "SCHEDULED" ? `Retraso: <b>${fmtDelay(t.delay_s)}</b>` : t.status}` +
        `${t.stalled ? " · parece detenido" : ""}<br>${t.stop_name ? `Próxima parada: ${t.stop_name}<br>` : ""}` +
        `<span style="color:${css("--text-muted")}">Tren ${t.trip_id} · ${ago(t.updated_at)}</span>`,
        { direction: "top", offset: [0, -6] });
      m.addTo(trainLayer);
      shown += 1;
    }
    const empty = $("#map-empty");
    if (shown === 0) {
      empty.hidden = false;
      const night = new Date().toLocaleString("es-ES", { timeZone: "Europe/Madrid", hour: "numeric", hour12: false });
      const h = parseInt(night, 10);
      empty.replaceChildren(el("span", {}, h >= 1 && h < 6
        ? "Ahora mismo no circula ningún tren con posición (servicio nocturno). Vuelve a partir de las 5:30."
        : "No hay trenes con posición para este filtro."));
    } else {
      empty.hidden = true;
    }
  }

  // ------------------------------------------------------------------- tiles
  function tile(label, value, note, hero = false) {
    return el("div", { class: `tile${hero ? " hero" : ""}` },
      el("div", { class: "label" }, label), el("div", { class: "value" }, value),
      note ? el("div", { class: "note" }, note) : null);
  }
  function renderTiles(trains, anomalies) {
    const running = trains.filter((t) => t.status !== "CANCELED");
    const usable = running.filter((t) => t.delay_s !== null && !t.stalled &&
      !(t.flags || []).some((f) => f === "retraso_extremo" || f === "adelanto_extremo"));
    const mean = usable.length ? usable.reduce((a, t) => a + t.delay_s, 0) / usable.length : null;
    const onTime = usable.length ? (100 * usable.filter((t) => t.delay_s < (ON_TIME[t.network] || 180)).length) / usable.length : null;
    const canceled = trains.filter((t) => t.status === "CANCELED").length;
    const active = anomalies.filter((a) => a.status === "active").length;
    $("#tiles").replaceChildren(
      tile("Trenes en circulación", nf.format(running.length), state.net ? NET_LABEL[state.net] : "Cercanías + AV/LD", true),
      tile("Retraso medio", mean === null ? "—" : fmtDelay(mean), `${usable.length} trenes con dato`),
      tile("Puntuales", onTime === null ? "—" : `${nf1.format(onTime)} %`, "< 3 min Cercanías · < 5 min AV/LD"),
      tile("Anomalías activas", nf.format(active), active ? "ver lista" : "nada fuera de lo normal"),
      tile("Cancelados", nf.format(canceled), "publicados por Renfe ahora"),
    );
  }

  // ------------------------------------------------------------------ líneas
  function sparkline(history) {
    const s = svg("svg", { class: "spark", viewBox: "0 0 84 22", "aria-hidden": "true" });
    const pts = history.filter((p) => p.v !== null);
    if (pts.length < 2) return s;
    const vs = pts.map((p) => p.v);
    const max = Math.max(60, ...vs), min = Math.min(0, ...vs);
    const x = (i) => 2 + (i * 80) / (pts.length - 1);
    const y = (v) => 19 - ((v - min) / (max - min || 1)) * 16;
    s.append(svg("path", { d: pts.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.v).toFixed(1)}`).join(""), fill: "none", stroke: css("--neutral"), "stroke-width": 1.5, "stroke-linejoin": "round" }));
    const last = pts[pts.length - 1];
    s.append(svg("circle", { cx: x(pts.length - 1), cy: y(last.v), r: 2.5, fill: css("--accent") }));
    return s;
  }
  function renderLines(lines) {
    const q = state.search.trim().toLowerCase();
    const rows = lines.filter(byNet).filter((l) => !q || `${l.line} ${l.area}`.toLowerCase().includes(q))
      .sort((a, b) => (a.line === "?") - (b.line === "?"));  // sin cruzar, al final
    const list = $("#lines");
    if (!rows.length) {
      list.replaceChildren(el("li", { class: "empty" }, "No hay líneas con trenes ahora mismo."));
      return;
    }
    list.replaceChildren(...rows.map((l) => {
      const s = status(l.mean_delay_s, l.network);
      const color = l.color ? `#${l.color.replace("#", "")}` : css("--neutral");
      const selected = state.line && state.line.line === l.line && state.line.area === l.area;
      return el("li", {
        class: selected ? "sel" : "", tabindex: "0",
        title: "Pulsa para ver solo esta línea en el mapa",
        onclick: () => { state.line = selected ? null : { line: l.line, area: l.area }; renderAll(); },
        onkeydown: (e) => { if (e.key === "Enter") e.currentTarget.click(); },
      },
        el("span", { class: "line-name" },
          el("span", { class: "badge", style: `background:${color};color:${textOn(color)}` }, l.line),
          el("span", { class: "area" }, l.area)),
        el("span", {}, nf.format(l.n_trains) + (l.n_canceled ? ` (+${l.n_canceled}✕)` : "")),
        el("span", { class: "delay" }, el("i", { class: "dot", style: `background:${s.color}` }), fmtDelay(l.mean_delay_s)),
        el("span", {}, l.pct_on_time === null ? "—" : `${Math.round(l.pct_on_time)} %`),
        el("span", {}, sparkline(l.history || [])));
    }));
  }

  // --------------------------------------------------------------- anomalías
  let anomalyHistory = [];
  function renderAnomalies(live) {
    const byId = new Map();
    for (const a of anomalyHistory) byId.set(a.anomaly_id, { ...a, status: a.resolved_at ? "resolved" : "active" });
    for (const a of live) byId.set(a.anomaly_id, { ...byId.get(a.anomaly_id), ...a });
    const items = [...byId.values()].filter(byNet)
      .sort((a, b) => (a.status === "active") - (b.status === "active") || String(a.started_at).localeCompare(String(b.started_at)))
      .reverse().slice(0, 60);
    const activeN = items.filter((a) => a.status === "active").length;
    $("#anom-count").textContent = `${activeN} activas · ${items.length - activeN} resueltas (24 h)`;
    const list = $("#anomalies");
    if (!items.length) { list.replaceChildren(el("li", { class: "empty" }, "Sin anomalías en las últimas 24 h.")); return; }
    list.replaceChildren(...items.map((a) => {
      const k = ANOM[a.type] || { icon: "?", label: a.type, color: "--neutral" };
      const when = a.status === "active" ? `desde ${hhmm.format(new Date(a.started_at))}` :
        `${hhmm.format(new Date(a.started_at))}–${a.resolved_at ? hhmm.format(new Date(a.resolved_at)) : ""}`;
      return el("li", { class: a.status === "active" ? "" : "resolved" },
        el("span", { class: "ico", style: `background:${css(k.color)}`, "aria-hidden": "true" }, k.icon),
        el("div", {},
          el("div", {}, a.message),
          el("div", { class: "meta" }, `${k.label} · ${a.status === "active" ? "activa" : "resuelta"} · ${when}`)));
    }));
  }
  function renderAlerts(alerts) {
    const rows = alerts.filter(byNet).sort((a, b) => String(b.active_start).localeCompare(String(a.active_start)));
    $("#alerts-count").textContent = `${rows.length} activos`;
    const list = $("#alerts");
    if (!rows.length) { list.replaceChildren(el("li", { class: "empty" }, "Renfe no publica avisos ahora mismo.")); return; }
    list.replaceChildren(...rows.slice(0, 80).map((a) => el("li", {},
      el("span", { class: "ico", style: `background:${css("--neutral")}`, "aria-hidden": "true" }, "i"),
      el("div", {},
        el("div", {}, ...(a.lines || []).slice(0, 6).map((l) => el("span", { class: "tag" }, l)), a.description || a.header || "(sin texto)"),
        el("div", { class: "meta" }, `${NET_LABEL[a.network] || a.network}${a.active_start ? ` · desde ${dayHhmm.format(new Date(a.active_start))}` : ""}`)))));
  }

  // ----------------------------------------------------------------- calidad
  function renderQuality(q, counters) {
    const feeds = Object.keys(q).filter((f) => !state.net || f.startsWith(state.net === "ld" ? "ld_" : "cercanias_")).sort();
    const head = el("tr", {}, ...["Fuente", "Snapshots", "Eventos", "Válidos", "Rechazados", "Retraso del feed", "Reglas que saltan"].map((h) => el("th", {}, h)));
    const rows = feeds.map((f) => {
      const s = q[f];
      const issues = [
        ...Object.entries(s.errors).map(([k, v]) => `✕ ${k.replaceAll("_", " ")} (${nf.format(v)})`),
        ...Object.entries(s.warnings).map(([k, v]) => `! ${k.replaceAll("_", " ")} (${nf.format(v)})`),
      ];
      return el("tr", {},
        el("td", {}, FEED_LABEL[f] || f),
        el("td", {}, nf.format(s.snapshots)),
        el("td", {}, nf.format(s.n_in)),
        el("td", {}, `${nf1.format(s.pct_valid)} %`, el("span", { class: "meter", "aria-hidden": "true" }, el("b", { style: `width:${s.pct_valid}%` }))),
        el("td", {}, nf.format(s.n_rejected)),
        el("td", {}, `${Math.round(s.max_lag_s)} s`),
        el("td", { class: "issues" }, issues.length ? issues.join(" · ") : "ninguna"));
    });
    $("#quality").replaceChildren(el("thead", {}, head), el("tbody", {}, ...rows));
    const c = counters || {};
    $("#counters").textContent = `Procesador: ${nf.format(c.eventos_retraso || 0)} eventos de retraso · ` +
      `${nf.format(c.viajes_de_dias_pasados || 0)} de trenes de días pasados (excluidos) · ` +
      `${nf.format(c.duplicados_ignorados || 0)} duplicados ignorados · ${nf.format(c.eventos_tardios || 0)} tardíos.`;
  }

  // ------------------------------------------------------------- gráfico SVG
  function lineChart(container, series, { yFmt, yMax = null, yUnit = "" }) {
    container.replaceChildren();
    const legendBox = series.length > 1 ? el("div", { class: "chart-legend" }, ...series.map((s) =>
      el("span", {}, el("i", { class: "key", style: `background:${s.color}` }), s.name))) : null;
    const all = series.flatMap((s) => s.points);
    if (!all.length) {
      container.append(el("p", { class: "muted small" }, "Todavía no hay histórico para este periodo."));
      return;
    }
    if (legendBox) container.append(legendBox);
    const W = container.clientWidth || 600;
    const H = Math.max(140, (container.clientHeight || 220) - (legendBox ? legendBox.offsetHeight + 4 : 0));
    const m = { t: 8, r: 12, b: 22, l: 40 };
    const s = svg("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": container.dataset.label || "gráfico" });
    const t0 = Math.min(...all.map((p) => p.t)), t1 = Math.max(...all.map((p) => p.t));
    let vmax = yMax ?? Math.max(1, ...all.map((p) => p.v));
    const step = niceStep(vmax / 4);
    vmax = yMax ?? Math.ceil(vmax / step) * step;
    const x = (t) => m.l + ((t - t0) / Math.max(1, t1 - t0)) * (W - m.l - m.r);
    const y = (v) => H - m.b - (v / vmax) * (H - m.t - m.b);
    const g = svg("g", { class: "axis" });
    for (let v = 0; v <= vmax + 1e-9; v += yMax ? 25 : step) {
      g.append(svg("line", { class: "gridline", x1: m.l, x2: W - m.r, y1: y(v), y2: y(v) }));
      const tx = svg("text", { x: m.l - 6, y: y(v) + 4, "text-anchor": "end" });
      tx.textContent = yFmt(v);
      g.append(tx);
    }
    const nTicks = Math.max(2, Math.min(6, Math.floor((W - m.l) / 90)));
    for (let i = 0; i <= nTicks; i++) {
      const t = t0 + ((t1 - t0) * i) / nTicks;
      const tx = svg("text", { x: x(t), y: H - 6, "text-anchor": i === 0 ? "start" : i === nTicks ? "end" : "middle" });
      tx.textContent = hhmm.format(new Date(t));
      g.append(tx);
    }
    s.append(g);
    for (const se of series) {
      let d = "", prev = null;
      for (const p of se.points) {
        const gap = prev && p.t - prev.t > 11 * 60 * 1000;
        d += `${!prev || gap ? "M" : "L"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`;
        prev = p;
      }
      s.append(svg("path", { class: "ln", d, stroke: se.color }));
      const last = se.points[se.points.length - 1];
      if (last) s.append(svg("circle", { cx: x(last.t), cy: y(last.v), r: 4, fill: se.color, stroke: css("--surface-1"), "stroke-width": 2 }));
    }
    const cross = svg("line", { class: "cross", y1: m.t, y2: H - m.b, visibility: "hidden" });
    const dots = series.map((se) => svg("circle", { r: 4, fill: se.color, stroke: css("--surface-1"), "stroke-width": 2, visibility: "hidden" }));
    s.append(cross, ...dots);
    const times = [...new Set(all.map((p) => p.t))].sort((a, b) => a - b);
    const hit = svg("rect", { class: "hit", x: m.l, y: m.t, width: W - m.l - m.r, height: H - m.t - m.b });
    hit.addEventListener("mousemove", (ev) => {
      const r = s.getBoundingClientRect();
      const px = ((ev.clientX - r.left) / r.width) * W;
      const tt = t0 + ((px - m.l) / (W - m.l - m.r)) * (t1 - t0);
      const t = times.reduce((a, b) => (Math.abs(b - tt) < Math.abs(a - tt) ? b : a));
      cross.setAttribute("x1", x(t)); cross.setAttribute("x2", x(t)); cross.setAttribute("visibility", "visible");
      let html = `<div><b>${dayHhmm.format(new Date(t))}</b></div>`;
      series.forEach((se, i) => {
        const p = se.points.find((q) => q.t === t);
        if (p) {
          dots[i].setAttribute("cx", x(t)); dots[i].setAttribute("cy", y(p.v)); dots[i].setAttribute("visibility", "visible");
          html += `<div class="row"><span><i class="key" style="background:${se.color}"></i>${se.name}</span><b>${yFmt(p.v, true)}${yUnit}</b></div>`;
        } else dots[i].setAttribute("visibility", "hidden");
      });
      showTip(html, ev.clientX, ev.clientY);
    });
    hit.addEventListener("mouseleave", () => { cross.setAttribute("visibility", "hidden"); dots.forEach((d) => d.setAttribute("visibility", "hidden")); hideTip(); });
    s.append(hit);
    s.style.height = `${H}px`;
    container.append(s);
  }
  function niceStep(raw) {
    const p = Math.pow(10, Math.floor(Math.log10(Math.max(raw, 1e-9))));
    const n = raw / p;
    return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * p;
  }
  function table(target, series, fmt) {
    const times = [...new Set(series.flatMap((s) => s.points.map((p) => p.t)))].sort((a, b) => b - a);
    const head = el("tr", {}, el("th", {}, "Hora"), ...series.map((s) => el("th", {}, s.name)));
    const rows = times.map((t) => el("tr", {}, el("td", {}, dayHhmm.format(new Date(t))),
      ...series.map((s) => { const p = s.points.find((q) => q.t === t); return el("td", {}, p ? fmt(p.v) : "—"); })));
    target.replaceChildren(el("table", {}, el("thead", {}, head), el("tbody", {}, ...rows)));
  }

  let kpiRows = [];
  function renderCharts() {
    const nets = state.net ? [state.net] : ["cercanias", "ld"];
    const mk = (field) => nets.map((n) => ({
      name: NET_LABEL[n], color: css(NET_SERIES[n]),
      points: kpiRows.filter((r) => r.network === n && r[field] !== null)
        .map((r) => ({ t: new Date(r.window_start).getTime(), v: field === "mean_delay_s" ? r[field] / 60 : r[field] })),
    })).filter((s) => s.points.length);
    const delay = mk("mean_delay_s"), ontime = mk("pct_on_time");
    $("#chart-delay").dataset.label = "Retraso medio por red en minutos";
    $("#chart-ontime").dataset.label = "Porcentaje de trenes puntuales por red";
    lineChart($("#chart-delay"), delay, { yFmt: (v, exact) => exact ? nf1.format(v) : nf.format(v), yUnit: " min" });
    lineChart($("#chart-ontime"), ontime, { yFmt: (v, exact) => exact ? nf1.format(v) : nf.format(v), yMax: 100, yUnit: " %" });
    table($("#table-delay"), delay, (v) => `${nf1.format(v)} min`);
    table($("#table-ontime"), ontime, (v) => `${nf1.format(v)} %`);
  }

  function renderStops(rows) {
    const list = $("#stops");
    if (!rows.length) { list.replaceChildren(el("li", { class: "empty" }, "Sin datos de estaciones en la última hora.")); return; }
    const max = Math.max(60, ...rows.map((r) => r.mean_delay_s));
    list.replaceChildren(...rows.slice(0, 12).map((r) => el("li", {},
      el("span", { class: "name", title: `${r.stop_name || r.stop_id} · ${nf.format(r.n_obs)} pasos de tren` }, r.stop_name || r.stop_id),
      el("span", { class: "track" }, el("span", { class: "fill", style: `display:block;width:${Math.max(0, (100 * r.mean_delay_s) / max)}%` })),
      el("span", { class: "val" }, fmtDelay(r.mean_delay_s)))));
  }

  function renderPrediction(p) {
    const box = $("#prediction");
    if (!p || !p.available) {
      box.replaceChildren(el("p", { class: "pred-empty" },
        el("strong", {}, "Modelo en preparación. "),
        p && p.reason ? p.reason : "",
        " Cuando esté entrenado, aquí verás el retraso previsto de cada tren dentro de 15 minutos y cuánto mejora frente a suponer que el retraso actual se mantiene."));
      return;
    }
    const m = p.metrics || {};
    const rows = (p.trains || []).filter(byNet).slice(0, 10);
    box.replaceChildren(
      el("p", { class: "muted small" }, `MAE del modelo ${nf1.format(m.mae_model_min)} min frente a ${nf1.format(m.mae_baseline_min)} min del baseline («el retraso se mantiene»), evaluado con ${nf.format(m.n_test || 0)} casos de días no vistos.`),
      el("div", { class: "tbl-wrap" }, el("table", {},
        el("thead", {}, el("tr", {}, ...["Tren", "Línea", "Ahora", "En 15 min"].map((h) => el("th", {}, h)))),
        el("tbody", {}, ...rows.map((t) => el("tr", {}, el("td", {}, t.trip_id), el("td", {}, t.line), el("td", {}, fmtDelay(t.delay_s)), el("td", {}, fmtDelay(t.pred_delay_s))))))));
  }

  // ------------------------------------------------------------ carga datos
  function renderAll() {
    const s = state.live;
    if (!s) return;
    const trains = s.trains.filter(byNet).filter((t) => !t.stale);
    renderTiles(trains, s.anomalies.filter(byNet));
    renderMap(trains);
    renderLines(s.lines);
    renderAnomalies(s.anomalies);
    renderAlerts(s.alerts);
    renderQuality(s.quality, s.counters);
  }
  async function loadLive() {
    const live = $("#live");
    try {
      state.live = await getJSON("/api/state");
      live.className = `live ${state.live.stale ? "stale" : "ok"}`;
      live.querySelector("span").textContent = state.live.stale
        ? `Datos con retraso (${ago(state.live.generated_at)})`
        : `En directo · actualizado ${ago(state.live.generated_at)}`;
      renderAll();
    } catch (e) {
      live.className = "live";
      live.querySelector("span").textContent = "Sin conexión con el procesador";
    }
  }
  async function loadHistory() {
    const net = state.net ? `&network=${state.net}` : "";
    try {
      const [k, st, an, pr] = await Promise.all([
        getJSON(`/api/kpis?hours=${state.hours}`),
        getJSON(`/api/stops?hours=1&limit=12${net}`),
        getJSON("/api/anomalies?hours=24"),
        getJSON("/api/predictions"),
      ]);
      kpiRows = k; anomalyHistory = an;
      renderCharts(); renderStops(st); renderPrediction(pr);
      if (state.live) renderAnomalies(state.live.anomalies);
    } catch (e) { console.warn(e); }
  }

  // ---------------------------------------------------------------- eventos
  document.querySelectorAll("[data-net]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("[data-net]").forEach((x) => x.classList.toggle("on", x === b));
    state.net = b.dataset.net; state.line = null;
    renderAll(); loadHistory();
  }));
  document.querySelectorAll("[data-hours]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("[data-hours]").forEach((x) => x.classList.toggle("on", x === b));
    state.hours = Number(b.dataset.hours); loadHistory();
  }));
  $("#line-search").addEventListener("input", (e) => { state.search = e.target.value; if (state.live) renderLines(state.live.lines); });
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => applyTheme(document.documentElement.dataset.theme));
  let rt; window.addEventListener("resize", () => { clearTimeout(rt); rt = setTimeout(renderCharts, 150); });

  renderLegend();
  loadLive(); loadHistory();
  setInterval(loadLive, 15000);
  setInterval(loadHistory, 60000);
})();
