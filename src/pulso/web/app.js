/* Pulso del transporte — panel (JS sin dependencias salvo Leaflet).
 * Dos fuentes de datos con la misma interfaz:
 *  - en directo: la API FastAPI (/api/*);
 *  - repetición: fotogramas estáticos generados con `pulso demo` (window.PULSO_DEMO).
 */
(() => {
  "use strict";

  // ------------------------------------------------------------- utilidades DOM
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

  // ------------------------------------------------------------------ constantes
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
  const UNKNOWN_LINE = "?";
  const TZ = "Europe/Madrid";
  const hhmm = new Intl.DateTimeFormat("es-ES", { timeZone: TZ, hour: "2-digit", minute: "2-digit" });
  const dayHhmm = new Intl.DateTimeFormat("es-ES", { timeZone: TZ, day: "numeric", month: "short", hour: "2-digit", minute: "2-digit" });
  const dayLong = new Intl.DateTimeFormat("es-ES", { timeZone: TZ, day: "numeric", month: "long", year: "numeric" });
  const nf = new Intl.NumberFormat("es-ES");
  const nf1 = new Intl.NumberFormat("es-ES", { maximumFractionDigits: 1 });

  const ui = { net: "", area: "", hours: 6, line: null, train: null, search: "", live: null, kpis: [], anomalies: [] };

  // --------------------------------------------------------------- fuentes de datos
  async function getJSON(url) {
    const r = await fetch(url, { cache: "no-store" });
    if (!r.ok) throw new Error(`${url}: ${r.status}`);
    return r.json();
  }

  const LiveSource = {
    mode: "live",
    now: () => Date.now(),
    state: () => getJSON("/api/state"),
    kpis: (hours) => getJSON(`/api/kpis?hours=${hours}`),
    stops: (net) => getJSON(`/api/stops?hours=1&limit=12${net ? `&network=${net}` : ""}`),
    anomalies: () => getJSON("/api/anomalies?hours=24"),
    predictions: () => getJSON("/api/predictions"),
  };

  function makeDemoSource(manifestUrl) {
    const base = manifestUrl.replace(/[^/]*$/, "");
    const cache = new Map();
    const src = {
      mode: "replay", manifest: null, idx: 0, allKpis: [], allAnoms: [], frame: null,
      now: () => (src.frame ? new Date(src.frame.t).getTime() : Date.now()),
      async init() {
        const [m, k, a] = await Promise.all([
          getJSON(manifestUrl), getJSON(base + "kpis.json"), getJSON(base + "anomalies.json"),
        ]);
        src.manifest = m; src.allKpis = k; src.allAnoms = a;
      },
      async load(i) {
        const f = src.manifest.frames[i];
        if (!cache.has(f.file)) cache.set(f.file, getJSON(base + f.file));
        src.frame = await cache.get(f.file);
        src.idx = i;
        const next = src.manifest.frames[i + 1];
        if (next && !cache.has(next.file)) cache.set(next.file, getJSON(base + next.file));
        return src.frame;
      },
      async state() {
        const f = src.frame;
        return { ...f, generated_at: f.t, watermark: f.t, stale: false };
      },
      async kpis(hours) {
        const t = src.now();
        return src.allKpis.filter((r) => {
          const w = new Date(r.window_start).getTime();
          return w <= t && w >= t - hours * 3600e3;
        });
      },
      async stops(net) { return (src.frame.stops || []).filter((s) => !net || s.network === net); },
      async anomalies() {
        const t = src.now();
        return src.allAnoms.filter((a) => new Date(a.started_at).getTime() <= t).map((a) => ({
          ...a, resolved_at: a.resolved_at && new Date(a.resolved_at).getTime() <= t ? a.resolved_at : null,
        }));
      },
      async predictions() { return { available: false }; },
    };
    return src;
  }

  const source = window.PULSO_DEMO ? makeDemoSource(window.PULSO_DEMO) : LiveSource;

  // ----------------------------------------------------------------- formato
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
    return 0.2126 * r + 0.7152 * g + 0.0722 * b > 0.6 ? "#0b0b0b" : "#ffffff";
  }
  function ago(iso) {
    if (!iso) return "";
    const s = Math.max(0, (source.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return `hace ${Math.round(s)} s`;
    if (s < 3600) return `hace ${Math.round(s / 60)} min`;
    return `hace ${nf1.format(s / 3600)} h`;
  }
  const lineColor = (c) => (c ? `#${String(c).replace("#", "")}` : css("--neutral"));
  const isUsable = (t) => t.status !== "CANCELED" && t.delay_s !== null && !t.stalled &&
    !(t.flags || []).some((f) => f === "retraso_extremo" || f === "adelanto_extremo");
  const matches = (x) => (!ui.net || x.network === ui.net) && (!ui.area || x.area === ui.area);

  // -------------------------------------------------------------- tooltip
  const tip = $("#tip");
  function showTip(html, x, y) {
    tip.innerHTML = html;
    tip.hidden = false;
    const r = tip.getBoundingClientRect();
    tip.style.left = `${Math.max(8, Math.min(window.innerWidth - r.width - 8, x + 14))}px`;
    tip.style.top = `${Math.max(8, y + r.height + 18 > window.innerHeight ? y - r.height - 12 : y + 14)}px`;
  }
  const hideTip = () => { tip.hidden = true; };

  // ------------------------------------------------------------------ tema
  function isDark() {
    const t = document.documentElement.dataset.theme;
    return t ? t === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  }
  function rerender() { if (ui.live) renderAll(); renderHistory(); }
  $("#theme").addEventListener("click", () => {
    const next = isDark() ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    try { localStorage.setItem("pulso-theme", next); } catch (_) { /* sin almacenamiento */ }
    rerender();
  });
  try { const t = localStorage.getItem("pulso-theme"); if (t) document.documentElement.dataset.theme = t; } catch (_) { /* nada */ }
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", rerender);

  // ------------------------------------------------------------------- mapa
  const SPAIN = [[36.0, -9.4], [43.8, 3.4]];
  const map = L.map("map", { zoomControl: true, attributionControl: true, preferCanvas: true }).fitBounds(SPAIN);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 18,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
  }).addTo(map);
  const trainLayer = L.layerGroup().addTo(map);
  let lastFitArea = null;

  function renderLegend() {
    const items = [0, 300, 700, 1300].map((d) => {
      const s = status(d, "cercanias");
      return el("span", {}, el("i", { class: "dot", style: `background:${s.color}` }), s.label);
    });
    items.push(el("span", {}, el("i", { class: "dot ring" }), "Detenido"));
    $("#map-legend").replaceChildren(...items);
  }

  function renderMap(trains) {
    trainLayer.clearLayers();
    const surface = css("--surface-1");
    const pts = [];
    for (const t of trains) {
      if (t.lat === null || t.lon === null || t.status === "CANCELED") continue;
      const dim = ui.line && (t.line !== ui.line.line || t.area !== ui.line.area);
      const s = status(t.delay_s, t.network);
      const sel = ui.train === t.trip_id;
      const m = L.circleMarker([t.lat, t.lon], t.stalled ? {
        radius: 6, weight: 3, color: css("--critical"), fillColor: surface, fillOpacity: 1,
      } : {
        radius: sel ? 9 : dim ? 4 : 6, weight: 2, color: sel ? css("--text-primary") : surface,
        fillColor: dim ? css("--neutral") : s.color, fillOpacity: dim ? 0.45 : 1, opacity: dim ? 0.5 : 1,
      });
      m.bindTooltip(`<b>${t.line}</b> · ${t.area}<br>${t.status === "SCHEDULED" ? fmtDelay(t.delay_s) : t.status}` +
        `${t.stop_name ? ` · ${t.stop_name}` : ""}`, { direction: "top", offset: [0, -6] });
      m.on("click", () => { ui.train = t.trip_id; renderTrainCard(); renderMap(currentTrains()); });
      m.addTo(trainLayer);
      pts.push([t.lat, t.lon]);
    }
    if (ui.area && ui.area !== lastFitArea && pts.length) {
      map.fitBounds(pts, { padding: [40, 40], maxZoom: 11 });
      lastFitArea = ui.area;
    } else if (!ui.area && lastFitArea) {
      map.fitBounds(SPAIN);
      lastFitArea = null;
    }
    const empty = $("#map-empty");
    if (pts.length === 0) {
      const h = parseInt(new Date(source.now()).toLocaleString("es-ES", { timeZone: TZ, hour: "numeric", hour12: false }), 10);
      empty.hidden = false;
      empty.replaceChildren(el("span", {}, h >= 1 && h < 6
        ? "Ahora no circula ningún tren con posición (servicio nocturno). Vuelve a partir de las 5:30."
        : "No hay trenes con posición para este filtro."));
    } else {
      empty.hidden = true;
    }
  }

  function renderTrainCard() {
    const card = $("#train-card");
    const t = ui.train && ui.live && ui.live.trains.find((x) => x.trip_id === ui.train);
    if (!t) { card.hidden = true; return; }
    const s = status(t.delay_s, t.network);
    const flags = [];
    if (t.stalled) flags.push("Parece detenido (o con el dato congelado)");
    if ((t.flags || []).includes("retraso_extremo")) flags.push("Retraso extremo: excluido de las medias");
    card.replaceChildren(
      el("div", { class: "tc-head" },
        el("span", { class: "line-name" },
          el("span", { class: "badge", style: `background:${lineColor(t.color)};color:${textOn(lineColor(t.color))}` }, t.line),
          el("span", { class: "area" }, t.area)),
        el("button", { class: "close", "aria-label": "Cerrar ficha", onclick: () => { ui.train = null; card.hidden = true; renderMap(currentTrains()); } }, "×")),
      el("div", { class: "tc-delay" }, el("i", { class: "dot", style: `background:${s.color}` }),
        t.status === "CANCELED" ? "Cancelado" : fmtDelay(t.delay_s)),
      el("dl", {},
        el("dt", {}, "Estado"), el("dd", {}, t.status === "SCHEDULED" ? s.label : t.status),
        el("dt", {}, "Próxima parada"), el("dd", {}, t.stop_name || "—"),
        el("dt", {}, "Red"), el("dd", {}, NET_LABEL[t.network] || t.network),
        el("dt", {}, "Tren"), el("dd", { class: "mono" }, t.trip_id),
        el("dt", {}, "Actualizado"), el("dd", {}, ago(t.updated_at))),
      ...(flags.length ? [el("div", { class: "muted small" }, flags.join(" · "))] : []));
    card.hidden = false;
  }

  // ------------------------------------------------------------------- tarjetas
  function trendSvg(values) {
    const s = svg("svg", { class: "trend", viewBox: "0 0 72 20", "aria-hidden": "true" });
    const v = values.filter((x) => x !== null && x !== undefined);
    if (v.length < 2) return s;
    const max = Math.max(...v), min = Math.min(...v);
    const x = (i) => 2 + (i * 68) / (v.length - 1);
    const y = (n) => 17 - ((n - min) / (max - min || 1)) * 14;
    s.append(svg("path", { d: v.map((n, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(n).toFixed(1)}`).join(""), fill: "none", stroke: css("--neutral"), "stroke-width": 1.5, "stroke-linejoin": "round" }));
    s.append(svg("circle", { cx: x(v.length - 1), cy: y(v[v.length - 1]), r: 2.5, fill: css("--accent") }));
    return s;
  }
  function tile(label, value, note, opts = {}) {
    return el("div", { class: `tile${opts.hero ? " hero-tile" : ""}` },
      el("div", { class: "label" }, label), el("div", { class: "value" }, value),
      el("div", { class: "note" }, el("span", {}, note || ""), opts.trend || null));
  }
  function networkTrend(field) {
    const by = new Map();
    for (const r of ui.kpis.filter((r) => !ui.net || r.network === ui.net)) {
      if (r[field] === null || r[field] === undefined) continue;
      const a = by.get(r.window_start) || { s: 0, n: 0 };
      const w = r.n_trains || 1;
      a.s += r[field] * w; a.n += w;
      by.set(r.window_start, a);
    }
    return [...by.entries()].sort((a, b) => String(a[0]).localeCompare(String(b[0]))).slice(-24).map(([, a]) => a.s / a.n);
  }
  function renderTiles(trains, anomalies) {
    const running = trains.filter((t) => t.status !== "CANCELED");
    const usable = trains.filter(isUsable);
    const mean = usable.length ? usable.reduce((a, t) => a + t.delay_s, 0) / usable.length : null;
    const onTime = usable.length ? (100 * usable.filter((t) => t.delay_s < (ON_TIME[t.network] || 180)).length) / usable.length : null;
    const canceled = trains.filter((t) => t.status === "CANCELED").length;
    const active = anomalies.filter((a) => a.status === "active").length;
    const scope = ui.area || (ui.net ? NET_LABEL[ui.net] : "Cercanías + AV/LD");
    $("#tiles").replaceChildren(
      tile("Trenes en circulación", nf.format(running.length), scope, { hero: true }),
      tile("Retraso medio", mean === null ? "—" : fmtDelay(mean), `${nf.format(usable.length)} con dato`,
        { trend: ui.area ? null : trendSvg(networkTrend("mean_delay_s")) }),
      tile("Puntuales", onTime === null ? "—" : `${nf1.format(onTime)} %`, "< 3 min Cerc. · < 5 min AV/LD",
        { trend: ui.area ? null : trendSvg(networkTrend("pct_on_time")) }),
      tile("Anomalías activas", nf.format(active), active ? "ver la lista" : "todo en orden"),
      tile("Cancelados", nf.format(canceled), "publicados por Renfe"),
    );
  }

  // -------------------------------------------------------------------- líneas
  function sparkline(history) {
    const s = svg("svg", { class: "spark", viewBox: "0 0 84 22", "aria-hidden": "true" });
    const pts = history.filter((p) => p.v !== null);
    if (pts.length < 2) return s;
    const vs = pts.map((p) => p.v);
    const max = Math.max(60, ...vs), min = Math.min(0, ...vs);
    const x = (i) => 2 + (i * 80) / (pts.length - 1);
    const y = (v) => 19 - ((v - min) / (max - min || 1)) * 16;
    s.append(svg("path", { d: pts.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p.v).toFixed(1)}`).join(""), fill: "none", stroke: css("--neutral"), "stroke-width": 1.5, "stroke-linejoin": "round" }));
    s.append(svg("circle", { cx: x(pts.length - 1), cy: y(pts[pts.length - 1].v), r: 2.5, fill: css("--accent") }));
    return s;
  }
  function renderLines(lines) {
    const q = ui.search.trim().toLowerCase();
    const rows = lines.filter(matches).filter((l) => !q || `${l.line} ${l.area}`.toLowerCase().includes(q))
      .sort((a, b) => (a.line === UNKNOWN_LINE) - (b.line === UNKNOWN_LINE));
    const list = $("#lines");
    if (!rows.length) { list.replaceChildren(el("li", { class: "empty" }, "No hay líneas con trenes para este filtro.")); return; }
    list.replaceChildren(...rows.map((l) => {
      const s = status(l.mean_delay_s, l.network);
      const color = lineColor(l.color);
      const selected = ui.line && ui.line.line === l.line && ui.line.area === l.area;
      return el("li", {
        class: selected ? "sel" : "", tabindex: "0",
        title: "Pulsa para ver solo esta línea en el mapa",
        onclick: () => { ui.line = selected ? null : { line: l.line, area: l.area }; renderAll(); },
        onkeydown: (e) => { if (e.key === "Enter") e.currentTarget.click(); },
      },
        el("span", { class: "line-name" },
          el("span", { class: "badge", style: `background:${color};color:${textOn(color)}` }, l.line),
          el("span", { class: "area" }, l.area)),
        el("span", {}, nf.format(l.n_trains) + (l.n_canceled ? ` +${l.n_canceled}✕` : "")),
        el("span", { class: "delay" }, el("i", { class: "dot", style: `background:${s.color}` }), fmtDelay(l.mean_delay_s)),
        el("span", {}, l.pct_on_time === null ? "—" : `${Math.round(l.pct_on_time)} %`),
        el("span", {}, sparkline(l.history || [])));
    }));
  }

  function renderAreas(lines) {
    const sel = $("#area");
    const areas = [...new Set(lines.filter((l) => !ui.net || l.network === ui.net).map((l) => l.area))]
      .filter((a) => a && a !== "Sin cruzar con horario").sort((a, b) => a.localeCompare(b, "es"));
    const key = areas.join("|");
    if (sel.dataset.key !== key) {
      sel.replaceChildren(el("option", { value: "" }, "Todos los núcleos"), ...areas.map((a) => el("option", { value: a }, a)));
      sel.dataset.key = key;
    }
    sel.value = areas.includes(ui.area) ? ui.area : "";
    ui.area = sel.value;
  }

  // ---------------------------------------------------------- anomalías y avisos
  function renderAnomalies(live) {
    const byId = new Map();
    for (const a of ui.anomalies) byId.set(a.anomaly_id, { ...a, status: a.resolved_at ? "resolved" : "active" });
    for (const a of live) byId.set(a.anomaly_id, { ...byId.get(a.anomaly_id), ...a });
    const items = [...byId.values()].filter((a) => (!ui.net || a.network === ui.net) && (!ui.area || a.area === ui.area))
      .sort((a, b) => (b.status === "active") - (a.status === "active") || String(b.started_at).localeCompare(String(a.started_at)))
      .slice(0, 60);
    const activeN = items.filter((a) => a.status === "active").length;
    $("#anom-count").textContent = `${activeN} activas · ${items.length - activeN} resueltas`;
    const list = $("#anomalies");
    if (!items.length) { list.replaceChildren(el("li", { class: "empty" }, "Sin anomalías para este filtro.")); return; }
    list.replaceChildren(...items.map((a) => {
      const k = ANOM[a.type] || { icon: "?", label: a.type, color: "--neutral" };
      const when = a.status === "active" ? `desde las ${hhmm.format(new Date(a.started_at))}` :
        `${hhmm.format(new Date(a.started_at))}–${a.resolved_at ? hhmm.format(new Date(a.resolved_at)) : ""}`;
      return el("li", { class: a.status === "active" ? "" : "resolved" },
        el("span", { class: "ico", style: `background:${css(k.color)}`, "aria-hidden": "true" }, k.icon),
        el("div", {}, el("div", {}, a.message),
          el("div", { class: "meta" }, `${k.label} · ${a.status === "active" ? "activa" : "resuelta"} · ${when}`)));
    }));
  }
  function renderAlerts(alerts) {
    const rows = alerts.filter((a) => !ui.net || a.network === ui.net)
      .sort((a, b) => String(b.active_start).localeCompare(String(a.active_start)));
    $("#alerts-count").textContent = `${rows.length} activos`;
    const list = $("#alerts");
    if (!rows.length) { list.replaceChildren(el("li", { class: "empty" }, "Renfe no publica avisos ahora mismo.")); return; }
    list.replaceChildren(...rows.slice(0, 80).map((a) => el("li", {},
      el("span", { class: "ico", style: `background:${css("--neutral")}`, "aria-hidden": "true" }, "i"),
      el("div", {},
        el("div", {}, ...(a.lines || []).slice(0, 6).map((l) => el("span", { class: "tag" }, l)), a.description || a.header || "(sin texto)"),
        el("div", { class: "meta" }, `${NET_LABEL[a.network] || a.network}${a.active_start ? ` · desde ${dayHhmm.format(new Date(a.active_start))}` : ""}`)))));
  }

  // ------------------------------------------------------------------ calidad
  function renderQuality(q, counters) {
    const feeds = Object.keys(q).filter((f) => !ui.net || f.startsWith(ui.net === "ld" ? "ld_" : "cercanias_")).sort();
    const head = el("tr", {}, ...["Fuente", "Snapshots", "Eventos", "Válidos", "Rechazados", "Retraso del feed", "Reglas que saltan"].map((h) => el("th", {}, h)));
    const rows = feeds.map((f) => {
      const s = q[f];
      const issues = [
        ...Object.entries(s.errors).map(([k, v]) => `✕ ${k.replaceAll("_", " ")} (${nf.format(v)})`),
        ...Object.entries(s.warnings).map(([k, v]) => `! ${k.replaceAll("_", " ")} (${nf.format(v)})`),
      ];
      return el("tr", {},
        el("td", {}, FEED_LABEL[f] || f), el("td", {}, nf.format(s.snapshots)), el("td", {}, nf.format(s.n_in)),
        el("td", {}, `${nf1.format(s.pct_valid)} %`, el("span", { class: "meter", "aria-hidden": "true" }, el("b", { style: `width:${s.pct_valid}%` }))),
        el("td", {}, nf.format(s.n_rejected)), el("td", {}, `${Math.round(s.max_lag_s)} s`),
        el("td", { class: "issues" }, issues.length ? issues.join(" · ") : "ninguna"));
    });
    $("#quality").replaceChildren(el("thead", {}, head), el("tbody", {}, ...rows));
    const c = counters || {};
    $("#counters").textContent = `Procesador: ${nf.format(c.eventos_retraso || 0)} eventos de retraso · ` +
      `${nf.format(c.viajes_de_dias_pasados || 0)} de trenes de días pasados (excluidos) · ` +
      `${nf.format(c.duplicados_ignorados || 0)} duplicados ignorados · ${nf.format(c.eventos_tardios || 0)} tardíos.`;
  }

  // ---------------------------------------------------------------- gráfico SVG
  function niceStep(raw) {
    const p = Math.pow(10, Math.floor(Math.log10(Math.max(raw, 1e-9))));
    const n = raw / p;
    return (n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10) * p;
  }
  function lineChart(container, series, { yFmt, yMax = null, yUnit = "" }) {
    container.replaceChildren();
    const all = series.flatMap((s) => s.points);
    if (!all.length) { container.append(el("p", { class: "muted small" }, "Todavía no hay histórico para este periodo.")); return; }
    const legendBox = series.length > 1 ? el("div", { class: "chart-legend" }, ...series.map((s) =>
      el("span", {}, el("i", { class: "key", style: `background:${s.color}` }), s.name))) : null;
    if (legendBox) container.append(legendBox);
    const W = container.clientWidth || 600;
    const H = Math.max(140, (container.clientHeight || 230) - (legendBox ? legendBox.offsetHeight + 4 : 0));
    const m = { t: 8, r: 12, b: 22, l: 40 };
    const s = svg("svg", { viewBox: `0 0 ${W} ${H}`, role: "img", "aria-label": container.dataset.label || "gráfico" });
    s.style.height = `${H}px`;
    const t0 = Math.min(...all.map((p) => p.t)), t1 = Math.max(...all.map((p) => p.t));
    let vmax = yMax ?? Math.max(1, ...all.map((p) => p.v));
    const step = yMax ? 25 : niceStep(vmax / 4);
    vmax = yMax ?? Math.ceil(vmax / step) * step;
    const x = (t) => m.l + ((t - t0) / Math.max(1, t1 - t0)) * (W - m.l - m.r);
    const y = (v) => H - m.b - (Math.max(0, v) / vmax) * (H - m.t - m.b);
    const g = svg("g", { class: "axis" });
    for (let v = 0; v <= vmax + 1e-9; v += step) {
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
        d += `${!prev || p.t - prev.t > 11 * 60 * 1000 ? "M" : "L"}${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`;
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
      const tt = t0 + ((((ev.clientX - r.left) / r.width) * W - m.l) / (W - m.l - m.r)) * (t1 - t0);
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
    container.append(s);
  }
  function table(target, series, fmt) {
    const times = [...new Set(series.flatMap((s) => s.points.map((p) => p.t)))].sort((a, b) => b - a);
    const head = el("tr", {}, el("th", {}, "Hora"), ...series.map((s) => el("th", {}, s.name)));
    const rows = times.map((t) => el("tr", {}, el("td", {}, dayHhmm.format(new Date(t))),
      ...series.map((s) => { const p = s.points.find((q) => q.t === t); return el("td", {}, p ? fmt(p.v) : "—"); })));
    target.replaceChildren(el("table", {}, el("thead", {}, head), el("tbody", {}, ...rows)));
  }
  function renderCharts() {
    const nets = ui.net ? [ui.net] : ["cercanias", "ld"];
    const mk = (field) => nets.map((n) => ({
      name: NET_LABEL[n], color: css(NET_SERIES[n]),
      points: ui.kpis.filter((r) => r.network === n && r[field] !== null && r[field] !== undefined)
        .map((r) => ({ t: new Date(r.window_start).getTime(), v: field === "mean_delay_s" ? r[field] / 60 : r[field] })),
    })).filter((s) => s.points.length);
    const delay = mk("mean_delay_s"), ontime = mk("pct_on_time");
    $("#chart-delay").dataset.label = "Retraso medio por red en minutos";
    $("#chart-ontime").dataset.label = "Porcentaje de trenes puntuales por red";
    lineChart($("#chart-delay"), delay, { yFmt: (v, exact) => (exact ? nf1.format(v) : nf.format(v)), yUnit: " min" });
    lineChart($("#chart-ontime"), ontime, { yFmt: (v, exact) => (exact ? nf1.format(v) : nf.format(v)), yMax: 100, yUnit: " %" });
    table($("#table-delay"), delay, (v) => `${nf1.format(v)} min`);
    table($("#table-ontime"), ontime, (v) => `${nf1.format(v)} %`);
  }

  function renderStops(rows) {
    const list = $("#stops");
    if (!rows.length) { list.replaceChildren(el("li", { class: "empty" }, "Sin datos de estaciones en la última hora.")); return; }
    const max = Math.max(60, ...rows.map((r) => r.mean_delay_s));
    list.replaceChildren(...rows.slice(0, 12).map((r) => el("li", {},
      el("span", { class: "name", title: `${r.stop_name || r.stop_id} · ${nf.format(r.n_obs)} pasos de tren` }, r.stop_name || r.stop_id),
      el("span", { class: "track" }, el("span", { class: "fill", style: `width:${Math.max(0, (100 * r.mean_delay_s) / max)}%` })),
      el("span", { class: "val" }, fmtDelay(r.mean_delay_s)))));
  }

  function renderPrediction(p) {
    const box = $("#prediction");
    if (!p || !p.available) {
      box.replaceChildren(el("p", { class: "pred-empty" },
        el("strong", {}, "Modelo en preparación. "),
        "Se entrenará con varios días de datos reales para estimar el retraso de cada tren dentro de 15 minutos, ",
        "y se comparará con la respuesta ingenua («el retraso actual se mantiene») con cifras honestas."));
      return;
    }
    const m = p.metrics || {};
    const rows = (p.trains || []).filter((t) => !ui.net || t.network === ui.net).slice(0, 10);
    box.replaceChildren(
      el("p", { class: "muted small" }, `MAE del modelo ${nf1.format(m.mae_model_min)} min frente a ${nf1.format(m.mae_baseline_min)} min del baseline, evaluado con ${nf.format(m.n_test || 0)} casos de días no vistos.`),
      el("div", { class: "tbl-wrap" }, el("table", {},
        el("thead", {}, el("tr", {}, ...["Tren", "Línea", "Ahora", "En 15 min"].map((h) => el("th", {}, h)))),
        el("tbody", {}, ...rows.map((t) => el("tr", {}, el("td", {}, t.trip_id), el("td", {}, t.line), el("td", {}, fmtDelay(t.delay_s)), el("td", {}, fmtDelay(t.pred_delay_s))))))));
  }

  // ---------------------------------------------------------------- orquestación
  function currentTrains() {
    return ui.live ? ui.live.trains.filter((t) => !t.stale).filter(matches) : [];
  }
  let stopsCache = [];
  function renderAll() {
    const s = ui.live;
    if (!s) return;
    renderAreas(s.lines);
    const trains = currentTrains();
    renderTiles(trains, s.anomalies.filter(matches));
    renderMap(trains);
    renderTrainCard();
    renderLines(s.lines);
    renderAnomalies(s.anomalies);
    renderAlerts(s.alerts);
    renderQuality(s.quality, s.counters);
    $("#filter-note").textContent = ui.line ? `Mapa filtrado: línea ${ui.line.line} (${ui.line.area}). Púlsala otra vez para quitar el filtro.` : "";
  }
  function renderHistory() {
    renderCharts();
    renderStops(stopsCache);
    if (ui.live) { renderAnomalies(ui.live.anomalies); renderTiles(currentTrains(), ui.live.anomalies.filter(matches)); }
  }

  async function loadState() {
    const live = $("#live");
    try {
      ui.live = await source.state();
      if (source.mode === "replay") {
        live.className = "live is-replay";
        live.querySelector("span").textContent = `Repetición · ${hhmm.format(new Date(ui.live.generated_at))}`;
      } else {
        live.className = `live ${ui.live.stale ? "stale" : "ok"}`;
        live.querySelector("span").textContent = ui.live.stale
          ? `Datos con retraso (${ago(ui.live.generated_at)})` : `En directo · ${ago(ui.live.generated_at)}`;
      }
      renderAll();
    } catch (e) {
      console.warn(e);
      live.className = "live";
      live.querySelector("span").textContent = "Sin conexión con el procesador";
    }
  }
  async function loadHistory() {
    try {
      const [k, st, an, pr] = await Promise.all([source.kpis(ui.hours), source.stops(ui.net), source.anomalies(), source.predictions()]);
      ui.kpis = k; ui.anomalies = an; stopsCache = st;
      renderHistory(); renderPrediction(pr);
    } catch (e) { console.warn(e); }
  }

  // ------------------------------------------------------------------ repetición
  async function startReplay() {
    await source.init();
    const m = source.manifest;
    const frames = m.frames;
    $("#replay").hidden = false;
    const since = new Date(m.since), until = new Date(m.until);
    $("#replay-label").textContent = `Datos reales del ${dayLong.format(since)}, de ${hhmm.format(since)} a ${hhmm.format(until)} (hora de Madrid), ` +
      "procesados con el mismo código que en directo.";
    const slider = $("#replay-slider");
    slider.max = String(frames.length - 1);
    const play = $("#replay-play");
    let timer = null;
    let busy = false;
    async function go(i) {
      if (busy) return;
      busy = true;
      try {
        await source.load(i);
        slider.value = String(i);
        $("#replay-time").textContent = hhmm.format(new Date(frames[i].t));
        await loadState();
        await loadHistory();
      } finally { busy = false; }
    }
    function schedule() {
      clearInterval(timer);
      if (play.classList.contains("paused")) return;
      timer = setInterval(() => go((source.idx + 1) % frames.length), Number($("#replay-speed").value));
    }
    play.addEventListener("click", () => {
      play.classList.toggle("paused");
      play.setAttribute("aria-label", play.classList.contains("paused") ? "Reproducir" : "Pausar");
      schedule();
    });
    $("#replay-speed").addEventListener("change", schedule);
    slider.addEventListener("input", () => go(Number(slider.value)));
    // Empieza en el momento con más trenes, para que la primera impresión sea representativa.
    const start = frames.reduce((best, f, i) => (f.trains > frames[best].trains ? i : best), 0);
    await go(start);
    schedule();
  }

  // ------------------------------------------------------------------ eventos
  document.querySelectorAll("[data-net]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("[data-net]").forEach((x) => x.classList.toggle("on", x === b));
    ui.net = b.dataset.net; ui.line = null; ui.area = "";
    renderAll(); loadHistory();
  }));
  $("#area").addEventListener("change", (e) => { ui.area = e.target.value; ui.line = null; renderAll(); });
  document.querySelectorAll("[data-hours]").forEach((b) => b.addEventListener("click", () => {
    document.querySelectorAll("[data-hours]").forEach((x) => x.classList.toggle("on", x === b));
    ui.hours = Number(b.dataset.hours); loadHistory();
  }));
  $("#line-search").addEventListener("input", (e) => { ui.search = e.target.value; if (ui.live) renderLines(ui.live.lines); });
  let rt; window.addEventListener("resize", () => { clearTimeout(rt); rt = setTimeout(renderCharts, 150); });

  renderLegend();
  if (source.mode === "replay") {
    startReplay().catch((e) => { console.error(e); $("#live span").textContent = "No se pudo cargar la repetición"; });
  } else {
    loadState(); loadHistory();
    setInterval(loadState, 15000);
    setInterval(loadHistory, 60000);
  }
})();
