(function () {
  "use strict";

  const REFRESH = (window.MERG_REFRESH || 60) * 1000;
  const DEFAULT_WINDOW = window.MERG_WINDOW || 8;
  let state = {
    events: [],
    recent: [],
    summary: {},
    sources: [],
    threats: [],
    filters: {
      hours: DEFAULT_WINDOW,
      asset_class: "all",
      severity: "all",
      classification: "all",
    },
    loading: true,
    expandedId: null,
    _georiskLoaded: false,
  };

  const $ = (s) => document.querySelector(s);
  const $$ = (s) => document.querySelectorAll(s);

  async function fetchJSON(url) {
    try {
      const r = await fetch(url);
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) {
      console.error("Fetch:", url, e);
      return null;
    }
  }

  function buildQuery() {
    const f = state.filters;
    const p = new URLSearchParams();
    p.set("hours", f.hours);
    if (f.asset_class !== "all") p.set("asset_class", f.asset_class);
    if (f.severity !== "all") p.set("severity", f.severity);
    if (f.classification !== "all") p.set("classification", f.classification);
    return p.toString();
  }

  async function refresh() {
    state._georiskLoaded = false;
    const q = buildQuery();
    const [evD, sumD, hlD] = await Promise.all([
      fetchJSON("/api/events?" + q),
      fetchJSON("/api/summary?" + q),
      fetchJSON("/health"),
    ]);
    if (evD) {
      state.events = evD.events || [];
      state.recent = evD.recent_events || [];
    }
    if (sumD) state.summary = sumD;
    if (hlD) state.sources = hlD.sources || [];
    state.loading = false;
    render();
    fetchJSON("/api/georisk").then((geoD) => {
      state._georiskLoaded = true;
      if (geoD) state.threats = geoD.threats || [];
      renderThreats();
    });
  }

  function render() {
    renderHeader();
    renderBanner();
    renderThreats();
    renderSummary();
    renderVerdict();
    renderDistortion();
    renderEvents();
    renderSources();
    renderLoading();
  }

  function renderHeader() {
    const ts = state.summary.updated_utc;
    const el = $("#header-updated");
    if (el && ts) el.textContent = new Date(ts).toLocaleTimeString();
    const dot = $("#header-dot");
    if (!dot) return;
    const v = state.summary.verdict || "";
    dot.className = "dot " + (v === "Yes" ? "dot-green" : v === "Caution" ? "dot-amber" : "dot-red");
  }

  function renderBanner() {
    const el = $("#banner-alert");
    if (!el) return;
    const b = state.summary.banner_alert;
    if (b) {
      el.textContent = b;
      el.className = "banner-alert active " + (b.includes("EXTREME") ? "banner-extreme" : "banner-caution");
    } else {
      el.className = "banner-alert";
    }
  }

  function renderThreats() {
    const el = $("#threat-panel");
    if (!el) return;
    if (!state.threats.length) {
      if (!state._georiskLoaded) {
        el.innerHTML = '<div class="threat-card" style="opacity:0.5;justify-content:center;padding:1rem"><span class="spinner" style="width:16px;height:16px;border-width:2px"></span><span style="font-size:.75rem;color:var(--text-muted);margin-left:.6rem">Loading geopolitical threat data…</span></div>';
      } else {
        el.innerHTML = "";
      }
      return;
    }

    el.innerHTML = state.threats.map((t) => {
      const status = t.source_status || "live";
      const delayed = status !== "live";
      const lev = (t.level || "Low").toLowerCase();
      const cls = delayed ? "tg-assessing" : "tg-" + lev;
      const barCls = delayed ? "tb-assessing" : "tb-" + lev;
      const c = t.components || {};
      const comps = [
        "Vol ratio: " + (c.volume_ratio ?? 0) + "/30",
        "Abs vol: " + (c.absolute_volume ?? 0) + "/25",
        "Severity: " + (c.severity_keywords ?? 0) + "/25",
        "Background: " + (c.background_context ?? 0) + "/16",
        "Recency: " + (c.recency ?? 0) + "/20",
        "Factors: " + (c.factor_diversity ?? 0) + "/10",
        "Breadth: " + (c.source_breadth ?? 0) + "/10",
        "Official: " + (c.official_signals ?? 0) + "/6",
        "Esc: " + (c.esc_count ?? 0) + " articles",
        "Deesc: " + (c.deesc_count ?? 0) + " articles",
        "Coverage: " + (t.coverage_articles || ((c.esc_count || 0) + (c.deesc_count || 0))) + " articles",
      ];
      if (delayed && t.source_note) comps.unshift("Status: " + t.source_note);
      const compHtml = comps.map((x) => '<span class="threat-comp">' + esc(x) + '</span>').join("");
      const freshnessHtml = renderThreatFreshness(t);
      const sourcesHtml = renderThreatSources(t.signal_sources || []);
      const factorsHtml = renderRiskFactors(t.risk_factors || []);
      const hdls = (t.top_headlines || []).map((h) => {
        const src = h.source_family ? ' · ' + h.source_family.replace(/_/g, ' ') : '';
        return '<li><a href="' + esc(h.url) + '" target="_blank" rel="noopener">' + esc(h.title) + '</a> <span style="color:var(--text-dim)">' + esc(h.domain) + esc(src) + '</span></li>';
      }).join("");
      const gaugeScore = delayed ? "—" : t.score;
      const gaugeLabel = delayed ? "ASSESSING" : t.level;
      const barWidth = delayed ? 12 : t.score;
      const delayedBadge = delayed ? '<span class="threat-comp">' + esc(status === "stale" ? "Assessing from recent coverage" : "Assessing live signal") + '</span>' : "";

      return '<div class="threat-card">'
        + '<div class="threat-gauge ' + cls + '">'
        + '<div class="tg-score">' + gaugeScore + '</div>'
        + '<div class="tg-label">' + esc(gaugeLabel) + '</div>'
        + '</div>'
        + '<div class="threat-body">'
        + '<div class="tb-title">' + esc(t.label) + ' <span class="threat-v2-badge">v2.3.1</span></div>'
        + '<div class="threat-bar-track"><div class="threat-bar-fill ' + barCls + '" style="width:' + barWidth + '%"></div></div>'
        + freshnessHtml
        + '<div class="tb-detail">' + esc(t.detail) + '</div>'
        + sourcesHtml
        + factorsHtml
        + '<div class="threat-components">' + delayedBadge + compHtml + '</div>'
        + (hdls ? '<details class="threat-headlines"><summary>Latest coverage</summary><ul>' + hdls + '</ul></details>' : '')
        + '</div>'
        + '</div>';
    }).join("");
  }


  function renderThreatFreshness(t) {
    const updated = fmtAgo(t.updated_utc);
    const next = fmtUntil(t.next_live_reassess_utc);
    const sourceStatus = t.source_status === "live" ? "Live" : "Assessing";
    const note = t.last_live_utc && t.source_status !== "live" ? " · last live " + fmtAgo(t.last_live_utc) : "";
    return '<div class="threat-freshness"><span>' + esc(sourceStatus) + '</span><span>Updated ' + esc(updated || 'now') + '</span><span>Next live reassessment ' + esc(next || 'soon') + '</span><span>' + esc(note ? note.slice(3) : '') + '</span></div>'.replace('<span></span>', '');
  }

  function renderThreatSources(sources) {
    if (!sources || !sources.length) return "";
    const items = sources.map((s) => {
      const cls = s.status === "live" ? "src-live" : "src-assessing";
      const count = (s.count || 0) + " hit" + ((s.count || 0) === 1 ? "" : "s");
      return '<span class="threat-source-chip ' + cls + '">' + esc(s.name) + ' · ' + esc(count) + '</span>';
    }).join("");
    return '<div class="threat-source-row"><div class="tf-title" style="margin-bottom:.35rem">Signal sources</div><div class="threat-source-list">' + items + '</div></div>';
  }

  function renderRiskFactors(factors) {
    if (!factors || !factors.length) return "";
    const items = factors.map((f) => {
      const cls = "rf-" + (f.kind || "context");
      const age = fmtAgo(f.latest_utc);
      const meta = [(f.count || 0) + " hit" + ((f.count || 0) === 1 ? "" : "s"), (f.source_count || 0) + " source" + ((f.source_count || 0) === 1 ? "" : "s"), age].filter(Boolean).join(" · ");
      const titleAttr = f.latest_title ? ' title="' + esc(f.latest_title) + '"' : "";
      const latestTitle = f.latest_title ? '<div class="rf-title">' + esc(f.latest_title) + '</div>' : '';
      return '<li class="risk-factor ' + cls + '"' + titleAttr + '>'
        + '<div class="rf-name">' + esc(f.label) + '</div>'
        + latestTitle
        + '<div class="rf-meta">' + esc(meta) + '</div>'
        + '</li>';
    }).join("");
    return '<div class="threat-factors"><div class="tf-title">Recent risk factors</div><ul class="risk-factor-list">' + items + '</ul></div>';
  }

  function kindLabel(kind) {
    if (kind === "escalation") return "Escalation";
    if (kind === "market") return "Market impact";
    if (kind === "pressure") return "Pressure";
    if (kind === "deescalation") return "De-escalation";
    return "Context";
  }

  function fmtAgo(iso) {
    if (!iso) return "";
    const d = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (d < 1) return "just now";
    if (d < 60) return d + "m ago";
    if (d < 1440) return Math.round(d / 60) + "h ago";
    return Math.round(d / 1440) + "d ago";
  }


  function fmtUntil(iso) {
    if (!iso) return "soon";
    const mins = Math.round((new Date(iso).getTime() - Date.now()) / 60000);
    if (mins <= 1) return "in <1m";
    if (mins < 60) return "in " + mins + "m";
    if (mins < 1440) return "in " + Math.round(mins / 60) + "h";
    return "in " + Math.round(mins / 1440) + "d";
  }

  function renderSummary() {
    const s = state.summary;
    setText("#sum-count", s.event_count ?? 0);
    const cE = $("#sum-count");
    if (cE) {
      const n = s.event_count || 0;
      cE.className = "value " + (n === 0 ? "val-green" : n <= 3 ? "val-amber" : "val-red");
    }
    setText("#sum-caution", s.highest_caution || "Low");
    const caE = $("#sum-caution");
    if (caE) {
      const c = (s.highest_caution || "Low").toLowerCase();
      caE.className = "value val-" + (c === "extreme" ? "extreme" : c === "high" ? "red" : c === "moderate" ? "amber" : "green");
    }
    setText("#sum-env", s.environment_label || "Normal");
    setText("#sum-window", state.filters.hours + "h window");
  }

  function renderVerdict() {
    const el = $("#verdict-card");
    if (!el) return;
    const v = state.summary.verdict || "Yes";
    const detail = state.summary.verdict_detail || "";
    const cov = state.summary.source_coverage || "full";
    const icon = v === "Yes" ? "✅" : v === "Caution" ? "⚠️" : "🛑";
    const headline = v === "Yes"
      ? "Technicals usable — no major event risk"
      : v === "Caution"
        ? "Event-sensitive — use caution with technicals"
        : "High event risk — technicals may be unreliable";
    const cls = v === "Yes" ? "verdict-yes" : v === "Caution" ? "verdict-caution" : "verdict-no";
    const covBadge = cov !== "full" ? '<span class="verdict-cov cov-' + cov + '">Source coverage: ' + cov + '</span>' : "";
    el.className = "verdict-card " + cls;
    el.innerHTML = '<div class="verdict-icon">' + icon + '</div><div class="verdict-body"><div class="headline">' + esc(headline) + '</div><div class="detail">' + esc(detail) + '</div>' + covBadge + '</div>';
  }

  function renderDistortion() {
    const el = $("#distortion-section");
    if (!el) return;
    const items = (state.summary.highest_distortion_events || []).filter((e) => e.impact_score >= 35);
    if (!items.length) {
      el.className = "distortion-section";
      return;
    }
    el.className = "distortion-section active";
    const h = items.map((e) => {
      const lev = e.caution_level.toLowerCase();
      const cls = lev === "extreme" ? "d-extreme" : lev === "high" ? "d-high" : "d-moderate";
      const ts = e.classification === "ongoing" ? '<span style="color:#ef4444">LIVE</span>' : fmtCountdown(e.start_time_utc);
      return '<li class="distortion-item"><span class="d-badge ' + cls + '">' + esc(e.caution_level) + '</span><span class="d-title">' + esc(e.title) + '</span><span class="d-time">' + ts + '</span></li>';
    }).join("");
    el.innerHTML = '<h2>⚡ Do Not Trust Technicals</h2><ul class="distortion-list">' + h + '</ul>';
  }

  function fmtCountdown(iso) {
    if (!iso) return "—";
    const d = Math.round((new Date(iso).getTime() - Date.now()) / 60000);
    if (d > 1440) return Math.round(d / 1440) + "d";
    if (d > 60) return Math.round(d / 60) + "h";
    if (d > 0) return d + "m";
    if (d > -60) return Math.abs(d) + "m ago";
    if (d > -1440) return Math.abs(Math.round(d / 60)) + "h ago";
    return Math.abs(Math.round(d / 1440)) + "d ago";
  }

  function fmtParts(iso) {
    if (!iso) return { n: "—", u: "" };
    const d = Math.round((new Date(iso).getTime() - Date.now()) / 60000);
    if (d > 1440) return { n: Math.round(d / 1440), u: "days" };
    if (d > 60) return { n: Math.round(d / 60), u: "hrs" };
    if (d > 0) return { n: d, u: "min" };
    if (d === 0) return { n: "NOW", u: "" };
    if (d > -60) return { n: Math.abs(d) + "m", u: "ago" };
    if (d > -1440) return { n: Math.abs(Math.round(d / 60)) + "h", u: "ago" };
    return { n: Math.abs(Math.round(d / 1440)) + "d", u: "ago" };
  }

  function renderEvents() {
    const el = $("#event-list");
    if (!el) return;
    const evts = state.events;
    const recent = state.recent;
    let html = "";

    if (!evts.length && !recent.length && !state.loading) {
      html = '<div class="empty-state"><div class="es-icon">🛡️</div><p>No event risk detected. Technicals usable.</p></div>';
    } else {
      if (evts.length) {
        html += evts.map((e) => cardHTML(e, false)).join("");
      } else if (!state.loading) {
        html += '<div class="empty-state" style="padding:1.5rem"><p>No upcoming events in the next ' + state.filters.hours + 'h. Technicals usable.</p></div>';
      }
      if (recent.length) {
        html += '<div class="recent-header">Recent events (completed)</div>';
        html += recent.map((e) => cardHTML(e, true)).join("");
      }
    }
    el.innerHTML = html;

    el.querySelectorAll(".event-card").forEach((card) => {
      card.addEventListener("click", () => {
        const id = card.dataset.id;
        const d = card.querySelector(".event-detail");
        if (!d) return;
        if (state.expandedId === id) {
          d.classList.remove("open");
          state.expandedId = null;
        } else {
          el.querySelectorAll(".event-detail.open").forEach((x) => x.classList.remove("open"));
          d.classList.add("open");
          state.expandedId = id;
        }
      });
    });
  }

  function cardHTML(e, dimmed) {
    const lev = (e.caution_level || "Low").toLowerCase();
    const isOngoing = e.classification === "ongoing";
    const cd = isOngoing ? { n: "LIVE", u: "" } : fmtParts(e.start_time_utc);
    const aMap = {
      "Stand aside until event passes": "action-stand-aside",
      "Avoid new entries near event": "action-avoid-entries",
      "Use caution": "action-use-caution",
      "Technicals usable": "action-technicals-ok",
    };
    const aCls = aMap[e.suggested_action] || "action-technicals-ok";
    const t = new Date(e.start_time_utc);
    const tStr = t.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
    const inst = (e.affected_instruments || []).map((i) => '<span class="inst-tag">' + esc(i) + '</span>').join("");
    let cW = "—";
    if (e.caution_window_start_utc && e.caution_window_end_utc) {
      const s2 = new Date(e.caution_window_start_utc);
      const e2 = new Date(e.caution_window_end_utc);
      cW = s2.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" }) + " → " + e2.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
    }
    const dim = dimmed ? ' style="opacity:0.5"' : "";
    const ongoingCls = isOngoing ? " event-ongoing" : "";
    const cdCls = isOngoing ? " cd-live" : "";
    const sinceStr = isOngoing ? '<span class="cd-since">since ' + t.toLocaleDateString(undefined, { month: "short", day: "numeric" }) + '</span>' : "";

    return '<div class="event-card sev-' + lev + ongoingCls + '"' + dim + ' data-id="' + esc(e.id) + '"><div class="ec-countdown' + cdCls + '"><span class="cd-num">' + cd.n + '</span><span class="cd-unit">' + cd.u + '</span>' + sinceStr + '</div><div class="ec-body"><div class="ec-title">' + esc(e.title) + '</div><div class="ec-badges"><span class="badge badge-' + lev + '">' + esc(e.caution_level) + ' · ' + e.impact_score + '</span><span class="badge badge-' + e.asset_class + '">' + esc(e.asset_class) + '</span><span class="badge badge-' + e.classification + '">' + esc(e.classification) + '</span></div><div class="ec-why">' + esc(e.why_it_matters || "") + '</div><div class="event-detail"><dl class="detail-grid"><dt>Time</dt><dd>' + tStr + '</dd><dt>Category</dt><dd>' + esc(e.category) + '</dd><dt>Event type</dt><dd>' + esc(e.event_type) + '</dd><dt>Source</dt><dd><a href="' + esc(e.source_url) + '" target="_blank" rel="noopener">' + esc(e.source_name) + '</a></dd><dt>Confidence</dt><dd>' + Math.round((e.confidence || 0) * 100) + '%</dd><dt>Caution window</dt><dd>' + cW + '</dd><dt>Description</dt><dd>' + esc(e.description || "—") + '</dd><dt>Instruments</dt><dd><div class="instruments-list">' + (inst || "—") + '</div></dd></dl></div></div><div class="ec-action ' + aCls + '">' + esc(e.suggested_action) + '</div></div>';
  }

  function renderSources() {
    const el = $("#source-grid");
    if (!el) return;
    el.innerHTML = state.sources.map((s) => {
      const st = s.status || "pending";
      const cls = !s.enabled ? "source-off" : st === "needs_key" ? "source-key" : st === "healthy" ? "source-ok" : st === "pending" ? "source-pending" : "source-fail";
      const lb = s.name.replace("Adapter", "");
      const ct = !s.enabled ? " off" : st === "needs_key" ? " key needed" : st === "pending" ? " waiting" : " (" + s.event_count + ")";
      return '<div class="source-chip ' + cls + '"><span class="s-dot"></span>' + esc(lb) + ct + '</div>';
    }).join("");
  }

  function renderLoading() {
    const el = $("#loading");
    if (el) el.className = state.loading ? "loading-overlay active" : "loading-overlay";
  }

  function setText(sel, val) {
    const el = $(sel);
    if (el) el.textContent = val;
  }

  function esc(s) {
    if (!s) return "";
    const d = document.createElement("div");
    d.textContent = String(s);
    return d.innerHTML;
  }

  function bindFilters() {
    $$('[data-hours]').forEach((btn) => {
      btn.addEventListener('click', () => {
        $$('[data-hours]').forEach((b) => b.classList.remove('btn-active'));
        btn.classList.add('btn-active');
        state.filters.hours = parseInt(btn.dataset.hours, 10);
        doRefresh();
      });
    });
    ["f-asset", "f-severity", "f-class"].forEach((id) => {
      const el = $("#" + id);
      if (!el) return;
      const key = id === "f-asset" ? "asset_class" : id === "f-severity" ? "severity" : "classification";
      el.addEventListener("change", () => {
        state.filters[key] = el.value;
        doRefresh();
      });
    });
  }

  let timer = null;
  async function doRefresh() {
    state.loading = true;
    renderLoading();
    await refresh();
  }

  async function init() {
    bindFilters();
    const db = document.querySelector('[data-hours="' + DEFAULT_WINDOW + '"]');
    if (db) db.classList.add("btn-active");
    await doRefresh();
    timer = setInterval(doRefresh, REFRESH);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
