(function () {
  const statusEl = document.getElementById("db-status");
  const summaryEl = document.getElementById("db-summary");
  const goldInfoEl = document.getElementById("db-gold-info");
  const tableWrap = document.getElementById("db-table-wrap");
  const tbody = document.getElementById("db-matches-body");
  const loadMoreBtn = document.getElementById("db-load-more");
  const refreshBtn = document.getElementById("db-refresh");
  const lastRefreshEl = document.getElementById("db-last-refresh");

  const pageLimit = 25;
  let offset = 0;
  let loading = false;
  let pollTimer = null;
  /** @type {Map<string, object>} */
  const detailCache = new Map();

  const fetchOpts = { headers: { Accept: "application/json" }, cache: "no-store" };

  function setStatus(kind, text) {
    statusEl.hidden = !text;
    statusEl.textContent = text || "";
    statusEl.classList.toggle("error", kind === "error");
    statusEl.classList.toggle("info", kind === "info");
  }

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatBytes(n) {
    if (n == null || Number.isNaN(n)) return "—";
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / (1024 * 1024)).toFixed(2)} MB`;
  }

  /** SQLite-style "YYYY-MM-DD HH:MM:SS" or ISO → human "Xm ago" / "just now" */
  function relativeAgo(raw) {
    if (!raw) return null;
    const normalized = String(raw).trim().replace(" ", "T");
    const t = Date.parse(normalized + (normalized.includes("Z") ? "" : "Z"));
    if (Number.isNaN(t)) return null;
    const sec = Math.max(0, Math.floor((Date.now() - t) / 1000));
    if (sec < 10) return "just now";
    if (sec < 60) return `${sec}s ago`;
    const min = Math.floor(sec / 60);
    if (min < 60) return `${min}m ago`;
    const h = Math.floor(min / 60);
    if (h < 48) return `${h}h ago`;
    const d = Math.floor(h / 24);
    return `${d}d ago`;
  }

  function markRefreshed() {
    const now = new Date();
    const t = now.toLocaleTimeString(undefined, {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
    lastRefreshEl.textContent = `Updated ${t}.`;
  }

  function renderSummary(data) {
    summaryEl.hidden = false;
    goldInfoEl.hidden = false;

    const queues = (data.queue_breakdown || [])
      .map(
        (q) =>
          `<li><span class="queue-id">${escapeHtml(String(q.queue_id))}</span> — ${escapeHtml(String(q.count))}</li>`
      )
      .join("");

    const cov =
      data.timeline_coverage_pct != null
        ? `${escapeHtml(String(data.timeline_coverage_pct))}%`
        : "—";
    const avgTl =
      data.avg_timeline_rows_per_match != null
        ? escapeHtml(String(data.avg_timeline_rows_per_match))
        : "—";
    const lastIngest = data.ingested_at_max || "—";
    const ago = relativeAgo(data.ingested_at_max);
    const lastLine =
      ago && lastIngest !== "—"
        ? `${escapeHtml(lastIngest)} <span class="subtle">UTC · ${escapeHtml(ago)}</span>`
        : escapeHtml(lastIngest);

    const laneReady = data.participants_lane_ready ?? "—";
    const ptotal = data.participants_count ?? "—";

    summaryEl.innerHTML = `
      <article class="stat-card stat-card-span db-stat-highlight">
        <h3>Collection (last 24h)</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_ingested_last_24h ?? "—"))}</p>
        <p class="stat-detail subtle">Matches ingested in the rolling past 24 hours (SQLite local time).</p>
      </article>
      <article class="stat-card stat-card-span db-stat-highlight">
        <h3>Latest ingest</h3>
        <p class="stat-detail">${lastLine}</p>
        <p class="stat-detail subtle">Raw value is UTC from the database (see match table for Eastern display).</p>
      </article>
      <article class="stat-card">
        <h3>Matches</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_count))}</p>
      </article>
      <article class="stat-card">
        <h3>With timeline</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_with_timeline ?? "—"))}</p>
        <p class="stat-detail subtle">Have gold/level samples</p>
      </article>
      <article class="stat-card">
        <h3>Missing timeline</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_without_timeline ?? "—"))}</p>
        <p class="stat-detail subtle">No ${escapeHtml("participant_timeline")} rows</p>
      </article>
      <article class="stat-card">
        <h3>Timeline coverage</h3>
        <p class="stat-value">${cov}</p>
        <p class="stat-detail subtle">Share of matches with any timeline data</p>
      </article>
      <article class="stat-card">
        <h3>Timeline rows</h3>
        <p class="stat-value">${escapeHtml(String(data.participant_timeline_rows ?? "—"))}</p>
        <p class="stat-detail subtle">Avg / timed match: ${avgTl}</p>
      </article>
      <article class="stat-card">
        <h3>Lane-ready rows</h3>
        <p class="stat-value">${escapeHtml(String(laneReady))}</p>
        <p class="stat-detail subtle">Participants with id + lane (${escapeHtml(String(ptotal))} total)</p>
      </article>
      <article class="stat-card">
        <h3>Participants</h3>
        <p class="stat-value">${escapeHtml(String(data.participants_count))}</p>
      </article>
      <article class="stat-card">
        <h3>File size</h3>
        <p class="stat-value">${escapeHtml(formatBytes(data.file_size_bytes))}</p>
      </article>
      <article class="stat-card stat-card-span">
        <h3>Ingested range</h3>
        <p class="stat-detail">${escapeHtml(data.ingested_at_min || "—")} → ${escapeHtml(data.ingested_at_max || "—")}</p>
        <p class="stat-detail subtle">Stored as UTC in SQLite.</p>
      </article>
      <article class="stat-card stat-card-span">
        <h3>By queue ID</h3>
        ${queues ? `<ul class="queue-list">${queues}</ul>` : "<p class=\"stat-detail\">No rows</p>"}
      </article>
    `;
  }

  function getDetailRowAfter(tr) {
    const n = tr.nextElementSibling;
    if (
      n &&
      n.classList.contains("db-match-detail") &&
      n.dataset.forMatch === tr.dataset.matchId
    ) {
      return n;
    }
    return null;
  }

  function setRowExpanded(tr, expanded) {
    tr.classList.toggle("is-expanded", expanded);
    tr.setAttribute("aria-expanded", expanded ? "true" : "false");
    const icon = tr.querySelector(".db-match-expand-icon");
    if (icon) icon.textContent = expanded ? "▼" : "▶";
  }

  function closeAllDetailRows() {
    tbody.querySelectorAll("tr.db-match-detail").forEach((r) => r.remove());
    tbody.querySelectorAll("tr.db-match-row").forEach((r) => setRowExpanded(r, false));
  }

  function renderParticipantCard(p) {
    const name = p.champion_name || `Champion ${p.champion_id}`;
    const icon = p.champion_icon_url
      ? `<img class="db-p-card-icon" src="${escapeHtml(p.champion_icon_url)}" alt="" width="40" height="40" />`
      : `<div class="db-p-card-icon-placeholder" aria-hidden="true"></div>`;
    const lane = p.team_position ? escapeHtml(p.team_position) : "—";
    const win = p.win ? "Win" : "Loss";
    const pid =
      p.participant_id != null && p.participant_id !== undefined
        ? escapeHtml(String(p.participant_id))
        : "—";
    const puuid = p.puuid_masked ? escapeHtml(p.puuid_masked) : "—";
    return `
      <div class="db-participant-card">
        ${icon}
        <div class="db-p-card-body">
          <div class="db-p-card-name">${escapeHtml(name)}</div>
          <div class="db-p-card-meta subtle">
            ID ${escapeHtml(String(p.champion_id))} · ${lane} · ${escapeHtml(win)}
          </div>
          <div class="db-p-card-meta subtle">
            Riot participant ${pid} · PUUID ${puuid}
          </div>
        </div>
      </div>
    `;
  }

  function renderDetailInner(d) {
    const m = d.match || {};
    const parts = d.participants || [];
    const groups = {};
    for (const p of parts) {
      const tid = p.team_id;
      if (!groups[tid]) groups[tid] = [];
      groups[tid].push(p);
    }
    const teamIds = Object.keys(groups)
      .map(Number)
      .sort((a, b) => a - b);
    const teamsHtml = teamIds
      .map((tid) => {
        const cards = groups[tid].map(renderParticipantCard).join("");
        return `
          <div class="db-team-block">
            <h4 class="db-team-title">Team ${escapeHtml(String(tid))}</h4>
            <div class="db-participant-grid">${cards}</div>
          </div>
        `;
      })
      .join("");

    const badgeClass = m.has_timeline ? "badge-ok" : "badge-warn";
    const badgeText = m.has_timeline ? "Timeline ready" : "No timeline";

    return `
      <div class="db-detail-inner">
        <dl class="db-detail-meta">
          <dt>Queue</dt><dd>${escapeHtml(String(m.queue_id ?? "—"))}</dd>
          <dt>Patch</dt><dd>${escapeHtml(m.game_version || "—")}</dd>
          <dt>Ingested (US Eastern)</dt><dd>${escapeHtml(m.ingested_at_est || "—")}</dd>
          <dt>Ingested (UTC stored)</dt><dd>${escapeHtml(m.ingested_at || "—")}</dd>
          <dt>Timeline rows</dt><dd>${escapeHtml(String(m.timeline_row_count ?? 0))}</dd>
          <dt>Status</dt><dd><span class="badge ${badgeClass}">${escapeHtml(badgeText)}</span></dd>
        </dl>
        ${teamsHtml || "<p class=\"subtle\">No participants in database for this match.</p>"}
      </div>
    `;
  }

  async function openDetailRow(tr) {
    const mid = tr.dataset.matchId;
    if (!mid) return;

    const existing = getDetailRowAfter(tr);
    if (existing) {
      existing.remove();
      setRowExpanded(tr, false);
      return;
    }

    closeAllDetailRows();

    const detailTr = document.createElement("tr");
    detailTr.className = "db-match-detail";
    detailTr.dataset.forMatch = mid;
    const colCount = 8;
    detailTr.innerHTML = `<td colspan="${colCount}"><div class="db-detail-loading">Loading…</div></td>`;
    tr.insertAdjacentElement("afterend", detailTr);
    setRowExpanded(tr, true);

    let payload = detailCache.get(mid);
    if (!payload) {
      const url = `/api/db/matches/${encodeURIComponent(mid)}`;
      const res = await fetch(url, fetchOpts);
      let body;
      try {
        body = await res.json();
      } catch {
        detailTr.querySelector("td").innerHTML =
          '<div class="db-detail-error">Invalid JSON from server.</div>';
        return;
      }
      if (!res.ok) {
        detailTr.querySelector("td").innerHTML = `<div class="db-detail-error">${escapeHtml(
          body.error || `Error ${res.status}`
        )}</div>`;
        return;
      }
      payload = body;
      detailCache.set(mid, payload);
    }

    detailTr.querySelector("td").innerHTML = renderDetailInner(payload);
  }

  function appendRows(rows) {
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.className = "db-match-row";
      tr.dataset.matchId = row.match_id;
      tr.setAttribute("tabindex", "0");
      tr.setAttribute("role", "button");
      tr.setAttribute("aria-expanded", "false");

      const tl = row.timeline_row_count != null ? row.timeline_row_count : 0;
      const hasTl = row.has_timeline || tl > 0;
      const badgeClass = hasTl ? "badge-ok" : "badge-warn";
      const badgeText = hasTl ? "Ready" : "No timeline";

      const est = row.ingested_at_est || "—";
      const utcRaw = row.ingested_at
        ? `<span class="db-ingest-utc subtle">UTC ${escapeHtml(row.ingested_at)}</span>`
        : "";

      tr.innerHTML = `
        <td class="db-col-expand"><span class="db-match-expand-icon" aria-hidden="true">▶</span></td>
        <td><code>${escapeHtml(row.match_id)}</code></td>
        <td>${escapeHtml(String(row.queue_id))}</td>
        <td>${escapeHtml(row.game_version || "—")}</td>
        <td class="db-ingest-cell">
          <span class="db-ingest-est">${escapeHtml(est)}</span>
          ${utcRaw}
        </td>
        <td class="tabular">${escapeHtml(String(row.participant_count))}</td>
        <td class="tabular">${escapeHtml(String(tl))}</td>
        <td><span class="badge ${badgeClass}">${escapeHtml(badgeText)}</span></td>
      `;
      tbody.appendChild(tr);
    }
  }

  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.db-match-row");
    if (!tr || !tbody.contains(tr)) return;
    e.preventDefault();
    openDetailRow(tr);
  });

  tbody.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const tr = e.target.closest("tr.db-match-row");
    if (!tr || !tbody.contains(tr)) return;
    e.preventDefault();
    openDetailRow(tr);
  });

  async function loadSummary() {
    const res = await fetch("/api/db/summary", fetchOpts);
    let body;
    try {
      body = await res.json();
    } catch {
      setStatus("error", "Invalid JSON from summary API.");
      return false;
    }
    if (!res.ok) {
      setStatus(
        "error",
        body.error ||
          `No database available (${res.status}). Set MATCHUP_DB_PATH to your SQLite file and ensure it exists.`
      );
      summaryEl.hidden = true;
      goldInfoEl.hidden = true;
      tableWrap.hidden = true;
      return false;
    }
    setStatus("", "");
    renderSummary(body);
    markRefreshed();
    return true;
  }

  async function loadMatches(reset) {
    if (loading) return;
    loading = true;
    loadMoreBtn.disabled = true;
    if (reset) {
      setStatus("info", "Refreshing matches…");
    } else {
      setStatus("info", "Loading matches…");
    }
    const useOffset = reset ? 0 : offset;
    const res = await fetch(
      `/api/db/matches?limit=${pageLimit}&offset=${useOffset}`,
      fetchOpts
    );
    let body;
    try {
      body = await res.json();
    } catch {
      setStatus("error", "Invalid JSON from matches API.");
      loading = false;
      loadMoreBtn.disabled = false;
      return;
    }
    if (!res.ok) {
      setStatus("error", body.error || `Error (${res.status})`);
      loading = false;
      loadMoreBtn.disabled = false;
      return;
    }
    setStatus("", "");
    const rows = body.matches || [];
    if (reset) {
      tbody.innerHTML = "";
      offset = 0;
      detailCache.clear();
      closeAllDetailRows();
    }
    appendRows(rows);
    offset += rows.length;
    tableWrap.hidden = false;
    loadMoreBtn.disabled = rows.length < pageLimit;
    loading = false;
    if (rows.length < pageLimit) {
      loadMoreBtn.hidden = true;
    } else {
      loadMoreBtn.hidden = false;
    }
  }

  async function refreshAll() {
    const ok = await loadSummary();
    if (ok) {
      await loadMatches(true);
    }
  }

  function startPolling() {
    if (pollTimer) clearInterval(pollTimer);
    pollTimer = setInterval(() => {
      if (document.visibilityState !== "visible") return;
      loadSummary();
    }, 30000);
  }

  loadMoreBtn.addEventListener("click", () => {
    loadMatches(false);
  });

  refreshBtn.addEventListener("click", () => {
    refreshAll();
  });

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") {
      loadSummary();
    }
  });

  (async function init() {
    const ok = await loadSummary();
    if (ok) {
      offset = 0;
      loadMoreBtn.hidden = false;
      loadMoreBtn.disabled = false;
      await loadMatches(true);
      startPolling();
    }
  })();
})();
