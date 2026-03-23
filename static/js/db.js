(function () {
  const statusEl = document.getElementById("db-status");
  const summaryEl = document.getElementById("db-summary");
  const tableWrap = document.getElementById("db-table-wrap");
  const tbody = document.getElementById("db-matches-body");
  const loadMoreBtn = document.getElementById("db-load-more");
  const pageSizeSelect = document.getElementById("db-page-size");
  const detailEl = document.getElementById("db-detail");
  const detailMeta = document.getElementById("db-detail-meta");
  const detailBody = document.getElementById("db-detail-body");
  const detailClose = document.getElementById("db-detail-close");

  let pageLimit = 25;
  let offset = 0;
  let loading = false;
  /** @type {string | null} */
  let selectedMatchId = null;

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

  function frac(a, b) {
    const x = Number(a);
    const y = Number(b);
    if (!y) return "—";
    return `${x}/${y}`;
  }

  function renderSummary(data) {
    summaryEl.hidden = false;
    const queues = (data.queue_breakdown || [])
      .map(
        (q) =>
          `<li><span class="queue-id">${escapeHtml(String(q.queue_id))}</span> — ${escapeHtml(String(q.count))}</li>`
      )
      .join("");
    const tlRows = data.timeline_row_count != null ? data.timeline_row_count : "—";
    const tlMatches =
      data.matches_with_timeline != null ? data.matches_with_timeline : "—";
    const riotN =
      data.participants_with_riot_id != null ? data.participants_with_riot_id : "—";
    const laneN =
      data.participants_with_lane != null ? data.participants_with_lane : "—";
    const goldReady = Boolean(data.gold_features_ready);
    const readinessClass = goldReady ? "stat-card stat-card-readiness is-ready" : "stat-card stat-card-readiness";

    summaryEl.innerHTML = `
      <article class="${readinessClass}">
        <h3>Gold lanes / curves</h3>
        <p class="stat-value">${goldReady ? "Ready" : "Incomplete"}</p>
        <p class="stat-detail subtle">Needs timeline rows, Riot participant id, and lane on participant rows</p>
      </article>
      <article class="stat-card">
        <h3>Matches</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_count))}</p>
      </article>
      <article class="stat-card">
        <h3>Participants</h3>
        <p class="stat-value">${escapeHtml(String(data.participants_count))}</p>
      </article>
      <article class="stat-card">
        <h3>Timeline rows</h3>
        <p class="stat-value">${escapeHtml(String(tlRows))}</p>
        <p class="stat-detail subtle">Across all stored games</p>
      </article>
      <article class="stat-card">
        <h3>Matches w/ timeline</h3>
        <p class="stat-value">${escapeHtml(String(tlMatches))}</p>
      </article>
      <article class="stat-card">
        <h3>Riot participant id</h3>
        <p class="stat-value">${escapeHtml(String(riotN))}</p>
        <p class="stat-detail subtle">Participant rows filled</p>
      </article>
      <article class="stat-card">
        <h3>Lane assigned</h3>
        <p class="stat-value">${escapeHtml(String(laneN))}</p>
        <p class="stat-detail subtle">team_position set</p>
      </article>
      <article class="stat-card">
        <h3>File size</h3>
        <p class="stat-value">${escapeHtml(formatBytes(data.file_size_bytes))}</p>
      </article>
      <article class="stat-card stat-card-span">
        <h3>Ingested range (US Eastern)</h3>
        <p class="stat-detail">${escapeHtml(data.ingested_at_min || "—")} → ${escapeHtml(data.ingested_at_max || "—")}</p>
      </article>
      <article class="stat-card stat-card-span">
        <h3>By queue ID</h3>
        ${queues ? `<ul class="queue-list">${queues}</ul>` : "<p class=\"stat-detail\">No rows</p>"}
      </article>
    `;
  }

  function setSelectedRow(matchId) {
    selectedMatchId = matchId;
    for (const tr of tbody.querySelectorAll("tr.db-match-row")) {
      const id = tr.dataset.matchId || "";
      tr.classList.toggle("is-selected", id === matchId);
    }
  }

  function appendRows(rows) {
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.classList.add("db-match-row");
      tr.dataset.matchId = row.match_id;
      tr.tabIndex = 0;
      const pc = row.participant_count ?? 0;
      const tl = row.timeline_row_count ?? 0;
      tr.innerHTML = `
        <td><code>${escapeHtml(row.match_id)}</code></td>
        <td>${escapeHtml(String(row.queue_id))}</td>
        <td>${escapeHtml(row.game_version || "—")}</td>
        <td>${escapeHtml(row.ingested_at || "—")}</td>
        <td class="tabular">${escapeHtml(String(pc))}</td>
        <td class="tabular">${escapeHtml(frac(row.participants_with_riot_id, pc))}</td>
        <td class="tabular">${escapeHtml(frac(row.participants_with_lane, pc))}</td>
        <td class="tabular">${tl > 0 ? escapeHtml(String(tl)) : "—"}</td>
      `;
      tbody.appendChild(tr);
    }
  }

  async function parseJsonResponse(res) {
    const text = await res.text();
    const trimmed = text.trim();
    if (!trimmed) {
      return {
        ok: false,
        message: `Empty response (HTTP ${res.status}).`,
      };
    }
    try {
      return { ok: true, body: JSON.parse(text) };
    } catch {
      const clip =
        trimmed.length > 220 ? `${trimmed.slice(0, 220)}…` : trimmed;
      return {
        ok: false,
        message: `Not JSON (HTTP ${res.status}). ${clip}`,
      };
    }
  }

  async function loadSummary() {
    const res = await fetch("/api/db/summary", {
      headers: { Accept: "application/json" },
    });
    const parsed = await parseJsonResponse(res);
    if (!parsed.ok) {
      setStatus("error", parsed.message);
      return false;
    }
    const body = parsed.body;
    if (!res.ok) {
      setStatus(
        "error",
        body.error ||
          `No database available (${res.status}). Set MATCHUP_DB_PATH to your SQLite file and ensure it exists.`
      );
      summaryEl.hidden = true;
      tableWrap.hidden = true;
      return false;
    }
    setStatus("", "");
    renderSummary(body);
    return true;
  }

  async function loadMatches() {
    if (loading) return;
    loading = true;
    loadMoreBtn.disabled = true;
    setStatus("info", "Loading matches…");
    const res = await fetch(
      `/api/db/matches?limit=${pageLimit}&offset=${offset}`,
      { headers: { Accept: "application/json" } }
    );
    const parsed = await parseJsonResponse(res);
    if (!parsed.ok) {
      setStatus("error", parsed.message);
      loading = false;
      loadMoreBtn.disabled = false;
      return;
    }
    const body = parsed.body;
    if (!res.ok) {
      setStatus("error", body.error || `Error (${res.status})`);
      loading = false;
      loadMoreBtn.disabled = false;
      return;
    }
    setStatus("", "");
    const rows = body.matches || [];
    if (offset === 0) {
      tbody.innerHTML = "";
    }
    appendRows(rows);
    offset += rows.length;
    tableWrap.hidden = false;
    loadMoreBtn.disabled = rows.length < pageLimit;
    loading = false;
    if (rows.length < pageLimit) {
      loadMoreBtn.hidden = true;
    }
    if (selectedMatchId) {
      setSelectedRow(selectedMatchId);
    }
  }

  async function loadMatchDetail(matchId) {
    setStatus("info", "Loading match…");
    const res = await fetch(
      `/api/db/matches/${encodeURIComponent(matchId)}`,
      { headers: { Accept: "application/json" } }
    );
    const parsed = await parseJsonResponse(res);
    if (!parsed.ok) {
      setStatus("error", parsed.message);
      return;
    }
    const body = parsed.body;
    if (!res.ok) {
      setStatus("error", body.error || `Error (${res.status})`);
      return;
    }
    setStatus("", "");
    const m = body.match || {};
    detailMeta.innerHTML = `
      <code>${escapeHtml(String(m.match_id || ""))}</code>
      · queue <strong>${escapeHtml(String(m.queue_id ?? "—"))}</strong>
      · ${escapeHtml(String(m.game_version || "—"))}
      · ingested <strong>${escapeHtml(String(m.ingested_at || "—"))}</strong> (US Eastern)
      · <strong>${escapeHtml(String(body.timeline_row_count ?? 0))}</strong> timeline rows
    `;
    detailBody.innerHTML = "";
    for (const p of body.participants || []) {
      const tr = document.createElement("tr");
      const icon = p.champion_icon_url
        ? `<img class="db-champ-icon" src="${escapeHtml(p.champion_icon_url)}" alt="" width="28" height="28" />`
        : "";
      const name = p.champion_name || "—";
      const pu = p.puuid_masked ? escapeHtml(p.puuid_masked) : "—";
      const lane = p.team_position != null && String(p.team_position).trim() ? escapeHtml(String(p.team_position)) : "—";
      const pid = p.participant_id != null ? escapeHtml(String(p.participant_id)) : "—";
      tr.innerHTML = `
        <td class="db-champ-cell">${icon} ${escapeHtml(name)}</td>
        <td class="tabular">${escapeHtml(String(p.champion_id))}</td>
        <td class="tabular">${escapeHtml(String(p.team_id))}</td>
        <td class="tabular">${p.win ? "Yes" : "No"}</td>
        <td>${lane}</td>
        <td class="tabular">${pid}</td>
        <td><code class="db-puuid">${pu}</code></td>
      `;
      detailBody.appendChild(tr);
    }
    detailEl.hidden = false;
    detailEl.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  function closeDetail() {
    detailEl.hidden = true;
    detailBody.innerHTML = "";
    setSelectedRow(null);
  }

  loadMoreBtn.addEventListener("click", () => {
    loadMatches();
  });

  pageSizeSelect.addEventListener("change", () => {
    const v = parseInt(pageSizeSelect.value, 10);
    pageLimit = Number.isNaN(v) ? 25 : Math.min(500, Math.max(1, v));
    offset = 0;
    loadMoreBtn.hidden = false;
    loadMatches();
  });

  tbody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.db-match-row");
    if (!tr || !tbody.contains(tr)) return;
    const id = tr.dataset.matchId;
    if (!id) return;
    setSelectedRow(id);
    loadMatchDetail(id);
  });

  tbody.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const tr = e.target.closest("tr.db-match-row");
    if (!tr || !tbody.contains(tr)) return;
    e.preventDefault();
    tr.click();
  });

  detailClose.addEventListener("click", closeDetail);

  (async function init() {
    const lim = parseInt(pageSizeSelect.value, 10);
    pageLimit = Number.isNaN(lim) ? 25 : lim;
    const ok = await loadSummary();
    if (ok) {
      offset = 0;
      loadMoreBtn.hidden = false;
      loadMoreBtn.disabled = false;
      await loadMatches();
    }
  })();
})();
