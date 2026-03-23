(function () {
  const statusEl = document.getElementById("db-status");
  const summaryEl = document.getElementById("db-summary");
  const tableWrap = document.getElementById("db-table-wrap");
  const tbody = document.getElementById("db-matches-body");
  const loadMoreBtn = document.getElementById("db-load-more");

  const pageLimit = 25;
  let offset = 0;
  let loading = false;

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

  function renderSummary(data) {
    summaryEl.hidden = false;
    const queues = (data.queue_breakdown || [])
      .map(
        (q) =>
          `<li><span class="queue-id">${escapeHtml(String(q.queue_id))}</span> — ${escapeHtml(String(q.count))}</li>`
      )
      .join("");
    summaryEl.innerHTML = `
      <article class="stat-card">
        <h3>Matches</h3>
        <p class="stat-value">${escapeHtml(String(data.matches_count))}</p>
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
      </article>
      <article class="stat-card stat-card-span">
        <h3>By queue ID</h3>
        ${queues ? `<ul class="queue-list">${queues}</ul>` : "<p class=\"stat-detail\">No rows</p>"}
      </article>
    `;
  }

  function appendRows(rows) {
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><code>${escapeHtml(row.match_id)}</code></td>
        <td>${escapeHtml(String(row.queue_id))}</td>
        <td>${escapeHtml(row.game_version || "—")}</td>
        <td>${escapeHtml(row.ingested_at || "—")}</td>
        <td>${escapeHtml(String(row.participant_count))}</td>
      `;
      tbody.appendChild(tr);
    }
  }

  async function loadSummary() {
    const res = await fetch("/api/db/summary", {
      headers: { Accept: "application/json" },
    });
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
  }

  loadMoreBtn.addEventListener("click", () => {
    loadMatches();
  });

  (async function init() {
    const ok = await loadSummary();
    if (ok) {
      offset = 0;
      loadMoreBtn.hidden = false;
      loadMoreBtn.disabled = false;
      await loadMatches();
    }
  })();
})();
