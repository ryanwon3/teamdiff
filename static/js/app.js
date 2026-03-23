(function () {
  const form = document.getElementById("gold-form");
  const statusEl = document.getElementById("status");
  const metaEl = document.getElementById("leader-meta");
  const leaderboardWrap = document.getElementById("leaderboard-wrap");
  const leaderboardBody = document.getElementById("leaderboard-body");
  const curvePanel = document.getElementById("curve-panel");
  const curveCaption = document.getElementById("curve-caption");
  const datalistEl = document.getElementById("champion-options");
  const champInput = document.getElementById("champion-input");
  const champIcon = document.getElementById("champion-icon");
  const chartCanvas = document.getElementById("gold-chart");
  const segmented = document.querySelector(".segmented");
  const minGamesInput = document.getElementById("min-games-input");
  const leadSortSelect = document.getElementById("lead-sort-select");
  const leadColHeader = document.getElementById("lead-col-header");
  const sortBlurbEl = document.getElementById("gold-leaderboard-sort-blurb");

  /** @type {Map<string, { name: string, icon_url: string }>} */
  let byNormName = new Map();
  /** @type {Chart | null} */
  let chartInstance = null;
  let curveMode = "time";
  /** @type {number | null} */
  let selectedOpponentId = null;

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

  /**
   * @param {Response} res
   * @returns {Promise<{ ok: true, body: any } | { ok: false, message: string }>}
   */
  async function parseJsonResponse(res) {
    const text = await res.text();
    const trimmed = text.trim();
    if (!trimmed) {
      return {
        ok: false,
        message: `Empty response (HTTP ${res.status}). Is the Flask server running?`,
      };
    }
    try {
      return { ok: true, body: JSON.parse(text) };
    } catch {
      const clip =
        trimmed.length > 220 ? `${trimmed.slice(0, 220)}…` : trimmed;
      return {
        ok: false,
        message: `Server did not return JSON (HTTP ${res.status}). ${clip}`,
      };
    }
  }

  function normName(s) {
    return String(s || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
  }

  function syncChampionIcon() {
    const raw = champInput.value.trim();
    if (/^\d+$/.test(raw)) {
      champIcon.hidden = true;
      champIcon.removeAttribute("src");
      return;
    }
    const meta = byNormName.get(normName(raw));
    if (meta && meta.icon_url) {
      champIcon.src = meta.icon_url;
      champIcon.alt = meta.name;
      champIcon.hidden = false;
    } else {
      champIcon.hidden = true;
      champIcon.removeAttribute("src");
    }
  }

  function debounce(fn, ms) {
    let t;
    return function () {
      clearTimeout(t);
      const args = arguments;
      t = setTimeout(() => fn.apply(null, args), ms);
    };
  }

  const debouncedIcon = debounce(syncChampionIcon, 200);

  async function loadChampions() {
    let res;
    try {
      res = await fetch("/api/champions", { headers: { Accept: "application/json" } });
    } catch {
      setStatus("error", "Could not load champion list (network).");
      return;
    }
    const parsed = await parseJsonResponse(res);
    if (!parsed.ok) {
      setStatus("error", parsed.message);
      return;
    }
    const body = parsed.body;
    if (!res.ok) {
      setStatus("error", body.error || `Champion list failed (${res.status})`);
      return;
    }
    const list = body.champions || [];
    byNormName = new Map();
    datalistEl.innerHTML = "";
    for (const c of list) {
      const opt = document.createElement("option");
      opt.value = c.name;
      datalistEl.appendChild(opt);
      byNormName.set(normName(c.name), {
        name: c.name,
        icon_url: c.icon_url,
      });
    }
  }

  function renderLeaderMeta(data) {
    metaEl.hidden = false;
    const name = data.champion_name || `ID ${data.champion_anchor_id}`;
    metaEl.innerHTML = `
      <p class="gold-meta-line"><strong>${escapeHtml(name)}</strong></p>
      <ul class="gold-meta-stats">
        <li>Matches as this champ (queue filter): <strong>${escapeHtml(String(data.anchor_match_count ?? "—"))}</strong></li>
        <li>With lane + participant id: <strong>${escapeHtml(String(data.anchor_matches_with_lane_meta ?? "—"))}</strong></li>
        <li>Lane-opponent games: <strong>${escapeHtml(String(data.lane_games ?? "—"))}</strong></li>
        <li>With gold snapshot ≤15 min: <strong>${escapeHtml(String(data.games_with_gold_at_15 ?? "—"))}</strong></li>
        <li>Min games per row: <strong>${escapeHtml(String(data.min_games ?? "—"))}</strong></li>
        <li>Avg Δ sort: <strong>${escapeHtml(data.lead_sort === "desc" ? "high → low" : "low → high")}</strong></li>
      </ul>
    `;
  }

  function updateLeaderboardSortUI(leadSort) {
    const desc = leadSort === "desc";
    if (leadColHeader) {
      leadColHeader.textContent = desc
        ? "Avg Δ gold ≤15 min ↓"
        : "Avg Δ gold ≤15 min ↑";
    }
    if (sortBlurbEl) {
      sortBlurbEl.innerHTML = desc
        ? "Sorted <strong>high→low</strong> (largest average opp gold lead first). Click a row for the curve."
        : "Default <strong>low→high</strong> (smallest average opp lead first). Click a row for the curve.";
    }
  }

  function renderLeaderboard(rows) {
    leaderboardBody.innerHTML = "";
    for (const row of rows) {
      const tr = document.createElement("tr");
      tr.dataset.opponentId = String(row.opponent_id);
      tr.tabIndex = 0;
      tr.classList.add("gold-leader-row");
      const icon = row.opponent_icon_url
        ? `<img class="lb-icon" src="${escapeHtml(row.opponent_icon_url)}" alt="" width="32" height="32" />`
        : "";
      const oname = row.opponent_name || `ID ${row.opponent_id}`;
      const lead =
        row.avg_gold_lead_at_15 == null ? "—" : String(row.avg_gold_lead_at_15);
      tr.innerHTML = `
        <td class="lb-icon-cell">${icon}</td>
        <td>${escapeHtml(oname)}</td>
        <td class="tabular">${escapeHtml(lead)}</td>
        <td class="tabular">${escapeHtml(String(row.games))}</td>
      `;
      leaderboardBody.appendChild(tr);
    }
  }

  function setSelectedRow(opponentId) {
    selectedOpponentId = opponentId;
    for (const tr of leaderboardBody.querySelectorAll("tr")) {
      const id = parseInt(tr.dataset.opponentId, 10);
      tr.classList.toggle("is-selected", id === opponentId);
    }
  }

  function destroyChart() {
    if (chartInstance) {
      chartInstance.destroy();
      chartInstance = null;
    }
  }

  function buildChart(payload) {
    destroyChart();
    const labels = payload.labels || [];
    const series = payload.series || [];
    const anchor = series.find((s) => s.key === "anchor") || series[0];
    const opp = series.find((s) => s.key === "opponent") || series[1];
    const colorA =
      getComputedStyle(document.documentElement).getPropertyValue("--accent").trim() ||
      "#c89b3c";
    const colorB =
      getComputedStyle(document.documentElement).getPropertyValue("--muted").trim() ||
      "#8b9bb4";

    const dsA = {
      label: (anchor && anchor.name) || payload.champion_a_name || "You",
      data: (anchor && anchor.data) || [],
      borderColor: colorA,
      backgroundColor: colorA + "33",
      tension: 0.2,
      fill: false,
    };
    const dsB = {
      label: (opp && opp.name) || payload.champion_b_name || "Opponent",
      data: (opp && opp.data) || [],
      borderColor: colorB,
      backgroundColor: colorB + "33",
      tension: 0.2,
      fill: false,
    };

    const xTitle = payload.mode === "level" ? "Champion level" : "Minute";

    chartInstance = new Chart(chartCanvas.getContext("2d"), {
      type: "line",
      data: {
        labels,
        datasets: [dsA, dsB],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { display: true },
          tooltip: {
            callbacks: {
              footer: (items) => {
                const idx = items[0]?.dataIndex;
                const ns = payload.games_per_point;
                if (idx == null || !ns || ns[idx] == null) return "";
                return `Games: ${ns[idx]}`;
              },
            },
          },
        },
        scales: {
          x: {
            title: { display: true, text: xTitle },
          },
          y: {
            title: { display: true, text: "Mean total gold" },
          },
        },
      },
    });
  }

  async function fetchCurve() {
    if (selectedOpponentId == null) return;
    const anchor = encodeURIComponent(champInput.value.trim());
    const opp = encodeURIComponent(String(selectedOpponentId));
    const mode = encodeURIComponent(curveMode);
    setStatus("info", "Loading curve…");
    let res;
    try {
      res = await fetch(
        `/api/gold-curve?champion_a=${anchor}&champion_b=${opp}&mode=${mode}`,
        { headers: { Accept: "application/json" } }
      );
    } catch {
      setStatus("error", "Network error loading curve.");
      return;
    }
    const parsed = await parseJsonResponse(res);
    if (!parsed.ok) {
      setStatus("error", parsed.message);
      return;
    }
    const body = parsed.body;
    if (!res.ok) {
      setStatus("error", body.error || `Curve error (${res.status})`);
      curvePanel.hidden = true;
      destroyChart();
      return;
    }
    setStatus("", "");
    curvePanel.hidden = false;
    const gMin = body.games_used_min != null ? body.games_used_min : "—";
    const gMax = body.games_used_max != null ? body.games_used_max : "—";
    curveCaption.textContent =
      body.mode === "level"
        ? `Level mode: mean gold when both laners share the same frame (minute + level). Games per point about ${gMin}–${gMax}. Lane games: ${body.games_lane}.`
        : `Time mode: minutes 0–25; mean gold when both have a frame at that minute. Games per point about ${gMin}–${gMax}. Lane games: ${body.games_lane}.`;
    buildChart(body);
  }

  async function loadGoldLeaderboard() {
    metaEl.hidden = true;
    leaderboardWrap.hidden = true;
    curvePanel.hidden = true;
    destroyChart();
    selectedOpponentId = null;
    setStatus("", "");

    const q = encodeURIComponent(champInput.value.trim());
    let minGames = parseInt(minGamesInput && minGamesInput.value, 10);
    if (Number.isNaN(minGames) || minGames < 0) minGames = 0;
    if (minGames > 50) minGames = 50;
    const leadSort =
      leadSortSelect && leadSortSelect.value === "desc" ? "desc" : "asc";
    setStatus("info", "Loading leaderboard…");

    let res;
    try {
      res = await fetch(
        `/api/gold-leaders?champion=${q}&min_games=${encodeURIComponent(String(minGames))}&lead_sort=${encodeURIComponent(leadSort)}`,
        { headers: { Accept: "application/json" } }
      );
    } catch {
      setStatus("error", "Network error.");
      return;
    }
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

    if (leadSortSelect && body.lead_sort) {
      leadSortSelect.value = body.lead_sort === "desc" ? "desc" : "asc";
    }

    setStatus("", "");
    renderLeaderMeta(body);
    updateLeaderboardSortUI(body.lead_sort || leadSort);
    const leaders = body.leaders || [];
    if (!leaders.length) {
      const lg = Number(body.lane_games ?? 0);
      const g15 = Number(body.games_with_gold_at_15 ?? 0);
      const mg = body.min_games ?? minGames;
      const anyM = Number(body.anchor_match_count ?? 0);
      const laneMeta = Number(body.anchor_matches_with_lane_meta ?? 0);
      let msg;
      if (anyM === 0) {
        msg =
          "This champion is not in your DB for the current queue filter. Check MATCHUP_QUEUE_ID or run collect_matches.py.";
      } else if (laneMeta === 0) {
        msg =
          "Matches lack lane / participant id. Re-fetch match details (restart collector with current code) or merge meta from Riot payloads.";
      } else if (lg === 0) {
        msg =
          "No same-lane opponent pairs. Opponents need matching team_position.";
      } else if (g15 === 0) {
        msg =
          "No timeline gold at ≤15 min. Ensure collect_matches.py fetches timelines for these games.";
      } else {
        msg = `No row met min_games=${mg}. Lower the minimum or ingest more games.`;
      }
      setStatus("info", msg);
      return;
    }
    leaderboardWrap.hidden = false;
    renderLeaderboard(leaders);
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    await loadGoldLeaderboard();
  });

  if (leadSortSelect) {
    leadSortSelect.addEventListener("change", () => {
      if (!champInput.value.trim()) return;
      loadGoldLeaderboard();
    });
  }

  leaderboardBody.addEventListener("click", (e) => {
    const tr = e.target.closest("tr.gold-leader-row");
    if (!tr || !leaderboardBody.contains(tr)) return;
    const id = parseInt(tr.dataset.opponentId, 10);
    if (Number.isNaN(id)) return;
    setSelectedRow(id);
    fetchCurve();
  });

  leaderboardBody.addEventListener("keydown", (e) => {
    if (e.key !== "Enter" && e.key !== " ") return;
    const tr = e.target.closest("tr.gold-leader-row");
    if (!tr || !leaderboardBody.contains(tr)) return;
    e.preventDefault();
    tr.click();
  });

  if (segmented) {
    segmented.addEventListener("click", (e) => {
      const btn = e.target.closest(".segmented-btn");
      if (!btn || !segmented.contains(btn)) return;
      const mode = btn.getAttribute("data-mode");
      if (!mode || mode === curveMode) return;
      curveMode = mode;
      for (const b of segmented.querySelectorAll(".segmented-btn")) {
        b.classList.toggle("is-active", b.getAttribute("data-mode") === mode);
      }
      if (selectedOpponentId != null) fetchCurve();
    });
  }

  champInput.addEventListener("input", debouncedIcon);
  champInput.addEventListener("blur", syncChampionIcon);

  loadChampions();
})();
