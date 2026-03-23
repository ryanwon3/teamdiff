(function () {
  const form = document.getElementById("matchup-form");
  const statusEl = document.getElementById("status");
  const resultsEl = document.getElementById("results");
  const datalistEl = document.getElementById("champion-options");
  const inputA = document.getElementById("champ-a-input");
  const inputB = document.getElementById("champ-b-input");
  const iconA = document.getElementById("champ-a-icon");
  const iconB = document.getElementById("champ-b-icon");

  /** @type {Map<string, { name: string, icon_url: string }>} */
  let byNormName = new Map();

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

  function normName(s) {
    return String(s || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, " ");
  }

  function syncIcon(inputEl, imgEl) {
    const raw = inputEl.value.trim();
    if (/^\d+$/.test(raw)) {
      imgEl.hidden = true;
      imgEl.removeAttribute("src");
      return;
    }
    const meta = byNormName.get(normName(raw));
    if (meta && meta.icon_url) {
      imgEl.src = meta.icon_url;
      imgEl.alt = meta.name;
      imgEl.hidden = false;
    } else {
      imgEl.hidden = true;
      imgEl.removeAttribute("src");
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

  const debouncedA = debounce(() => syncIcon(inputA, iconA), 200);
  const debouncedB = debounce(() => syncIcon(inputB, iconB), 200);

  async function loadChampions() {
    let res;
    try {
      res = await fetch("/api/champions", { headers: { Accept: "application/json" } });
    } catch {
      setStatus("error", "Could not load champion list (network).");
      return;
    }
    let body;
    try {
      body = await res.json();
    } catch {
      setStatus("error", "Invalid champion list JSON.");
      return;
    }
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

  function renderResults(data) {
    resultsEl.hidden = false;
    const wrPct =
      data.winrate == null ? null : (data.winrate * 100).toFixed(2);
    const wrDisplay = wrPct == null ? "—" : `${wrPct}%`;
    const warn = data.sample_size_warning
      ? '<p class="warn">Small sample: interpret with caution.</p>'
      : "";

    const nameA = data.champion_a_name || `ID ${data.champion_a}`;
    const nameB = data.champion_b_name || `ID ${data.champion_b}`;
    const iconAHtml = data.champion_a_icon
      ? `<img class="results-champ-icon" src="${escapeHtml(data.champion_a_icon)}" alt="" width="48" height="48" />`
      : "";
    const iconBHtml = data.champion_b_icon
      ? `<img class="results-champ-icon" src="${escapeHtml(data.champion_b_icon)}" alt="" width="48" height="48" />`
      : "";

    const source = data.source ? escapeHtml(String(data.source)) : "—";
    const fetchDetail =
      data.source === "sqlite"
        ? ""
        : `<dt>Match details fetched</dt><dd>${escapeHtml(
            String(data.match_detail_fetches)
          )}</dd>`;

    resultsEl.innerHTML = `
      <h2 class="results-title">Result</h2>
      <div class="results-hero">
        <p class="results-winrate-label">Win rate (${escapeHtml(nameA)} vs ${escapeHtml(nameB)})</p>
        <p class="results-winrate-value">${escapeHtml(wrDisplay)}</p>
        <p class="results-games">Head-to-head games: <strong>${escapeHtml(String(data.games))}</strong></p>
      </div>
      <div class="results-champions">
        <div class="results-champ">
          ${iconAHtml}
          <div>
            <div class="results-champ-name">${escapeHtml(nameA)}</div>
            <div class="results-champ-id subtle">Champion A · ${escapeHtml(String(data.champion_a))}</div>
          </div>
        </div>
        <div class="results-champ">
          ${iconBHtml}
          <div>
            <div class="results-champ-name">${escapeHtml(nameB)}</div>
            <div class="results-champ-id subtle">Champion B · ${escapeHtml(String(data.champion_b))}</div>
          </div>
        </div>
      </div>
      <dl class="results-secondary">
        <dt>Wins (A)</dt><dd>${escapeHtml(String(data.wins_a))}</dd>
        ${fetchDetail}
        <dt>Source</dt><dd>${source}</dd>
      </dl>
      ${warn}
    `;
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    resultsEl.hidden = true;
    setStatus("", "");

    const params = new URLSearchParams(new FormData(form));
    setStatus("info", "Fetching…");

    let res;
    try {
      res = await fetch(`/api/matchup?${params.toString()}`, {
        headers: { Accept: "application/json" },
      });
    } catch (err) {
      setStatus("error", "Network error.");
      return;
    }

    let body;
    try {
      body = await res.json();
    } catch {
      setStatus("error", "Invalid JSON response.");
      return;
    }

    if (!res.ok) {
      setStatus("error", body.error || `Error (${res.status})`);
      return;
    }

    setStatus("", "");
    renderResults(body);
  });

  inputA.addEventListener("input", debouncedA);
  inputB.addEventListener("input", debouncedB);
  inputA.addEventListener("blur", () => syncIcon(inputA, iconA));
  inputB.addEventListener("blur", () => syncIcon(inputB, iconB));

  loadChampions();
})();
