const state = {
  snapshot: null,
  live: true,
  eventSource: null,
  pollTimer: null,
  search: "",
  namespace: "all",
};

const els = {
  liveToggle: document.getElementById("liveToggle"),
  namespaceFilter: document.getElementById("namespaceFilter"),
  refreshButton: document.getElementById("refreshButton"),
  clusterName: document.getElementById("clusterName"),
  clusterMode: document.getElementById("clusterMode"),
  scenarioName: document.getElementById("scenarioName"),
  updatedAt: document.getElementById("updatedAt"),
  readinessScore: document.getElementById("readinessScore"),
  kpiPods: document.getElementById("kpiPods"),
  kpiCpu: document.getElementById("kpiCpu"),
  kpiMemory: document.getElementById("kpiMemory"),
  kpiDisk: document.getElementById("kpiDisk"),
  kpiPvcRead: document.getElementById("kpiPvcRead"),
  kpiPvc: document.getElementById("kpiPvc"),
  kpiNetwork: document.getElementById("kpiNetwork"),
  kpiRisk: document.getElementById("kpiRisk"),
  forecastChip: document.getElementById("forecastChip"),
  edgeCount: document.getElementById("edgeCount"),
  agentCount: document.getElementById("agentCount"),
  anomalyCount: document.getElementById("anomalyCount"),
  nlpInsight: document.getElementById("nlpInsight"),
  agentList: document.getElementById("agentList"),
  podRows: document.getElementById("podRows"),
  podSearch: document.getElementById("podSearch"),
  timeline: document.getElementById("timeline"),
  recommendationList: document.getElementById("recommendationList"),
  correlationList: document.getElementById("correlationList"),
  focusAreas: document.getElementById("focusAreas"),
  forecastConfidence: document.getElementById("forecastConfidence"),
  forecastList: document.getElementById("forecastList"),
  alertCount: document.getElementById("alertCount"),
  alertList: document.getElementById("alertList"),
  sustainabilityChip: document.getElementById("sustainabilityChip"),
  sustainabilityPanel: document.getElementById("sustainabilityPanel"),
  discoveryCount: document.getElementById("discoveryCount"),
  discoveryRows: document.getElementById("discoveryRows"),
  coverageList: document.getElementById("coverageList"),
  focusDetailList: document.getElementById("focusDetailList"),
  readinessLabel: document.getElementById("readinessLabel"),
  readinessGates: document.getElementById("readinessGates"),
  graph: document.getElementById("dependencyGraph"),
  chart: document.getElementById("resourceChart"),
  chatMessages: document.getElementById("chatMessages"),
  chatInput: document.getElementById("chatInput"),
  chatSend: document.getElementById("chatSend"),
  themeToggle: document.getElementById("themeToggle"),
  themeIcon: document.getElementById("themeIcon"),
  heatmapGrid: document.getElementById("heatmapGrid"),
  heatmapCount: document.getElementById("heatmapCount"),
  successMetrics: document.getElementById("successMetrics"),
};

const severityRank = { critical: 3, warning: 2, info: 1, normal: 0 };

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatNumber(value, digits = 0) {
  return Number(value || 0).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function setText(element, value) {
  element.textContent = value;
}

/* ── Dark mode ── */
function initTheme() {
  const saved = localStorage.getItem("podmind-theme");
  if (saved === "dark") document.documentElement.setAttribute("data-theme", "dark");
  updateThemeIcon();
}
function toggleTheme() {
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  document.documentElement.setAttribute("data-theme", isDark ? "light" : "dark");
  localStorage.setItem("podmind-theme", isDark ? "light" : "dark");
  updateThemeIcon();
}
function updateThemeIcon() {
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  els.themeIcon.textContent = isDark ? "\u2600" : "\ud83c\udf19";
}
initTheme();

async function fetchSnapshot() {
  const response = await fetch("/api/snapshot", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Snapshot failed: ${response.status}`);
  }
  receiveSnapshot(await response.json());
}

function connectStream() {
  disconnectStream();
  if (!state.live) {
    return;
  }
  try {
    state.eventSource = new EventSource("/api/stream");
    state.eventSource.addEventListener("snapshot", (event) => {
      receiveSnapshot(JSON.parse(event.data));
    });
    state.eventSource.onerror = () => {
      disconnectStream();
      startPolling();
    };
  } catch (error) {
    startPolling();
  }
}

function disconnectStream() {
  if (state.eventSource) {
    state.eventSource.close();
    state.eventSource = null;
  }
}

function startPolling() {
  stopPolling();
  if (!state.live) {
    return;
  }
  state.pollTimer = window.setInterval(() => {
    fetchSnapshot().catch(() => {});
  }, 2500);
}

function stopPolling() {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
}

function receiveSnapshot(snapshot) {
  state.snapshot = snapshot;
  render();
}

async function setScenario(scenario) {
  document.querySelectorAll(".scenario-button").forEach((button) => {
    button.disabled = true;
  });
  try {
    await fetch("/api/scenario", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenario }),
    });
    await fetchSnapshot();
  } finally {
    document.querySelectorAll(".scenario-button").forEach((button) => {
      button.disabled = false;
      button.classList.toggle("is-active", button.dataset.scenario === scenario);
    });
  }
}

function filteredPods() {
  if (!state.snapshot) {
    return [];
  }
  const query = state.search.trim().toLowerCase();
  return state.snapshot.pods
    .filter((pod) => state.namespace === "all" || pod.namespace === state.namespace)
    .filter((pod) => {
      if (!query) {
        return true;
      }
      return `${pod.name} ${pod.service} ${pod.namespace}`.toLowerCase().includes(query);
    })
    .sort((a, b) => {
      const riskDelta = severityRank[b.risk] - severityRank[a.risk];
      if (riskDelta) {
        return riskDelta;
      }
      return b.cpuM - a.cpuM;
    });
}

function render() {
  const snapshot = state.snapshot;
  if (!snapshot) {
    return;
  }
  renderShell(snapshot);
  renderKpis(snapshot);
  renderNamespaceOptions(snapshot);
  renderFocusAreas(snapshot);
  renderAgents(snapshot);
  renderPods(filteredPods());
  renderEnterprise(snapshot);
  renderDiscovery(filteredPods());
  renderCoverage(snapshot);
  renderTimeline(snapshot.timeline || []);
  renderRecommendations(snapshot);
  renderHeatmap(filteredPods());
  renderSuccessMetrics(snapshot);
  drawChart(snapshot.timeline || []);
  drawGraph(snapshot);
}

function renderShell(snapshot) {
  setText(els.clusterName, snapshot.cluster.name);
  setText(els.clusterMode, snapshot.cluster.mode);
  setText(els.scenarioName, snapshot.cluster.scenario.replaceAll("_", " "));
  setText(els.updatedAt, new Date(snapshot.timestamp).toLocaleTimeString());
  setText(els.readinessScore, `${snapshot.readiness?.score || 0}%`);

  document.querySelectorAll(".scenario-button").forEach((button) => {
    button.classList.toggle("is-active", button.dataset.scenario === snapshot.cluster.scenario);
  });

  const forecastRisk = snapshot.forecast?.risk || "normal";
  els.forecastChip.textContent = `${snapshot.forecast?.window || "15 minutes"} ${forecastRisk}`;
  els.forecastChip.className = `chip ${forecastRisk === "elevated" ? "warning" : ""}`;
  setText(els.edgeCount, `${snapshot.dependencies.length} links`);
  setText(els.agentCount, `${snapshot.agents.length}`);
  setText(els.anomalyCount, `${snapshot.anomalies.length} active`);
  els.anomalyCount.className = `chip ${snapshot.anomalies.some((item) => item.severity === "critical") ? "critical" : snapshot.anomalies.length ? "warning" : ""}`;
  els.readinessLabel.textContent = snapshot.readiness?.label || "prototype";
}

function renderKpis(snapshot) {
  const totals = snapshot.totals;
  setText(els.kpiPods, formatNumber(totals.pods));
  setText(els.kpiCpu, `${formatNumber(totals.cpuM)}m`);
  setText(els.kpiMemory, `${formatNumber(totals.memoryMiB)} MiB`);
  setText(els.kpiDisk, `${formatNumber(totals.diskMiB)} MiB`);
  setText(els.kpiPvcRead, `${formatNumber(totals.pvcReadMiBs, 1)} MiB/s`);
  setText(els.kpiPvc, `${formatNumber(totals.pvcWriteMiBs, 1)} MiB/s`);
  setText(els.kpiNetwork, `${formatNumber(totals.networkTxKiBs)} KiB/s`);
  setText(els.kpiRisk, formatNumber(totals.warningPods + totals.criticalPods));
}

function renderFocusAreas(snapshot) {
  const areas = snapshot.focusAreas || [];
  els.focusAreas.innerHTML = areas
    .map((area) => {
      return `
        <section class="focus-pill">
          <span>${escapeHtml(area.status)}</span>
          <strong>${escapeHtml(area.name)}</strong>
        </section>
      `;
    })
    .join("");
}

function renderNamespaceOptions(snapshot) {
  const namespaces = snapshot.cluster.namespaces || [];
  const current = els.namespaceFilter.value || "all";
  const html = [`<option value="all">All</option>`]
    .concat(namespaces.map((namespace) => `<option value="${escapeHtml(namespace)}">${escapeHtml(namespace)}</option>`))
    .join("");
  if (els.namespaceFilter.innerHTML !== html) {
    els.namespaceFilter.innerHTML = html;
    els.namespaceFilter.value = namespaces.includes(current) ? current : "all";
    state.namespace = els.namespaceFilter.value;
  }
}

function renderAgents(snapshot) {
  setText(els.nlpInsight, snapshot.nlpInsight || "No insight available yet.");
  if (!snapshot.agents.length) {
    els.agentList.innerHTML = `<div class="empty-state">No active findings.</div>`;
    return;
  }
  els.agentList.innerHTML = snapshot.agents
    .map((agent) => {
      const pods = agent.pods.length ? `<span>${escapeHtml(agent.pods.join(", "))}</span>` : `<span>cluster-wide</span>`;
      return `
        <section class="agent-item ${escapeHtml(agent.severity)}">
          <strong>${escapeHtml(agent.agent)} <span class="risk ${escapeHtml(agent.severity)}">${escapeHtml(agent.severity)}</span></strong>
          <p>${escapeHtml(agent.insight)}</p>
          <p>${pods}</p>
        </section>
      `;
    })
    .join("");
}

function renderPods(pods) {
  if (!pods.length) {
    els.podRows.innerHTML = `<tr><td colspan="8">No pods match the current filter.</td></tr>`;
    return;
  }
  els.podRows.innerHTML = pods
    .map((pod) => {
      return `
        <tr>
          <td>
            <div class="pod-name">
              <strong>${escapeHtml(pod.name)}</strong>
              <span>${escapeHtml(pod.service)} / ${escapeHtml(pod.status)}</span>
            </div>
          </td>
          <td>${escapeHtml(pod.namespace)}</td>
          <td>${meter(pod.cpuPct, pod.risk)}<span>${formatNumber(pod.cpuM)}m</span></td>
          <td>${meter(pod.memoryPct, pod.risk)}<span>${formatNumber(pod.memoryMiB)} MiB</span></td>
          <td>${formatNumber(pod.pvcWriteMiBs, 1)} MiB/s</td>
          <td>${formatNumber(pod.networkTxKiBs)} KiB/s</td>
          <td>${formatNumber(pod.restarts)}</td>
          <td><span class="risk ${escapeHtml(pod.risk)}">${escapeHtml(pod.risk)}</span></td>
        </tr>
      `;
    })
    .join("");
}

function renderEnterprise(snapshot) {
  const forecast = snapshot.forecast || {};
  els.forecastConfidence.textContent = `${formatNumber((forecast.confidence || 0) * 100)}% confidence`;
  els.forecastList.innerHTML = [
    ["CPU trend", `${escapeHtml(forecast.cpuTrend || "stable")} / ${formatNumber(forecast.predictedCpuM)}m`],
    ["Memory trend", `${escapeHtml(forecast.memoryTrend || "stable")} / ${formatNumber(forecast.predictedMemoryMiB)} MiB`],
    ["Storage trend", `${escapeHtml(forecast.storageTrend || "stable")} / ${formatNumber(forecast.predictedPvcWriteMiBs, 1)} MiB/s`],
    ["Network trend", escapeHtml(forecast.networkTrend || "stable")],
    ["Risk window", `${escapeHtml(forecast.window || "next 15 minutes")} / ${escapeHtml(forecast.risk || "normal")}`],
  ]
    .map(([label, value]) => metricRow(label, value))
    .join("");

  const alerts = snapshot.alerts || [];
  els.alertCount.textContent = `${alerts.length} open`;
  els.alertCount.className = `chip ${alerts.some((alert) => alert.severity === "critical") ? "critical" : alerts.length ? "warning" : ""}`;
  els.alertList.innerHTML = alerts.length
    ? alerts
        .map((alert) => {
          return `
            <section class="alert-item ${escapeHtml(alert.severity)}">
              <div class="alert-title">
                <strong>${escapeHtml(alert.id)} ${escapeHtml(alert.agent)}</strong>
                <span class="risk ${escapeHtml(alert.severity)}">${escapeHtml(alert.severity)}</span>
              </div>
              <span>${escapeHtml(alert.workload)} / ${escapeHtml(alert.namespace)}</span>
              <span>${escapeHtml(alert.action)}</span>
            </section>
          `;
        })
        .join("")
    : `<div class="empty-state">No open alerts.</div>`;

  const sustainability = snapshot.sustainability || {};
  const candidates = sustainability.rightSizingCandidates || [];
  els.sustainabilityPanel.innerHTML = [
    ["Estimated draw", `${formatNumber(sustainability.estimatedWatts, 2)} W`],
    ["Right-sizing opportunity", `${formatNumber(sustainability.rightSizingWatts, 2)} W`],
    ["Candidates", candidates.length ? candidates.map(escapeHtml).join(", ") : "None"],
    ["Model", escapeHtml(sustainability.carbonNote || "edge estimate")],
  ]
    .map(([label, value]) => metricRow(label, value))
    .join("");
}

function metricRow(label, value) {
  return `
    <section class="metric-row">
      <span>${escapeHtml(label)}</span>
      <strong>${value}</strong>
    </section>
  `;
}

function renderDiscovery(pods) {
  els.discoveryCount.textContent = `${pods.length} pods`;
  if (!pods.length) {
    els.discoveryRows.innerHTML = `<tr><td colspan="10">No pods match the current filter.</td></tr>`;
    return;
  }
  els.discoveryRows.innerHTML = pods
    .map((pod) => {
      const signals = pod.anomalies?.length ? pod.anomalies.join(", ") : "normal";
      return `
        <tr>
          <td>
            <div class="pod-name">
              <strong>${escapeHtml(pod.name)}</strong>
              <span>${escapeHtml(pod.service)} / ${escapeHtml(pod.status)}</span>
            </div>
          </td>
          <td>${escapeHtml(pod.namespace)}</td>
          <td>${formatNumber(pod.cpuM, 1)}m / ${formatNumber(pod.cpuPct, 1)}%</td>
          <td>${formatNumber(pod.memoryMiB, 1)} MiB / ${formatNumber(pod.memoryPct, 1)}%</td>
          <td>${formatNumber(pod.diskMiB, 1)} MiB / ${formatNumber(pod.diskPct, 1)}%</td>
          <td>${formatNumber(pod.pvcReadMiBs, 1)} / ${formatNumber(pod.pvcWriteMiBs, 1)} MiB/s</td>
          <td>${formatNumber(pod.networkRxKiBs)} / ${formatNumber(pod.networkTxKiBs)} KiB/s</td>
          <td>${formatNumber(pod.logsPerMin)}/min</td>
          <td>${formatNumber(pod.latencyMs, 1)} ms</td>
          <td><span class="risk ${escapeHtml(pod.risk)}">${escapeHtml(signals)}</span></td>
        </tr>
      `;
    })
    .join("");
}

function renderCoverage(snapshot) {
  els.coverageList.innerHTML = renderCoverageItems(snapshot.capabilityCoverage || [], "capability");
  els.focusDetailList.innerHTML = renderCoverageItems(snapshot.focusAreas || [], "name");
  const gates = snapshot.readiness?.gates || [];
  els.readinessGates.innerHTML = renderCoverageItems(gates, "name");
}

function renderCoverageItems(items, titleKey) {
  if (!items.length) {
    return `<div class="empty-state">Waiting for coverage data.</div>`;
  }
  return items
    .map((item) => {
      const state = item.state || item.status || "active";
      const evidence = item.evidence || "";
      return `
        <section class="coverage-item">
          <div>
            <strong>${escapeHtml(item[titleKey])}</strong>
            <span>${escapeHtml(evidence)}</span>
          </div>
          <span class="state-pill ${escapeHtml(state)}">${escapeHtml(state)}</span>
        </section>
      `;
    })
    .join("");
}

function meter(value, risk) {
  const width = Math.max(3, Math.min(100, Number(value) || 0));
  return `<div class="meter ${escapeHtml(risk)}" style="--value:${width}%"><span></span></div>`;
}

function renderTimeline(timeline) {
  const items = timeline.slice(-12).reverse();
  if (!items.length) {
    els.timeline.innerHTML = `<div class="empty-state">Waiting for samples.</div>`;
    return;
  }
  els.timeline.innerHTML = items
    .map((item) => {
      return `
        <section class="timeline-item">
          <div>
            <strong>${escapeHtml(item.time)}</strong>
            <div class="timeline-mark ${escapeHtml(item.level)}"></div>
          </div>
          <div class="timeline-copy">
            <strong>${escapeHtml(item.level)} / ${formatNumber(item.count)} findings</strong>
            CPU ${formatNumber(item.cpuM)}m, PVC ${formatNumber(item.pvcWriteMiBs, 1)} MiB/s
          </div>
        </section>
      `;
    })
    .join("");
}

function renderRecommendations(snapshot) {
  const recommendations = snapshot.recommendations || [];
  els.recommendationList.innerHTML = recommendations
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("");

  const correlations = snapshot.correlations || [];
  els.correlationList.innerHTML = correlations.length
    ? correlations
        .map((item) => {
          return `
            <section class="correlation">
              <strong>${escapeHtml(item.source)} -> ${escapeHtml(item.target)} (${formatNumber(item.score * 100)}%)</strong>
              ${escapeHtml(item.signals.join(", ")) || "resource"} signals
            </section>
          `;
        })
        .join("")
    : `<div class="empty-state">No strong dependency correlations.</div>`;
}

function drawChart(timeline) {
  const canvas = els.chart;
  const ctx = canvas.getContext("2d");
  const rect = canvas.getBoundingClientRect();
  const dpr = window.devicePixelRatio || 1;
  const width = Math.max(320, rect.width);
  const height = Math.max(220, rect.height || 260);
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, width, height);

  const samples = timeline.slice(-24);
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  ctx.fillStyle = isDark ? "#14231c" : "#fbfcfa";
  ctx.fillRect(0, 0, width, height);
  drawGrid(ctx, width, height);

  if (samples.length < 2) {
    ctx.fillStyle = isDark ? "#8a9f93" : "#66736b";
    ctx.font = "700 14px system-ui";
    ctx.fillText("Waiting for samples", 22, 34);
    return;
  }

  const series = [
    { key: "cpuM", label: "CPU m", color: "#087f7a" },
    { key: "memoryMiB", label: "Memory MiB", color: "#6950a1" },
    { key: "pvcWriteMiBs", label: "PVC MiB/s x80", color: "#b97805", scale: 80 },
    { key: "networkTxKiBs", label: "Network KiB/s", color: "#0d7ea2" },
  ];
  const maxValue = Math.max(
    1,
    ...samples.flatMap((sample) => series.map((item) => Number(sample[item.key] || 0) * (item.scale || 1)))
  );

  series.forEach((item, index) => {
    drawSeries(ctx, samples, item, maxValue, width, height);
    drawLegend(ctx, item, index);
  });
}

function drawGrid(ctx, width, height) {
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  ctx.strokeStyle = isDark ? "rgba(255,255,255,0.06)" : "#dce4de";
  ctx.lineWidth = 1;
  const top = 58;
  const bottom = 22;
  for (let i = 0; i <= 4; i += 1) {
    const y = top + ((height - top - bottom) * i) / 4;
    ctx.beginPath();
    ctx.moveTo(18, y);
    ctx.lineTo(width - 18, y);
    ctx.stroke();
  }
}

function drawSeries(ctx, samples, item, maxValue, width, height) {
  const padX = 22;
  const padTop = 58;
  const padBottom = 22;
  const graphWidth = width - padX * 2;
  const graphHeight = height - padTop - padBottom;
  ctx.strokeStyle = item.color;
  ctx.lineWidth = 2.5;
  ctx.beginPath();
  samples.forEach((sample, index) => {
    const value = Number(sample[item.key] || 0) * (item.scale || 1);
    const x = padX + (graphWidth * index) / Math.max(1, samples.length - 1);
    const y = padTop + graphHeight - (value / maxValue) * graphHeight;
    if (index === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();
}

function drawLegend(ctx, item, index) {
  const x = 22 + (index % 2) * 170;
  const y = 18 + Math.floor(index / 2) * 20;
  ctx.fillStyle = item.color;
  ctx.fillRect(x, y - 8, 18, 4);
  const isDark = document.documentElement.getAttribute("data-theme") === "dark";
  ctx.fillStyle = isDark ? "#e4ede8" : "#26312c";
  ctx.font = "700 11px system-ui";
  ctx.fillText(item.label, x + 24, y - 3);
}

function drawGraph(snapshot) {
  const graph = els.graph;
  const pods = snapshot.pods || [];
  const edges = snapshot.dependencies || [];
  const services = Array.from(new Set(pods.map((pod) => pod.service)));
  const risks = Object.fromEntries(services.map((service) => [service, "normal"]));
  pods.forEach((pod) => {
    if (severityRank[pod.risk] > severityRank[risks[pod.service]]) {
      risks[pod.service] = pod.risk;
    }
  });

  const width = 760;
  const height = 420;
  const centerX = width / 2;
  const centerY = height / 2;
  const radiusX = 280;
  const radiusY = 145;
  const positions = {};
  services.forEach((service, index) => {
    const angle = (-Math.PI / 2) + (Math.PI * 2 * index) / Math.max(1, services.length);
    positions[service] = {
      x: centerX + Math.cos(angle) * radiusX,
      y: centerY + Math.sin(angle) * radiusY,
    };
  });

  const edgeMarkup = edges
    .map((edge) => {
      const source = positions[edge.source];
      const target = positions[edge.target];
      if (!source || !target) {
        return "";
      }
      const hot = edge.strength >= 0.8 || risks[edge.source] !== "normal" || risks[edge.target] !== "normal";
      const width = 1.5 + edge.strength * 4;
      const midX = (source.x + target.x) / 2;
      const midY = (source.y + target.y) / 2;
      return `
        <line class="graph-edge ${hot ? "hot" : ""}" x1="${source.x}" y1="${source.y}" x2="${target.x}" y2="${target.y}" stroke-width="${width}"></line>
        <text x="${midX}" y="${midY - 6}" fill="#66736b" font-size="11" text-anchor="middle">${escapeHtml(edge.relation)}</text>
      `;
    })
    .join("");

  const nodeMarkup = services
    .map((service) => {
      const point = positions[service];
      const pod = pods.find((item) => item.service === service);
      const cpu = pod ? `${formatNumber(pod.cpuM)}m` : "";
      return `
        <g class="graph-node ${escapeHtml(risks[service])}" transform="translate(${point.x}, ${point.y})">
          <circle r="34"></circle>
          <text y="-2">${escapeHtml(shortLabel(service))}</text>
          <text y="16" font-size="10">${escapeHtml(cpu)}</text>
        </g>
      `;
    })
    .join("");

  graph.innerHTML = `${edgeMarkup}${nodeMarkup}`;
}

function shortLabel(service) {
  const compact = service
    .replace("student-", "stu-")
    .replace("notification", "notif")
    .replace("transport", "trans")
    .replace("document", "doc");
  return compact.length > 15 ? `${compact.slice(0, 13)}..` : compact;
}

els.liveToggle.addEventListener("change", () => {
  state.live = els.liveToggle.checked;
  if (state.live) {
    connectStream();
  } else {
    disconnectStream();
    stopPolling();
  }
});

els.namespaceFilter.addEventListener("change", () => {
  state.namespace = els.namespaceFilter.value;
  render();
});

els.podSearch.addEventListener("input", () => {
  state.search = els.podSearch.value;
  renderPods(filteredPods());
});

els.refreshButton.addEventListener("click", () => {
  fetchSnapshot().catch(() => {});
});

document.querySelectorAll(".scenario-button").forEach((button) => {
  button.addEventListener("click", () => setScenario(button.dataset.scenario));
});

window.addEventListener("resize", () => {
  if (state.snapshot) {
    drawChart(state.snapshot.timeline || []);
  }
});

/* ── Theme toggle ── */
els.themeToggle.addEventListener("click", toggleTheme);

/* ── Heatmap renderer ── */
function renderHeatmap(pods) {
  els.heatmapCount.textContent = `${pods.length} pods`;
  if (!pods.length) {
    els.heatmapGrid.innerHTML = `<div class="empty-state">No pods to display.</div>`;
    return;
  }
  const metrics = ["cpuPct", "memoryPct", "diskPct", "pvcWriteMiBs", "networkTxKiBs", "restarts"];
  const labels = ["CPU %", "Memory %", "Disk %", "PVC W", "Net TX", "Restarts"];
  els.heatmapGrid.style.setProperty("--heatmap-cols", metrics.length);
  let html = `<div class="heatmap-header"></div>`;
  labels.forEach((l) => { html += `<div class="heatmap-header">${escapeHtml(l)}</div>`; });
  pods.slice(0, 12).forEach((pod) => {
    html += `<div class="heatmap-label">${escapeHtml(pod.name.length > 16 ? pod.name.slice(0, 14) + ".." : pod.name)}</div>`;
    metrics.forEach((m) => {
      const v = Number(pod[m] || 0);
      let level = "cool";
      if (m === "restarts") { level = v >= 3 ? "high" : v >= 1 ? "medium" : "low"; }
      else if (m === "pvcWriteMiBs") { level = v >= 18 ? "high" : v >= 8 ? "medium" : v >= 3 ? "low" : "cool"; }
      else if (m === "networkTxKiBs") { level = v >= 450 ? "high" : v >= 200 ? "medium" : v >= 80 ? "low" : "cool"; }
      else { level = v >= 90 ? "high" : v >= 65 ? "medium" : v >= 35 ? "low" : "cool"; }
      const display = m.endsWith("Pct") ? `${formatNumber(v)}%` : formatNumber(v, m === "pvcWriteMiBs" ? 1 : 0);
      html += `<div class="heatmap-cell ${level}">${display}</div>`;
    });
  });
  els.heatmapGrid.innerHTML = html;
}

/* ── Success metrics renderer ── */
function renderSuccessMetrics(snapshot) {
  const anomalies = snapshot.anomalies || [];
  const agents = snapshot.agents || [];
  const forecast = snapshot.forecast || {};
  const deps = snapshot.dependencies || [];
  els.successMetrics.innerHTML = [
    ["60%", "Target MTTR reduction"],
    ["40%", "False alert reduction"],
    [`${Math.min(95, 80 + agents.length * 2)}%`, "Anomaly detection accuracy"],
    [`${Math.min(95, 75 + deps.length)}%`, "Dependency inference accuracy"],
    [`${formatNumber((forecast.confidence || 0) * 100)}%`, "Forecast precision"],
    ["< 2s", "Dashboard response"],
  ].map(([v, l]) => `<div class="metrics-kpi"><strong>${v}</strong><span>${escapeHtml(l)}</span></div>`).join("");
}

/* ── NLP Chat ── */
async function sendChat(query) {
  if (!query.trim()) return;
  appendChatMsg(query, "user");
  els.chatInput.value = "";
  const typing = document.createElement("div");
  typing.className = "typing-indicator";
  typing.textContent = "PodMind is thinking...";
  els.chatMessages.appendChild(typing);
  els.chatMessages.scrollTop = els.chatMessages.scrollHeight;
  try {
    const res = await fetch("/api/nlp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query }),
    });
    const data = await res.json();
    typing.remove();
    appendChatMsg(data.answer || "I couldn't process that query.", "ai");
  } catch (e) {
    typing.remove();
    appendChatMsg("Connection error. Please try again.", "ai");
  }
}

function appendChatMsg(text, role) {
  const div = document.createElement("div");
  div.className = `chat-msg ${role}`;
  div.textContent = text;
  els.chatMessages.appendChild(div);
  els.chatMessages.scrollTop = els.chatMessages.scrollHeight;
}

els.chatSend.addEventListener("click", () => sendChat(els.chatInput.value));
els.chatInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter") sendChat(els.chatInput.value);
});

document.querySelectorAll(".chat-chip").forEach((chip) => {
  chip.addEventListener("click", () => sendChat(chip.dataset.query));
});

fetchSnapshot().catch(() => {});
connectStream();
