const storageKey = "binlog-web-form";

const state = {
  currentTaskId: "",
  currentResult: null,
  displayFrom: null,
  displayTo: null,
  detailFrom: null,
  detailTo: null,
  detailPage: 1,
  detailPageSize: 20,
  detailTotal: 0,
  bigTxnFrom: null,
  bigTxnTo: null,
  aggregatePage: 1,
  aggregatePageSize: 12,
  aggregateTotal: 0,
  bigTxnPage: 1,
  bigTxnPageSize: 10,
  bigTxnTotal: 0,
  selectedBucketStart: "",
  pollTimer: null,
};

const elements = {};

document.addEventListener("DOMContentLoaded", () => {
  cacheElements();
  bindEvents();
  hydrateForm();
  syncBigTxnModeUI();
  syncMetricOptions();
  ensureDefaultTimes();
  updateCliPreview();
  loadTaskHistory();
});

function cacheElements() {
  [
    "analyzeForm",
    "host",
    "port",
    "user",
    "password",
    "startTime",
    "endTime",
    "startFile",
    "bigTxnThresholdLabel",
    "bigTxnThreshold",
    "bigTxnMode",
    "bigTxnBytesThresholdLabel",
    "bigTxnBytesThreshold",
    "submitBtn",
    "fillLastHourBtn",
    "cleanupBtn",
    "closePageBtn",
    "cliPreview",
    "taskStatus",
    "currentFile",
    "eventsProcessed",
    "filesProgress",
    "progressMessage",
    "taskHistory",
    "metricSelect",
    "rangeFrom",
    "rangeTo",
    "rangeSubtitle",
    "applyRangeBtn",
    "resetRangeBtn",
    "timelineMeta",
    "timelineFocus",
    "summaryCards",
    "timelineChart",
    "chartTooltip",
    "chartHint",
    "detailRangeLabel",
    "dmlFilterFrom",
    "dmlFilterTo",
    "dmlFilterType",
    "dmlFilterSchema",
    "dmlFilterTable",
    "dmlFilterRowCount",
    "dmlFilterTxnBytes",
    "dmlFilterBinlog",
    "dmlFilterGTID",
    "applyDmlFilterBtn",
    "resetDmlFilterBtn",
    "bigTxnRangeFrom",
    "bigTxnRangeTo",
    "applyBigTxnRangeBtn",
    "resetBigTxnRangeBtn",
    "bigTxnFilterStart",
    "bigTxnFilterEnd",
    "bigTxnFilterBinlog",
    "bigTxnFilterTable",
    "bigTxnFilterGTID",
    "bigTxnFilterBytes",
    "applyBigTxnFilterBtn",
    "resetBigTxnFilterBtn",
    "dmlTableBody",
    "aggregateTableBody",
    "prevAggregatePageBtn",
    "nextAggregatePageBtn",
    "aggregatePaginationInfo",
    "bigTxnStats",
    "bigTxnChart",
    "bigTxnHint",
    "bigTxnRangeLabel",
    "bigTxnTableBody",
    "prevBigTxnPageBtn",
    "nextBigTxnPageBtn",
    "bigTxnPaginationInfo",
    "prevPageBtn",
    "nextPageBtn",
    "paginationInfo",
  ].forEach((id) => {
    elements[id] = document.getElementById(id);
  });
}

function bindEvents() {
  elements.analyzeForm.addEventListener("submit", submitAnalysis);
  elements.fillLastHourBtn.addEventListener("click", fillLastHour);
  elements.cleanupBtn.addEventListener("click", cleanupFiles);
  elements.closePageBtn.addEventListener("click", closePage);
  elements.applyRangeBtn.addEventListener("click", applyDisplayRange);
  elements.resetRangeBtn.addEventListener("click", resetDisplayRange);
  elements.applyDmlFilterBtn.addEventListener("click", applyDmlFilter);
  elements.resetDmlFilterBtn.addEventListener("click", resetDmlFilter);
  elements.applyBigTxnRangeBtn.addEventListener("click", applyBigTxnRange);
  elements.resetBigTxnRangeBtn.addEventListener("click", resetBigTxnRange);
  elements.applyBigTxnFilterBtn.addEventListener("click", applyBigTxnFilter);
  elements.resetBigTxnFilterBtn.addEventListener("click", resetBigTxnFilter);
  elements.bigTxnMode.addEventListener("change", () => {
    syncBigTxnModeUI();
    syncMetricOptions();
    persistForm();
    updateCliPreview();
    if (state.currentResult) {
      renderChart();
      updateRangeSubtitle();
    }
  });
  elements.metricSelect.addEventListener("change", () => {
    renderChart();
    updateRangeSubtitle();
  });
  elements.prevPageBtn.addEventListener("click", () => changePage(-1));
  elements.nextPageBtn.addEventListener("click", () => changePage(1));
  elements.prevAggregatePageBtn.addEventListener("click", () => changeAggregatePage(-1));
  elements.nextAggregatePageBtn.addEventListener("click", () => changeAggregatePage(1));
  elements.prevBigTxnPageBtn.addEventListener("click", () => changeBigTxnPage(-1));
  elements.nextBigTxnPageBtn.addEventListener("click", () => changeBigTxnPage(1));

  [
    elements.host,
    elements.port,
    elements.user,
    elements.password,
    elements.startTime,
    elements.endTime,
    elements.startFile,
    elements.bigTxnThreshold,
    elements.bigTxnMode,
    elements.bigTxnBytesThreshold,
  ].forEach((input) => {
    input.addEventListener("input", () => {
      persistForm();
      updateCliPreview();
    });
  });
}

function hydrateForm() {
  const raw = localStorage.getItem(storageKey);
  if (!raw) {
    return;
  }
  try {
    const saved = JSON.parse(raw);
    setValue(elements.host, saved.host);
    setValue(elements.port, saved.port);
    setValue(elements.user, saved.user);
    setValue(elements.password, saved.password);
    setValue(elements.startTime, saved.startTime);
    setValue(elements.endTime, saved.endTime);
    setValue(elements.startFile, saved.startFile);
    setValue(elements.bigTxnThreshold, saved.bigTxnThreshold);
    setValue(elements.bigTxnMode, saved.bigTxnMode || "rows");
    setValue(elements.bigTxnBytesThreshold, saved.bigTxnBytesThreshold);
  } catch (_) {
    localStorage.removeItem(storageKey);
  }
}

function ensureDefaultTimes() {
  if (!elements.startTime.value || !elements.endTime.value) {
    fillLastHour();
  }
}

function fillLastHour() {
  const end = new Date();
  const start = new Date(end.getTime() - 60 * 60 * 1000);
  elements.startTime.value = toLocalInputValue(start);
  elements.endTime.value = toLocalInputValue(end);
  persistForm();
  updateCliPreview();
}

function persistForm() {
  localStorage.setItem(
    storageKey,
    JSON.stringify({
      host: elements.host.value,
      port: elements.port.value,
      user: elements.user.value,
      password: elements.password.value,
      startTime: elements.startTime.value,
      endTime: elements.endTime.value,
      startFile: elements.startFile.value,
      bigTxnThreshold: elements.bigTxnThreshold.value,
      bigTxnMode: elements.bigTxnMode.value,
      bigTxnBytesThreshold: elements.bigTxnBytesThreshold.value,
    })
  );
}

function updateCliPreview() {
  const mode = getSelectedBigTxnMode();
  const args = [
    "./bin/binlog-analyzer-linux-amd64",
    `-host "${elements.host.value || "127.0.0.1"}"`,
    `-port ${elements.port.value || 3306}`,
    `-user "${elements.user.value || "root"}"`,
  ];

  if (elements.password.value) {
    args.push(`-password "******"`);
  }
  if (elements.startTime.value) {
    args.push(`-start-time "${toApiTime(elements.startTime.value)}"`);
  }
  if (elements.endTime.value) {
    args.push(`-end-time "${toApiTime(elements.endTime.value)}"`);
  }
  if (elements.startFile.value) {
    args.push(`-start-file "${elements.startFile.value}"`);
  }
  if (mode === "rows" && elements.bigTxnThreshold.value && Number(elements.bigTxnThreshold.value) > 0) {
    args.push(`-big-txn-threshold ${elements.bigTxnThreshold.value}`);
  }
  if (mode !== "rows") {
    args.push(`-big-txn-mode "${mode}"`);
  }
  if (mode === "bytes" && elements.bigTxnBytesThreshold.value && Number(elements.bigTxnBytesThreshold.value) > 0) {
    args.push(`-big-txn-bytes-threshold ${elements.bigTxnBytesThreshold.value}`);
  }

  elements.cliPreview.textContent = args.join(" \\\n  ");
}

async function submitAnalysis(event) {
  event.preventDefault();
  const mode = getSelectedBigTxnMode();

  const payload = {
    host: elements.host.value.trim(),
    port: Number(elements.port.value || 3306),
    user: elements.user.value.trim(),
    password: elements.password.value,
    startTime: toApiTime(elements.startTime.value),
    endTime: toApiTime(elements.endTime.value),
    startFile: elements.startFile.value.trim(),
    bigTxnThreshold: mode === "rows" ? Number(elements.bigTxnThreshold.value || 0) : 0,
    bigTxnMode: mode,
    bigTxnBytesThreshold: mode === "bytes" ? Number(elements.bigTxnBytesThreshold.value || 0) : 0,
  };

  elements.submitBtn.disabled = true;
  elements.submitBtn.textContent = "提交中...";

  try {
    const task = await requestJSON("/api/tasks", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    state.currentTaskId = task.id;
    state.currentResult = null;
    state.displayFrom = null;
    state.displayTo = null;
    state.detailFrom = null;
    state.detailTo = null;
    state.detailPage = 1;
    state.detailTotal = 0;
    state.bigTxnFrom = null;
    state.bigTxnTo = null;
    state.aggregatePage = 1;
    state.aggregateTotal = 0;
    state.bigTxnPage = 1;
    state.bigTxnTotal = 0;
    state.selectedBucketStart = "";

    updateTaskStatus(task);
    renderEmptyResult();
    startPolling(task.id);
    loadTaskHistory();
  } catch (error) {
    window.alert(error.message);
  } finally {
    elements.submitBtn.disabled = false;
    elements.submitBtn.textContent = "开始分析";
  }
}

function startPolling(taskId) {
  if (state.pollTimer) {
    window.clearInterval(state.pollTimer);
  }
  pollTask(taskId);
  state.pollTimer = window.setInterval(() => pollTask(taskId), 2000);
}

async function pollTask(taskId) {
  try {
    const task = await requestJSON(`/api/tasks/${taskId}`);
    updateTaskStatus(task);
    renderTaskHistory([task], true);

    if (task.status === "completed") {
      window.clearInterval(state.pollTimer);
      state.pollTimer = null;
      await loadResult(taskId);
      loadTaskHistory();
    } else if (task.status === "failed") {
      window.clearInterval(state.pollTimer);
      state.pollTimer = null;
      loadTaskHistory();
    }
  } catch (error) {
    elements.progressMessage.textContent = error.message;
  }
}

async function loadTaskHistory() {
  try {
    const data = await requestJSON("/api/tasks");
    renderTaskHistory(data.tasks || []);

    if (!state.currentTaskId && Array.isArray(data.tasks) && data.tasks.length > 0) {
      const latest = data.tasks[0];
      state.currentTaskId = latest.id;
      updateTaskStatus(latest);
      if (latest.status === "running" || latest.status === "queued") {
        startPolling(latest.id);
      } else if (latest.status === "completed") {
        await loadResult(latest.id);
      }
    }
  } catch (error) {
    elements.taskHistory.textContent = error.message;
  }
}

function renderTaskHistory(tasks, prepend = false) {
  if (!Array.isArray(tasks) || tasks.length === 0) {
    if (!prepend) {
      elements.taskHistory.classList.add("empty");
      elements.taskHistory.textContent = "暂无任务记录";
    }
    return;
  }

  const html = tasks
    .slice(0, 8)
    .map((task) => {
      const statusClass = `status-${task.status || "queued"}`;
      return `
        <div class="task-row">
          <div class="task-id">${escapeHTML(task.id)}</div>
          <div class="${statusClass}">${escapeHTML(task.status || "-")}</div>
          <div class="task-time">${escapeHTML(task.request.startTime || "-")} ~ ${escapeHTML(task.request.endTime || "-")}</div>
          <div class="task-time">${escapeHTML(task.request.host || "-")}:${escapeHTML(task.request.port || "-")}</div>
        </div>
      `;
    })
    .join("");

  elements.taskHistory.classList.remove("empty");
  elements.taskHistory.innerHTML = html;
}

function updateTaskStatus(task) {
  const status = task.status || "unknown";
  elements.taskStatus.textContent = status;
  elements.taskStatus.className = `status-${status}`;
  elements.currentFile.textContent = task.progress?.currentFile || "-";
  elements.eventsProcessed.textContent = formatNumber(task.progress?.eventsProcessed || 0);
  elements.filesProgress.textContent = `${task.progress?.filesCompleted || 0} / ${task.progress?.filesTotal || 0}`;
  elements.progressMessage.textContent = task.error || task.progress?.message || "等待任务启动";
}

async function loadResult(taskId) {
  const result = await requestJSON(`/api/tasks/${taskId}/result`);
  state.currentResult = result;
  syncMetricOptions();

  const summary = result.summary;
  state.displayFrom = new Date(summary.startTime);
  state.displayTo = new Date(summary.endTime);
  state.detailFrom = new Date(summary.startTime);
  state.detailTo = new Date(summary.endTime);
  state.bigTxnFrom = new Date(summary.startTime);
  state.bigTxnTo = new Date(summary.endTime);
  state.detailPage = 1;
  state.detailTotal = 0;
  state.aggregatePage = 1;
  state.aggregateTotal = 0;
  state.bigTxnPage = 1;
  state.bigTxnTotal = 0;
  state.selectedBucketStart = "";

  elements.rangeFrom.value = toLocalInputValue(state.displayFrom);
  elements.rangeTo.value = toLocalInputValue(state.displayTo);
  updateRangeSubtitle();
  syncDmlFiltersToDetailRange();
  elements.bigTxnRangeFrom.value = toLocalInputValue(state.bigTxnFrom);
  elements.bigTxnRangeTo.value = toLocalInputValue(state.bigTxnTo);
  elements.bigTxnRangeLabel.textContent = `当前大事务时间范围：${formatDateTime(state.bigTxnFrom)} ~ ${formatDateTime(state.bigTxnTo)}`;
  syncBigTxnFiltersToRange();

  renderSummaryCards(summary);
  renderChart();
  await refreshRangeData();
}

function renderSummaryCards(summary) {
  const bigTxnMode = summary.bigTxnMode || "rows";
  const cards = [
    { label: "总 DML 行数", value: formatNumber(summary.totalRows || 0), className: "highlight" },
    { label: "DML 记录数", value: formatNumber(summary.eventCount || 0), className: "highlight" },
    { label: "大事务判定", value: bigTxnMode === "bytes" ? "按事务字节" : "按影响行数", className: "highlight" },
    { label: "大事务阈值", value: formatBigTxnThreshold(summary), className: "highlight" },
  ];

  if (summary.dmlTruncated) {
    cards.push({ label: "明细截断", value: `已保存 ${formatNumber(summary.dmlStoredEvents || 0)} 条`, className: "highlight" });
  }

  elements.summaryCards.innerHTML = cards
    .map(
      (card) => `
        <article class="metric-card ${card.className || ""}">
          <span>${escapeHTML(card.label)}</span>
          <strong>${escapeHTML(card.value)}</strong>
        </article>
      `
    )
    .join("");
}

function renderChart() {
  const svg = elements.timelineChart;
  svg.innerHTML = "";

  if (!state.currentResult) {
    renderChartPlaceholder("等待分析结果");
    return;
  }

  const visibleBuckets = getVisibleBuckets();
  if (visibleBuckets.length === 0) {
    renderChartPlaceholder("当前筛选范围内没有时间桶数据");
    return;
  }

  const width = 960;
  const height = 340;
  const margin = { top: 18, right: 24, bottom: 52, left: 72 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;
  const metric = elements.metricSelect.value;
  const palette = metricPalette(metric);
  const values = visibleBuckets.map((bucket) => metricValue(bucket, metric));
  const maxValue = Math.max(...values, 1);
  const stepX = visibleBuckets.length === 1 ? 0 : chartWidth / (visibleBuckets.length - 1);
  renderTimelineMeta(visibleBuckets, metric);
  updateTimelineFocus();

  const g = createSVG("g", { transform: `translate(${margin.left},${margin.top})` });
  svg.appendChild(g);

  for (let i = 0; i <= 4; i += 1) {
    const y = (chartHeight / 4) * i;
    const line = createSVG("line", {
      x1: 0,
      y1: y,
      x2: chartWidth,
      y2: y,
      stroke: i === 4 ? "rgba(19, 102, 214, 0.2)" : "rgba(19, 102, 214, 0.08)",
      "stroke-width": 1,
    });
    g.appendChild(line);

    const value = formatMetricAxisValue(maxValue - (maxValue / 4) * i, metric);
    const label = createSVG("text", {
      x: -12,
      y: y + 4,
      "text-anchor": "end",
      fill: "#5b6778",
      "font-size": "12",
    });
    label.textContent = value;
    g.appendChild(label);
  }

  const points = visibleBuckets.map((bucket, index) => {
    const x = visibleBuckets.length === 1 ? chartWidth / 2 : stepX * index;
    const value = metricValue(bucket, metric);
    const y = chartHeight - (value / maxValue) * chartHeight;
    return { x, y, bucket, value };
  });

  const barWidth = Math.max(Math.min(chartWidth / Math.max(visibleBuckets.length, 1) * 0.68, 18), 6);
  points.forEach((point) => {
    const rectHeight = Math.max(chartHeight - point.y, point.value > 0 ? 4 : 0);
    g.appendChild(
      createSVG("rect", {
        x: point.x - barWidth / 2,
        y: chartHeight - rectHeight,
        width: barWidth,
        height: rectHeight,
        rx: 5,
        fill: palette.bar,
      })
    );
  });

  if (points.length > 1) {
    const areaPath = points
      .map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`)
      .join(" ");
    const closedPath = `${areaPath} L ${points[points.length - 1].x} ${chartHeight} L ${points[0].x} ${chartHeight} Z`;
    g.appendChild(
      createSVG("path", {
        d: closedPath,
        fill: palette.area,
      })
    );
  }

  g.appendChild(
    createSVG("polyline", {
      points: points.map((point) => `${point.x},${point.y}`).join(" "),
      fill: "none",
      stroke: palette.line,
      "stroke-width": 3,
      "stroke-linejoin": "round",
      "stroke-linecap": "round",
    })
  );

  const clickableWidth = Math.max(chartWidth / Math.max(visibleBuckets.length, 1), 12);
  points.forEach((point) => {
    if (visibleBuckets.length <= 140) {
      g.appendChild(
        createSVG("circle", {
          cx: point.x,
          cy: point.y,
          r: 4.5,
          fill: state.selectedBucketStart === point.bucket.start ? palette.active : "#ffffff",
          stroke: palette.line,
          "stroke-width": 2,
        })
      );
    }

    const bucketRect = createSVG("rect", {
      x: point.x - clickableWidth / 2,
      y: 0,
      width: clickableWidth,
      height: chartHeight,
      fill: "transparent",
      cursor: "pointer",
    });
    bucketRect.addEventListener("mouseenter", () => {
      showChartTooltip(point, metric);
    });
    bucketRect.addEventListener("mousemove", (event) => {
      moveChartTooltip(event);
    });
    bucketRect.addEventListener("mouseleave", () => {
      hideChartTooltip();
      updateChartHint();
    });
    bucketRect.addEventListener("click", () => {
      state.selectedBucketStart = point.bucket.start;
      state.detailFrom = new Date(point.bucket.start);
      state.detailTo = new Date(point.bucket.end);
      state.detailPage = 1;
      state.aggregatePage = 1;
      renderChart();
      refreshRangeData();
    });
    g.appendChild(bucketRect);
  });

  const tickCount = Math.min(6, visibleBuckets.length);
  for (let i = 0; i < tickCount; i += 1) {
    const index = visibleBuckets.length === 1 ? 0 : Math.round((visibleBuckets.length - 1) * (i / Math.max(tickCount - 1, 1)));
    const point = points[index];
    const label = createSVG("text", {
      x: point.x,
      y: chartHeight + 26,
      "text-anchor": i === 0 ? "start" : i === tickCount - 1 ? "end" : "middle",
      fill: "#5b6778",
      "font-size": "12",
    });
    label.textContent = axisLabel(point.bucket.start);
    g.appendChild(label);
  }

  updateChartHint();

  const legend = createSVG("text", {
    x: margin.left,
    y: 14,
    fill: palette.line,
    "font-size": "12",
    "font-weight": "700",
  });
  legend.textContent = `当前指标: ${metricLabel(metric)}`;
  svg.appendChild(legend);
}

function renderChartPlaceholder(message) {
  const svg = elements.timelineChart;
  svg.innerHTML = "";
  const text = createSVG("text", {
    x: 480,
    y: 170,
    "text-anchor": "middle",
    fill: "#5b6778",
    "font-size": "16",
  });
  text.textContent = message;
  svg.appendChild(text);
}

function updateChartHint() {
  if (!state.currentResult) {
    elements.chartHint.textContent = "提交任务后，这里会显示 DML 折线图。";
    return;
  }
  const from = state.displayFrom ? formatDateTime(state.displayFrom) : "-";
  const to = state.displayTo ? formatDateTime(state.displayTo) : "-";
  const metric = metricLabel(elements.metricSelect.value);
  const bigTxnNote = getBigTxnMode() === "bytes"
    ? `大事务按事务字节判定，阈值 ${formatBigTxnThreshold(state.currentResult.summary)}`
    : `大事务按影响行数判定，阈值 ${formatBigTxnThreshold(state.currentResult.summary)}`;
  const detail = state.detailFrom && state.detailTo
    ? `，当前明细区间 ${formatDateTime(state.detailFrom)} ~ ${formatDateTime(state.detailTo)}`
    : "";
  elements.chartHint.textContent = `当前图表范围 ${from} ~ ${to}，指标为 ${metric}${detail}。点击折线图中的时间桶，可快速查看该段 DML。${bigTxnNote}。`;
}

function getVisibleBuckets() {
  if (!state.currentResult) {
    return [];
  }
  const from = state.displayFrom || new Date(state.currentResult.summary.startTime);
  const to = state.displayTo || new Date(state.currentResult.summary.endTime);
  return (state.currentResult.buckets || []).filter((bucket) => {
    const start = new Date(bucket.start);
    const end = new Date(bucket.end);
    return !(end < from || start > to);
  });
}

function metricValue(bucket, metric) {
  const bytesMode = getBigTxnMode() === "bytes" && metric === "total";
  if (bytesMode) {
    return Number(bucket.sumBigTxnBytes || 0);
  }
  switch (metric) {
    case "insert":
      return bucket.insertRows || 0;
    case "update":
      return bucket.updateRows || 0;
    case "delete":
      return bucket.deleteRows || 0;
    default:
      return bucket.totalRows || 0;
  }
}

function metricLabel(metric) {
  if (getBigTxnMode() === "bytes" && metric === "total") {
    return "大事务总字节";
  }
  switch (metric) {
    case "insert":
      return "INSERT";
    case "update":
      return "UPDATE";
    case "delete":
      return "DELETE";
    default:
      return "DML";
  }
}

function metricPalette(metric) {
  switch (metric) {
    case "insert":
      return { line: "#198754", area: "rgba(25, 135, 84, 0.14)", bar: "rgba(25, 135, 84, 0.22)", active: "#0f5132" };
    case "update":
      return { line: "#b7791f", area: "rgba(183, 121, 31, 0.16)", bar: "rgba(183, 121, 31, 0.22)", active: "#7c5a14" };
    case "delete":
      return { line: "#b42318", area: "rgba(180, 35, 24, 0.14)", bar: "rgba(180, 35, 24, 0.20)", active: "#7a1a12" };
    default:
      return { line: "#1366d6", area: "rgba(19, 102, 214, 0.12)", bar: "rgba(19, 102, 214, 0.18)", active: "#0b3a7d" };
  }
}

async function applyDisplayRange() {
  if (!state.currentResult) {
    return;
  }
  const from = parseLocalDate(elements.rangeFrom.value);
  const to = parseLocalDate(elements.rangeTo.value);
  if (!from || !to || to < from) {
    window.alert("请选择合法的显示时间范围");
    return;
  }
  state.displayFrom = from;
  state.displayTo = to;
  state.detailFrom = from;
  state.detailTo = to;
  state.detailPage = 1;
  state.aggregatePage = 1;
  state.selectedBucketStart = "";
  updateRangeSubtitle();
  renderChart();
  await refreshRangeData();
}

async function resetDisplayRange() {
  if (!state.currentResult) {
    return;
  }
  const summary = state.currentResult.summary;
  state.displayFrom = new Date(summary.startTime);
  state.displayTo = new Date(summary.endTime);
  state.detailFrom = new Date(summary.startTime);
  state.detailTo = new Date(summary.endTime);
  state.bigTxnFrom = new Date(summary.startTime);
  state.bigTxnTo = new Date(summary.endTime);
  state.detailPage = 1;
  state.detailTotal = 0;
  state.aggregatePage = 1;
  state.aggregateTotal = 0;
  state.bigTxnPage = 1;
  state.bigTxnTotal = 0;
  state.selectedBucketStart = "";

  elements.rangeFrom.value = toLocalInputValue(state.displayFrom);
  elements.rangeTo.value = toLocalInputValue(state.displayTo);
  updateRangeSubtitle();
  syncDmlFiltersToDetailRange();
  elements.bigTxnRangeFrom.value = toLocalInputValue(state.bigTxnFrom);
  elements.bigTxnRangeTo.value = toLocalInputValue(state.bigTxnTo);
  elements.bigTxnRangeLabel.textContent = `当前大事务时间范围：${formatDateTime(state.bigTxnFrom)} ~ ${formatDateTime(state.bigTxnTo)}`;
  syncBigTxnFiltersToRange();

  renderChart();
  await refreshRangeData();
}

async function refreshRangeData() {
  if (!state.currentTaskId || !state.currentResult) {
    return;
  }

  const from = state.detailFrom || state.displayFrom;
  const to = state.detailTo || state.displayTo;
  if (!from || !to) {
    return;
  }

  elements.detailRangeLabel.textContent = `当前明细范围：${formatDateTime(from)} ~ ${formatDateTime(to)}`;

  const query = new URLSearchParams({
    from: toApiTime(elements.dmlFilterFrom.value || toLocalInputValue(from)),
    to: toApiTime(elements.dmlFilterTo.value || toLocalInputValue(to)),
    limit: String(state.detailPageSize),
    offset: String((state.detailPage - 1) * state.detailPageSize),
  });
  if (elements.dmlFilterType.value) {
    query.set("type", elements.dmlFilterType.value);
  }
  if (elements.dmlFilterSchema.value.trim()) {
    query.set("schema", elements.dmlFilterSchema.value.trim());
  }
  if (elements.dmlFilterTable.value.trim()) {
    query.set("table", elements.dmlFilterTable.value.trim());
  }
  if (elements.dmlFilterRowCount.value.trim()) {
    query.set("row_count", elements.dmlFilterRowCount.value.trim());
  }
  if (elements.dmlFilterTxnBytes.value.trim()) {
    query.set("transaction_bytes", elements.dmlFilterTxnBytes.value.trim());
  }
  if (elements.dmlFilterBinlog.value.trim()) {
    query.set("binlog", elements.dmlFilterBinlog.value.trim());
  }
  if (elements.dmlFilterGTID.value.trim()) {
    query.set("gtid", elements.dmlFilterGTID.value.trim());
  }

  try {
    const [dmlData, tableData] = await Promise.all([
      requestJSON(`/api/tasks/${state.currentTaskId}/dml?${query.toString()}`),
      requestJSON(`/api/tasks/${state.currentTaskId}/tables?${query.toString()}`),
    ]);
    state.detailTotal = Number(dmlData.total || 0);
    state.aggregateTotal = Array.isArray(tableData.items) ? tableData.items.length : 0;
    state.bigTxnTotal = getFilteredBigTransactions().length;
    renderDMLTable(dmlData.items || []);
    renderAggregateTable(tableData.items || []);
    renderBigTransactions(getFilteredBigTransactions());
    renderPagination();
    renderAggregatePagination();
    renderBigTxnPagination();
  } catch (error) {
    state.detailTotal = 0;
    state.aggregateTotal = 0;
    state.bigTxnTotal = 0;
    elements.dmlTableBody.innerHTML = `<tr><td colspan="8" class="empty-cell">${escapeHTML(error.message)}</td></tr>`;
    elements.aggregateTableBody.innerHTML = `<tr><td colspan="6" class="empty-cell">${escapeHTML(error.message)}</td></tr>`;
    renderBigTransactions([]);
    renderPagination();
    renderAggregatePagination();
    renderBigTxnPagination();
  }
}

function renderDMLTable(items) {
  if (!Array.isArray(items) || items.length === 0) {
    const extra = getBigTxnMode() === "bytes" ? "；当前大事务模式为按事务字节判定" : "";
    elements.dmlTableBody.innerHTML = `<tr><td colspan="8" class="empty-cell">当前时间段没有 DML 明细${escapeHTML(extra)}</td></tr>`;
    return;
  }

  elements.dmlTableBody.innerHTML = items
    .map(
      (item) => `
        <tr>
          <td>${escapeHTML(formatDateTime(new Date(item.time)))}</td>
          <td><span class="tag">${escapeHTML(item.type)}</span></td>
          <td>${escapeHTML(item.schema)}</td>
          <td>${escapeHTML(item.table)}</td>
          <td>${escapeHTML(formatNumber(item.rowCount || 0))}</td>
          <td>${escapeHTML(formatBigTxnMetric(item.transactionLength || 0, "bytes"))}</td>
          <td>${escapeHTML(item.binlogFile || "-")}</td>
          <td>${escapeHTML(item.gtid || "-")}</td>
        </tr>
      `
    )
    .join("");
}

function renderAggregateTable(items) {
  if (!Array.isArray(items) || items.length === 0) {
    const extra = getBigTxnMode() === "bytes" ? "；对象列表仍按 DML 行数聚合" : "";
    elements.aggregateTableBody.innerHTML = `<tr><td colspan="6" class="empty-cell">当前时间段没有聚合结果${escapeHTML(extra)}</td></tr>`;
    return;
  }

  const start = (state.aggregatePage - 1) * state.aggregatePageSize;
  const pageItems = items.slice(start, start + state.aggregatePageSize);

  elements.aggregateTableBody.innerHTML = pageItems
    .map(
      (item) => `
        <tr>
          <td>${escapeHTML(item.schema)}</td>
          <td>${escapeHTML(item.table)}</td>
          <td>${escapeHTML(formatNumber(item.insertRows || 0))}</td>
          <td>${escapeHTML(formatNumber(item.updateRows || 0))}</td>
          <td>${escapeHTML(formatNumber(item.deleteRows || 0))}</td>
          <td>${escapeHTML(formatNumber(item.totalRows || 0))}</td>
        </tr>
      `
    )
    .join("");
}

function renderBigTransactions(items) {
  renderBigTxnChart(items);
  const mode = getBigTxnMode();
  const metricLabelText = mode === "bytes" ? "最大字节" : "最大行数";
  if (!Array.isArray(items) || items.length === 0) {
    elements.bigTxnStats.innerHTML = `
      <article class="metric-card"><span>大事务数量</span><strong>0</strong></article>
      <article class="metric-card"><span>${escapeHTML(metricLabelText)}</span><strong>0</strong></article>
      <article class="metric-card"><span>覆盖文件数</span><strong>0</strong></article>
    `;
    elements.bigTxnHint.textContent = "当前时间段暂无大事务数据";
    elements.bigTxnTableBody.innerHTML = `<tr><td colspan="7" class="empty-cell">暂无大事务数据</td></tr>`;
    return;
  }

  const maxMetricValue = mode === "bytes"
    ? Math.max(...items.map((item) => item.transactionLength || 0), 0)
    : Math.max(...items.map((item) => item.rowCount || 0), 0);
  const fileCount = new Set(items.map((item) => item.binlogFile || "")).size;
  elements.bigTxnStats.innerHTML = `
    <article class="metric-card"><span>大事务数量</span><strong>${escapeHTML(formatNumber(items.length))}</strong></article>
    <article class="metric-card"><span>${escapeHTML(metricLabelText)}</span><strong>${escapeHTML(formatBigTxnMetric(maxMetricValue, mode))}</strong></article>
    <article class="metric-card"><span>覆盖文件数</span><strong>${escapeHTML(formatNumber(fileCount))}</strong></article>
  `;
  elements.bigTxnHint.textContent = `当前时间段共发现 ${formatNumber(items.length)} 个大事务，当前判定模式为${mode === "bytes" ? "事务字节" : "影响行数"}，阈值 ${formatBigTxnThreshold(state.currentResult?.summary)}。`;

  const start = (state.bigTxnPage - 1) * state.bigTxnPageSize;
  const pageItems = items.slice(start, start + state.bigTxnPageSize);
  elements.bigTxnTableBody.innerHTML = pageItems
    .map((item) => `
      <tr>
        <td>${escapeHTML(formatDateTime(new Date(item.startTime)))}</td>
        <td>${escapeHTML(formatDateTime(new Date(item.endTime)))}</td>
        <td>${escapeHTML(formatNumber(item.rowCount || 0))}</td>
        <td>${escapeHTML(formatNumber(item.transactionLength || 0))}</td>
        <td>${escapeHTML(item.binlogFile || "-")}</td>
        <td>${escapeHTML(((item.tables || []).join(", ")) || "-")}</td>
        <td>${escapeHTML(item.gtid || "-")}</td>
      </tr>
    `)
    .join("");
}

function renderEmptyResult() {
  elements.summaryCards.innerHTML = "";
  elements.timelineMeta.innerHTML = "";
  elements.rangeFrom.value = "";
  elements.rangeTo.value = "";
  elements.rangeSubtitle.textContent = "当前范围：未设置";
  elements.timelineFocus.textContent = "当前未选中时间桶";
  renderChartPlaceholder("任务运行中，等待结果");
  elements.chartHint.textContent = "任务开始后，这里会自动刷新。";
  elements.detailRangeLabel.textContent = "当前未选择时间范围";
  state.detailPage = 1;
  state.detailTotal = 0;
  state.bigTxnFrom = null;
  state.bigTxnTo = null;
  elements.bigTxnRangeLabel.textContent = "当前大事务时间范围：未设置";
  syncBigTxnFiltersToRange();
  state.aggregatePage = 1;
  state.aggregateTotal = 0;
  state.bigTxnPage = 1;
  state.bigTxnTotal = 0;
  elements.dmlTableBody.innerHTML = `<tr><td colspan="8" class="empty-cell">暂无数据</td></tr>`;
  elements.aggregateTableBody.innerHTML = `<tr><td colspan="6" class="empty-cell">暂无数据</td></tr>`;
  renderBigTransactions([]);
  renderPagination();
  renderAggregatePagination();
  renderBigTxnPagination();
}

function closePage() {
  fetch("/api/shutdown", { method: "POST" })
    .then(() => {
      elements.progressMessage.textContent = "程序正在关闭";
      window.setTimeout(() => {
        window.close();
        window.setTimeout(() => {
          if (!document.hidden) {
            document.body.innerHTML = '<div style="padding:24px;font:14px/1.6 sans-serif;color:#132238">程序已关闭，可以直接关闭此页面。</div>';
          }
        }, 250);
      }, 300);
    })
    .catch((error) => {
      window.alert(error.message);
    });
}

function cleanupFiles() {
  const confirmed = window.confirm("将删除历史分析报告和 Web 运行日志，不会删除程序二进制。是否继续？");
  if (!confirmed) {
    return;
  }

  fetch("/api/cleanup", { method: "POST" })
    .then(async (response) => {
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data.error || "清理失败");
      }
      const freed = formatBytes(data.freedBytes || 0);
      elements.progressMessage.textContent = `清理完成，共删除 ${formatNumber(data.deletedCount || 0)} 个文件，释放 ${freed}`;
    })
    .catch((error) => {
      window.alert(error.message);
    });
}

function applyDmlFilter() {
  state.detailPage = 1;
  refreshRangeData();
}

function resetDmlFilter() {
  syncDmlFiltersToDetailRange();
  elements.dmlFilterType.value = "";
  elements.dmlFilterSchema.value = "";
  elements.dmlFilterTable.value = "";
  elements.dmlFilterRowCount.value = "";
  elements.dmlFilterTxnBytes.value = "";
  elements.dmlFilterBinlog.value = "";
  elements.dmlFilterGTID.value = "";
  state.detailPage = 1;
  refreshRangeData();
}

function syncDmlFiltersToDetailRange() {
  const from = state.detailFrom || state.displayFrom;
  const to = state.detailTo || state.displayTo;
  elements.dmlFilterFrom.value = from ? toLocalInputValue(from) : "";
  elements.dmlFilterTo.value = to ? toLocalInputValue(to) : "";
}

function syncBigTxnFiltersToRange() {
  const from = state.bigTxnFrom || state.displayFrom;
  const to = state.bigTxnTo || state.displayTo;
  if (elements.bigTxnFilterStart) {
    elements.bigTxnFilterStart.value = from ? toLocalInputValue(from) : "";
  }
  if (elements.bigTxnFilterEnd) {
    elements.bigTxnFilterEnd.value = to ? toLocalInputValue(to) : "";
  }
}

function renderPagination() {
  const totalPages = Math.max(1, Math.ceil(state.detailTotal / state.detailPageSize));
  if (state.detailPage > totalPages) {
    state.detailPage = totalPages;
  }
  elements.paginationInfo.textContent = `第 ${state.detailPage} / ${totalPages} 页，共 ${formatNumber(state.detailTotal)} 条`;
  elements.prevPageBtn.disabled = state.detailPage <= 1;
  elements.nextPageBtn.disabled = state.detailPage >= totalPages || state.detailTotal === 0;
}

async function changePage(step) {
  const totalPages = Math.max(1, Math.ceil(state.detailTotal / state.detailPageSize));
  const nextPage = state.detailPage + step;
  if (nextPage < 1 || nextPage > totalPages) {
    return;
  }
  state.detailPage = nextPage;
  await refreshRangeData();
}

function renderAggregatePagination() {
  const totalPages = Math.max(1, Math.ceil(state.aggregateTotal / state.aggregatePageSize));
  if (state.aggregatePage > totalPages) {
    state.aggregatePage = totalPages;
  }
  elements.aggregatePaginationInfo.textContent = `第 ${state.aggregatePage} / ${totalPages} 页，共 ${formatNumber(state.aggregateTotal)} 条`;
  elements.prevAggregatePageBtn.disabled = state.aggregatePage <= 1;
  elements.nextAggregatePageBtn.disabled = state.aggregatePage >= totalPages || state.aggregateTotal === 0;
}

async function changeAggregatePage(step) {
  const totalPages = Math.max(1, Math.ceil(state.aggregateTotal / state.aggregatePageSize));
  const nextPage = state.aggregatePage + step;
  if (nextPage < 1 || nextPage > totalPages) {
    return;
  }
  state.aggregatePage = nextPage;
  await refreshRangeData();
}

function renderBigTxnPagination() {
  const totalPages = Math.max(1, Math.ceil(state.bigTxnTotal / state.bigTxnPageSize));
  if (state.bigTxnPage > totalPages) {
    state.bigTxnPage = totalPages;
  }
  elements.bigTxnPaginationInfo.textContent = `第 ${state.bigTxnPage} / ${totalPages} 页，共 ${formatNumber(state.bigTxnTotal)} 条`;
  elements.prevBigTxnPageBtn.disabled = state.bigTxnPage <= 1;
  elements.nextBigTxnPageBtn.disabled = state.bigTxnPage >= totalPages || state.bigTxnTotal === 0;
}

async function changeBigTxnPage(step) {
  const totalPages = Math.max(1, Math.ceil(state.bigTxnTotal / state.bigTxnPageSize));
  const nextPage = state.bigTxnPage + step;
  if (nextPage < 1 || nextPage > totalPages) {
    return;
  }
  state.bigTxnPage = nextPage;
  renderBigTransactions(getFilteredBigTransactions());
  renderBigTxnPagination();
}

function getFilteredBigTransactions() {
  if (!state.currentResult) {
    return [];
  }
  const from = parseLocalDate(elements.bigTxnFilterStart?.value) || state.bigTxnFrom || state.displayFrom;
  const to = parseLocalDate(elements.bigTxnFilterEnd?.value) || state.bigTxnTo || state.displayTo;
  const binlog = elements.bigTxnFilterBinlog?.value.trim().toLowerCase() || "";
  const table = elements.bigTxnFilterTable?.value.trim().toLowerCase() || "";
  const gtid = elements.bigTxnFilterGTID?.value.trim().toLowerCase() || "";
  const bytes = Number(elements.bigTxnFilterBytes?.value || 0);
  return (state.currentResult.bigTransactions || []).filter((item) => {
    const start = new Date(item.startTime);
    if (start < from || start > to) {
      return false;
    }
    if (binlog && !(item.binlogFile || "").toLowerCase().includes(binlog)) {
      return false;
    }
    if (table) {
      const joinedTables = (item.tables || []).join(", ").toLowerCase();
      if (!joinedTables.includes(table)) {
        return false;
      }
    }
    if (gtid && !(item.gtid || "").toLowerCase().includes(gtid)) {
      return false;
    }
    if (bytes > 0 && Number(item.transactionLength || 0) < bytes) {
      return false;
    }
    return true;
  });
}

async function applyBigTxnRange() {
  if (!state.currentResult) {
    return;
  }
  const from = parseLocalDate(elements.bigTxnRangeFrom.value);
  const to = parseLocalDate(elements.bigTxnRangeTo.value);
  if (!from || !to || to < from) {
    window.alert("请选择合法的大事务时间范围");
    return;
  }
  state.bigTxnFrom = from;
  state.bigTxnTo = to;
  state.bigTxnPage = 1;
  elements.bigTxnRangeLabel.textContent = `当前大事务时间范围：${formatDateTime(state.bigTxnFrom)} ~ ${formatDateTime(state.bigTxnTo)}`;
  syncBigTxnFiltersToRange();
  state.bigTxnTotal = getFilteredBigTransactions().length;
  renderBigTransactions(getFilteredBigTransactions());
  renderBigTxnPagination();
}

async function resetBigTxnRange() {
  if (!state.currentResult) {
    return;
  }
  const summary = state.currentResult.summary;
  state.bigTxnFrom = new Date(summary.startTime);
  state.bigTxnTo = new Date(summary.endTime);
  state.bigTxnPage = 1;
  elements.bigTxnRangeFrom.value = toLocalInputValue(state.bigTxnFrom);
  elements.bigTxnRangeTo.value = toLocalInputValue(state.bigTxnTo);
  elements.bigTxnRangeLabel.textContent = `当前大事务时间范围：${formatDateTime(state.bigTxnFrom)} ~ ${formatDateTime(state.bigTxnTo)}`;
  syncBigTxnFiltersToRange();
  state.bigTxnTotal = getFilteredBigTransactions().length;
  renderBigTransactions(getFilteredBigTransactions());
  renderBigTxnPagination();
}

function applyBigTxnFilter() {
  state.bigTxnPage = 1;
  state.bigTxnTotal = getFilteredBigTransactions().length;
  renderBigTransactions(getFilteredBigTransactions());
  renderBigTxnPagination();
}

function resetBigTxnFilter() {
  syncBigTxnFiltersToRange();
  elements.bigTxnFilterBinlog.value = "";
  elements.bigTxnFilterTable.value = "";
  elements.bigTxnFilterGTID.value = "";
  elements.bigTxnFilterBytes.value = "";
  state.bigTxnPage = 1;
  state.bigTxnTotal = getFilteredBigTransactions().length;
  renderBigTransactions(getFilteredBigTransactions());
  renderBigTxnPagination();
}

function renderBigTxnChart(items) {
  const svg = elements.bigTxnChart;
  svg.innerHTML = "";
  if (!Array.isArray(items) || items.length === 0) {
    renderGenericChartPlaceholder(svg, 130, "当前时间段暂无大事务");
    return;
  }

  const sortedItems = [...items]
    .sort((a, b) => new Date(a.startTime) - new Date(b.startTime))
    .slice(0, 400);
  const width = 960;
  const height = 220;
  const margin = { top: 30, right: 108, bottom: 40, left: 52 };
  const chartWidth = width - margin.left - margin.right;
  const chartHeight = height - margin.top - margin.bottom;
  const buckets = buildBigTxnBuckets(sortedItems);
  const maxCount = Math.max(...buckets.map((item) => item.count || 0), 1);
  const useBytes = getBigTxnMode() === "bytes";
  const maxMetric = Math.max(...buckets.map((item) => useBytes ? item.maxBytes || 0 : item.maxRows || 0), 1);
  const g = createSVG("g", { transform: `translate(${margin.left},${margin.top})` });
  svg.appendChild(g);

  for (let i = 0; i <= 3; i += 1) {
    const y = (chartHeight / 3) * i;
    g.appendChild(createSVG("line", {
      x1: 0,
      y1: y,
      x2: chartWidth,
      y2: y,
      stroke: "rgba(19, 102, 214, 0.08)",
      "stroke-width": 1,
    }));

    const leftLabel = createSVG("text", {
      x: -10,
      y: y + 4,
      "text-anchor": "end",
      fill: "#1366d6",
      "font-size": "11",
    });
    leftLabel.textContent = formatNumber(Math.round(maxCount - (maxCount / 3) * i));
    g.appendChild(leftLabel);

    const rightLabel = createSVG("text", {
      x: chartWidth + 10,
      y: y + 4,
      "text-anchor": "start",
      fill: "#b42318",
      "font-size": "11",
    });
    rightLabel.textContent = useBytes
      ? formatBigTxnMetric(maxMetric - (maxMetric / 3) * i, "bytes")
      : formatBigTxnMetric(Math.round(maxMetric - (maxMetric / 3) * i), "rows");
    g.appendChild(rightLabel);
  }

  g.appendChild(createSVG("line", {
    x1: 0,
    y1: chartHeight,
    x2: chartWidth,
    y2: chartHeight,
    stroke: "rgba(19, 102, 214, 0.16)",
    "stroke-width": 1,
  }));

  const stepX = buckets.length === 1 ? chartWidth / 2 : chartWidth / Math.max(buckets.length, 1);
  const linePoints = [];

  buckets.forEach((item, index) => {
    const x = buckets.length === 1 ? chartWidth / 2 : stepX * index + stepX * 0.15;
    const barWidth = Math.max(stepX * 0.7, 10);
    const barHeight = Math.max((item.count / maxCount) * (chartHeight - 30), item.count > 0 ? 6 : 0);
    const y = chartHeight - barHeight;
    const rect = createSVG("rect", {
      x,
      y,
      width: barWidth,
      height: barHeight,
      rx: 6,
      fill: "rgba(19, 102, 214, 0.72)",
    });
    rect.addEventListener("mouseenter", () => {
      const metricText = getBigTxnMode() === "bytes"
        ? `最大字节: ${formatBigTxnMetric(item.maxBytes, "bytes")}`
        : `最大行数: ${formatBigTxnMetric(item.maxRows, "rows")}`;
      elements.bigTxnHint.textContent = `${formatBucketRange(item)} | 大事务数量: ${formatNumber(item.count)} | ${metricText}`;
    });
    rect.addEventListener("mouseleave", () => {
      elements.bigTxnHint.textContent = `当前时间段共发现 ${formatNumber(items.length)} 个大事务。上方时间轴展示分布，下方表格展示详细信息。`;
    });
    g.appendChild(rect);

    const lineX = x + barWidth / 2;
    const metricValue = useBytes ? (item.maxBytes || 0) : (item.maxRows || 0);
    const lineY = chartHeight - (metricValue / maxMetric) * (chartHeight - 30);
    linePoints.push(`${lineX},${lineY}`);
    g.appendChild(createSVG("circle", {
      cx: lineX,
      cy: lineY,
      r: 3.5,
      fill: "#b42318",
    }));
  });

  g.appendChild(createSVG("polyline", {
    points: linePoints.join(" "),
    fill: "none",
    stroke: "#b42318",
    "stroke-width": 2.2,
    "stroke-linejoin": "round",
    "stroke-linecap": "round",
  }));

  const tickCount = Math.min(6, buckets.length);
  for (let i = 0; i < tickCount; i += 1) {
    const index = buckets.length === 1 ? 0 : Math.round((buckets.length - 1) * (i / Math.max(tickCount - 1, 1)));
    const x = buckets.length === 1 ? chartWidth / 2 : stepX * index + stepX * 0.5;
    const label = createSVG("text", {
      x,
      y: chartHeight + 22,
      "text-anchor": i === 0 ? "start" : i === tickCount - 1 ? "end" : "middle",
      fill: "#5b6778",
      "font-size": "12",
    });
    label.textContent = bigTxnAxisLabel(buckets[index].start);
    g.appendChild(label);
  }

  const rightTop = createSVG("text", {
    x: chartWidth + 10,
    y: -16,
    fill: "#b42318",
    "font-size": "11",
  });
  rightTop.textContent = useBytes
    ? `最大字节 ${formatBigTxnMetric(maxMetric, "bytes")}`
    : `最大行数 ${formatBigTxnMetric(maxMetric, "rows")}`;
  g.appendChild(rightTop);
  const leftTop = createSVG("text", {
    x: 0,
    y: -16,
    fill: "#1366d6",
    "font-size": "11",
  });
  leftTop.textContent = `大事务数量 ${formatNumber(maxCount)}`;
  g.appendChild(leftTop);

  const legendBlue = createSVG("text", {
    x: 0,
    y: -2,
    fill: "#1366d6",
    "font-size": "11",
    "font-weight": "700",
  });
  legendBlue.textContent = "蓝色柱状 = 大事务数量";
  g.appendChild(legendBlue);

  const legendRed = createSVG("text", {
    x: 174,
    y: -2,
    fill: "#b42318",
    "font-size": "11",
    "font-weight": "700",
  });
  legendRed.textContent = useBytes ? "红色折线 = 最大事务字节" : "红色折线 = 最大事务行数";
  g.appendChild(legendRed);
}

function buildBigTxnBuckets(items) {
  if (!items.length) {
    return [];
  }
  const from = state.bigTxnFrom || new Date(items[0].startTime);
  const to = state.bigTxnTo || new Date(items[items.length - 1].startTime);
  const span = Math.max(to - from, 1);
  const targetBuckets = 18;
  const bucketMs = Math.max(Math.ceil(span / targetBuckets), 60 * 1000);
  const buckets = [];

  for (let cursor = from.getTime(); cursor <= to.getTime(); cursor += bucketMs) {
    buckets.push({
      start: new Date(cursor).toISOString(),
      end: new Date(Math.min(cursor + bucketMs, to.getTime())).toISOString(),
      count: 0,
      maxRows: 0,
      maxBytes: 0,
    });
  }

  items.forEach((item) => {
    const ts = new Date(item.startTime).getTime();
    const idx = Math.min(Math.max(Math.floor((ts - from.getTime()) / bucketMs), 0), buckets.length - 1);
    buckets[idx].count += 1;
    buckets[idx].maxRows = Math.max(buckets[idx].maxRows, item.rowCount || 0);
    buckets[idx].maxBytes = Math.max(buckets[idx].maxBytes, item.transactionLength || 0);
  });
  return buckets;
}

function renderGenericChartPlaceholder(svg, y, message) {
  svg.innerHTML = "";
  const text = createSVG("text", {
    x: 480,
    y,
    "text-anchor": "middle",
    fill: "#5b6778",
    "font-size": "16",
  });
  text.textContent = message;
  svg.appendChild(text);
}

function renderTimelineMeta(buckets, metric) {
  if (!Array.isArray(buckets) || buckets.length === 0) {
    elements.timelineMeta.innerHTML = "";
    return;
  }

  const metricValues = buckets.map((bucket) => metricValue(bucket, metric));
  const peakValue = Math.max(...metricValues, 0);
  const peakIndex = metricValues.findIndex((value) => value === peakValue);
  const totalValue = metricValues.reduce((sum, value) => sum + value, 0);
  const avgValue = buckets.length > 0 ? Math.round(totalValue / buckets.length) : 0;
  const activeBuckets = metricValues.filter((value) => value > 0).length;
  const peakBucket = peakIndex >= 0 ? buckets[peakIndex] : null;

  const cards = [
    { label: "峰值时间桶", value: peakBucket ? shortBucketRange(peakBucket) : "-", className: "compact" },
    { label: "峰值", value: formatMetricAxisValue(peakValue, metric), className: "compact" },
    { label: "活跃桶", value: `${formatNumber(activeBuckets)} / ${formatNumber(buckets.length)}`, className: "compact" },
    { label: "平均值", value: formatMetricAxisValue(avgValue, metric), className: "compact" },
    { label: "大事务数", value: formatNumber(state.currentResult?.summary?.bigTxnCount || 0), className: "compact" },
    { label: "大事务模式", value: getBigTxnMode() === "bytes" ? "事务字节" : "影响行数", className: "compact" },
  ];

  elements.timelineMeta.innerHTML = cards
    .map((card) => `
      <article class="metric-card ${card.className || ""}">
        <span>${escapeHTML(card.label)}</span>
        <strong>${escapeHTML(card.value)}</strong>
      </article>
    `)
    .join("");
}

function updateTimelineFocus() {
  if (!state.currentResult || !state.selectedBucketStart) {
    elements.timelineFocus.textContent = `当前未选中时间桶。点击时间轴中的柱线区域，可直接联动下方 DML 明细。大事务当前按${getBigTxnMode() === "bytes" ? "事务字节" : "影响行数"}判定。`;
    return;
  }
  const bucket = (state.currentResult.buckets || []).find((item) => item.start === state.selectedBucketStart);
  if (!bucket) {
    elements.timelineFocus.textContent = `当前未选中时间桶。点击时间轴中的柱线区域，可直接联动下方 DML 明细。大事务当前按${getBigTxnMode() === "bytes" ? "事务字节" : "影响行数"}判定。`;
    return;
  }
  const bigTxnMetric = getBigTxnMode() === "bytes"
    ? ` | 大事务最大字节 ${formatBigTxnMetric(bucket.maxBigTxnBytes || 0, "bytes")}`
    : ` | 大事务最大行数 ${formatBigTxnMetric(bucket.maxBigTxnRows || 0, "rows")}`;
  elements.timelineFocus.textContent = `当前选中时间桶：${formatBucketRange(bucket)} | DML ${formatNumber(bucket.totalRows || 0)} | INSERT ${formatNumber(bucket.insertRows || 0)} | UPDATE ${formatNumber(bucket.updateRows || 0)} | DELETE ${formatNumber(bucket.deleteRows || 0)} | 记录数 ${formatNumber(bucket.eventCount || 0)}${bigTxnMetric}`;
}

function shortBucketRange(bucket) {
  const start = new Date(bucket.start);
  const end = new Date(bucket.end);
  return `${formatDateLabel(start)} ~ ${formatDateLabel(end)}`;
}

function formatDateLabel(date) {
  if (!date) {
    return "-";
  }
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day} ${hour}:${minute}`;
}

function formatRangeInput(from, to) {
  if (!from || !to) {
    return "";
  }
  return `${formatDateTime(from)} ~ ${formatDateTime(to)}`;
}

function parseRangeInput(value) {
  const text = String(value || "").trim();
  const normalized = text.replace(/[～〜]/g, "~");
  const parts = normalized.split(/\s*~\s*/);
  if (parts.length !== 2) {
    return { error: "时间范围格式错误，请使用：YYYY-MM-DD HH:MM:SS ~ YYYY-MM-DD HH:MM:SS" };
  }
  const from = parseDateTimeText(parts[0]);
  const to = parseDateTimeText(parts[1]);
  if (!from || !to) {
    return { error: "时间范围格式错误，请使用：YYYY-MM-DD HH:MM:SS ~ YYYY-MM-DD HH:MM:SS" };
  }
  return { from, to };
}

function parseDateTimeText(value) {
  const normalized = String(value || "")
    .trim()
    .replaceAll("/", "-")
    .replace("T", " ");
  const match = normalized.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) {
    return null;
  }

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6] || "0");

  if (
    !Number.isInteger(year) ||
    month < 1 || month > 12 ||
    day < 1 || day > 31 ||
    hour < 0 || hour > 23 ||
    minute < 0 || minute > 59 ||
    second < 0 || second > 59
  ) {
    return null;
  }

  const parsed = new Date(year, month - 1, day, hour, minute, second);
  if (
    parsed.getFullYear() !== year ||
    parsed.getMonth() !== month - 1 ||
    parsed.getDate() !== day ||
    parsed.getHours() !== hour ||
    parsed.getMinutes() !== minute ||
    parsed.getSeconds() !== second
  ) {
    return null;
  }
  return parsed;
}

function updateRangeSubtitle() {
  if (!state.displayFrom || !state.displayTo) {
    elements.rangeSubtitle.textContent = "当前范围：未设置";
    return;
  }
  elements.rangeSubtitle.textContent = `当前范围：${formatDateTime(state.displayFrom)} ~ ${formatDateTime(state.displayTo)} | 当前指标：${metricLabel(elements.metricSelect.value)}`;
}

function showChartTooltip(point, metric) {
  const bigTxnMetricLine = getBigTxnMode() === "bytes"
    ? `大事务最大字节：${formatBigTxnMetric(point.bucket.maxBigTxnBytes || 0, "bytes")}`
    : `大事务最大行数：${formatBigTxnMetric(point.bucket.maxBigTxnRows || 0, "rows")}`;
  const timelineMetricLine = getBigTxnMode() === "bytes" && metric === "total"
    ? `当前指标 ${metricLabel(metric)}：${formatBigTxnMetric(point.value || 0, "bytes")}`
    : `当前指标 ${metricLabel(metric)}：${formatNumber(point.value || 0)}`;
  elements.chartTooltip.hidden = false;
  elements.chartTooltip.innerHTML = [
    `时间范围：${formatTooltipRange(point.bucket)}`,
    `DML行数：${formatNumber(point.bucket.totalRows || 0)}`,
    `DML记录数：${formatNumber(point.bucket.eventCount || 0)}`,
    timelineMetricLine,
    bigTxnMetricLine,
  ].join("<br>");
}

function moveChartTooltip(event) {
  const offsetX = 14;
  const offsetY = 14;
  elements.chartTooltip.style.left = `${event.offsetX + offsetX}px`;
  elements.chartTooltip.style.top = `${event.offsetY + offsetY}px`;
}

function hideChartTooltip() {
  elements.chartTooltip.hidden = true;
}

function formatTooltipRange(bucket) {
  const start = new Date(bucket.start);
  const end = new Date(bucket.end);
  const sameDay = start.getFullYear() === end.getFullYear()
    && start.getMonth() === end.getMonth()
    && start.getDate() === end.getDate();
  if (sameDay) {
    return `${formatDateTime(start)}~${String(end.getHours()).padStart(2, "0")}:${String(end.getMinutes()).padStart(2, "0")}`;
  }
  return `${formatDateTime(start)} ~ ${formatDateTime(end)}`;
}

function requestJSON(url, options) {
  return fetch(url, options).then(async (response) => {
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || "请求失败");
    }
    return data;
  });
}

function toLocalInputValue(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");
  const second = String(date.getSeconds()).padStart(2, "0");
  return `${year}-${month}-${day}T${hour}:${minute}:${second}`;
}

function parseLocalDate(value) {
  if (!value) {
    return null;
  }
  return new Date(value);
}

function toApiTime(value) {
  return value ? value.replace("T", " ") : "";
}

function formatDateTime(date) {
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}:${String(date.getSeconds()).padStart(2, "0")}`;
}

function axisLabel(value) {
  const date = new Date(value);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");

  if (!state.currentResult || !state.displayFrom || !state.displayTo) {
    return `${month}-${day} ${hour}:${minute}`;
  }

  const spanMs = state.displayTo.getTime() - state.displayFrom.getTime();
  if (spanMs > 24 * 60 * 60 * 1000) {
    return `${year}-${month}-${day}`;
  }
  return `${month}-${day} ${hour}:${minute}`;
}

function bigTxnAxisLabel(value) {
  const date = new Date(value);
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hour = String(date.getHours()).padStart(2, "0");
  const minute = String(date.getMinutes()).padStart(2, "0");

  if (!state.bigTxnFrom || !state.bigTxnTo) {
    return `${month}-${day} ${hour}:${minute}`;
  }

  const sameDay =
    state.bigTxnFrom.getFullYear() === state.bigTxnTo.getFullYear() &&
    state.bigTxnFrom.getMonth() === state.bigTxnTo.getMonth() &&
    state.bigTxnFrom.getDate() === state.bigTxnTo.getDate();

  if (sameDay) {
    return `${hour}:${minute}`;
  }

  return `${month}-${day} ${hour}:${minute}`;
}

function formatBucketRange(bucket) {
  return `${formatDateTime(new Date(bucket.start))} ~ ${formatDateTime(new Date(bucket.end))}`;
}

function getBigTxnMode() {
  return state.currentResult?.summary?.bigTxnMode || elements.bigTxnMode?.value || "rows";
}

function syncMetricOptions() {
  if (!elements.metricSelect) {
    return;
  }
  const bytesMode = getBigTxnMode() === "bytes";
  Array.from(elements.metricSelect.options).forEach((option) => {
    if (option.value === "total") {
      option.textContent = bytesMode ? "大事务总字节" : "DML 总行数";
      option.disabled = false;
      return;
    }
    option.disabled = bytesMode;
  });
  if (bytesMode && elements.metricSelect.value !== "total") {
    elements.metricSelect.value = "total";
  }
}

function formatMetricAxisValue(value, metric) {
  if (getBigTxnMode() === "bytes" && metric === "total") {
    return formatBigTxnMetric(value || 0, "bytes");
  }
  return formatNumber(Math.round(value || 0));
}

function getSelectedBigTxnMode() {
  return elements.bigTxnMode?.value || "rows";
}

function syncBigTxnModeUI() {
  const mode = getSelectedBigTxnMode();
  const usingBytes = mode === "bytes";
  elements.bigTxnThreshold.disabled = usingBytes;
  elements.bigTxnBytesThreshold.disabled = !usingBytes;
  if (elements.bigTxnThresholdLabel) {
    elements.bigTxnThresholdLabel.textContent = usingBytes ? "-big-txn-threshold (bytes模式下忽略)" : "-big-txn-threshold";
  }
  if (elements.bigTxnBytesThresholdLabel) {
    elements.bigTxnBytesThresholdLabel.textContent = usingBytes ? "-big-txn-bytes-threshold" : "-big-txn-bytes-threshold (rows模式下忽略)";
  }
  elements.bigTxnThreshold.placeholder = usingBytes ? "当前模式忽略该参数" : "";
  elements.bigTxnBytesThreshold.placeholder = usingBytes ? "" : "当前模式忽略该参数";
}

function formatBigTxnThreshold(summary) {
  if (!summary) {
    return "-";
  }
  if ((summary.bigTxnMode || "rows") === "bytes") {
    return formatBigTxnMetric(summary.bigTxnBytesThreshold || 0, "bytes");
  }
  return formatBigTxnMetric(summary.bigTxnThreshold || 0, "rows");
}

function formatBigTxnMetric(value, mode) {
  if ((mode || "rows") === "bytes") {
    const raw = Number(value || 0);
    return `${formatBytes(raw)} (${formatNumber(raw)})`;
  }
  return formatNumber(value || 0);
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString("zh-CN");
}

function formatBytes(bytes) {
  const value = Number(bytes || 0);
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  if (value < 1024 * 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function createSVG(tagName, attrs) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tagName);
  Object.entries(attrs).forEach(([key, value]) => {
    node.setAttribute(key, String(value));
  });
  return node;
}

function setValue(element, value) {
  if (value === undefined || value === null || value === "") {
    return;
  }
  element.value = value;
}

function escapeHTML(text) {
  return String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
