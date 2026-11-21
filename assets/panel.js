/**
 * Bughound's conversation with the board — the whole of it.
 *
 * The words are the board's own:
 *
 *   board → here   { type: "pi-studio.host.hello", nonce, plugin }
 *   here → board   { type: "pi-studio.visual.request", nonce, id, method, params }
 *   board → here   { type: "pi-studio.visual.result", nonce, id, result }
 *   board → here   { type: "pi-studio.visual.error", nonce, id, error: { code, message } }
 *
 * `state` carries worktree lanes, their last 30 minutes of activity, failure dots, and selected-check
 * logs. The panel polls it; `run` queues a run for the selected lane, `toggle_pause` pauses watching,
 * and `log` returns the lane's latest output. The board adds the workspace this tab belongs to, so
 * nothing here names a folder.
 */
let nonce = "";
let nextId = 0;
const pending = new Map();

const dotEl = document.getElementById("dot");
const metaEl = document.getElementById("meta");
const pendingEl = document.getElementById("pending");
const logEl = document.getElementById("log");
const detailHeadingEl = document.getElementById("detailHeading");
const logContextEl = document.getElementById("logContext");
const runAllBtn = document.getElementById("runAll");
const runFailedBtn = document.getElementById("runFailed");
const pauseBtn = document.getElementById("pause");
const copyBtn = document.getElementById("copy");
const setIntervalEl = document.getElementById("setInterval");
const setDebounceEl = document.getElementById("setDebounce");
const setTimeoutEl = document.getElementById("setTimeout");
const setNoteEl = document.getElementById("setNote");
const saveSettingsBtn = document.getElementById("saveSettings");
const watchPythonEl = document.getElementById("watchPython");
const watchJavascriptEl = document.getElementById("watchJavascript");
const fullConfigEl = document.getElementById("fullOnConfigChange");
const watchNoteEl = document.getElementById("watchNote");
const panelMainEl = document.querySelector(".panel-main");
const selectedEl = document.getElementById("selected");
const selectedSplitterEl = document.getElementById("selectedSplitter");
const laneSplitterEl = document.getElementById("laneSplitter");
const laneScrollEl = document.getElementById("laneScroll");
const plotEl = document.getElementById("plot");
const plotTracksEl = document.getElementById("plotTracks");
const timelineArtEl = document.getElementById("timelineArt");
const ticksEl = document.getElementById("ticks");

const NOTHING_ON = "nothing is switched on for this folder";
const MIN_SECONDS = 60;
const MAX_SECONDS = 1800;
const EVENT_COLORS = {
  scan: "#58a6ff",
  settle: "#e3b341",
  run: "#d2a8ff",
  pass: "#7ee08c",
};
const PHASE_LABELS = {
  scan: "Scanning",
  settle: "Edits settling",
  debouncing: "Edits settling",
  running: "Running checks",
  idle: "Idle",
  paused: "Paused",
  off: "Watch off",
  unwatched: "Unwatched",
  no_tests: "No tests",
};

let pollTimer = 0;
let pollBusy = false;
let currentState = null;
let selectedId = "";
let selectedFailure = null;
let visibleSeconds = MIN_SECONDS;
let lastLoadedLogId = null;
const laneButtons = new Map();
let watchSaving = false;

function ask(method, params = {}) {
  const id = (nextId += 1);
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    window.parent.postMessage({ type: "pi-studio.visual.request", nonce, id, method, params }, "*");
  });
}

function lanesFrom(state) {
  return Array.isArray(state?.workspaces) ? state.workspaces : [];
}

function selectedLane(state) {
  if (!selectedId) return null;
  return lanesFrom(state).find((lane) => lane.id === selectedId) || null;
}

function phaseLabel(row) {
  if (!row) return "no workspace";
  return PHASE_LABELS[row.phase] || row.phase || "idle";
}

function timeAgo(atMs, nowMs) {
  const seconds = Math.max(0, Math.round((nowMs - atMs) / 1000));
  return seconds === 0 ? "Now" : `-${seconds}s`;
}

function ageSeconds(atMs, nowMs) {
  return Math.max(0, (nowMs - atMs) / 1000);
}

function describeWatch(watch) {
  const on = [];
  if (watch?.python) on.push("python tests");
  if (watch?.javascript) on.push("javascript tests");
  return on.length ? `watching ${on.join(" and ")}` : "nothing is switched on";
}

function activityMessages(state) {
  const lanes = lanesFrom(state);
  const lines = [];
  if (!lanes.length) {
    return "No Bughound activity yet.\nEnable a test suite in settings to start this feed.";
  }
  for (const row of lanes) {
    const prefix = row.name || row.id;
    const watch = row.watch || { python: false, javascript: false };
    if (watch.python !== true && watch.javascript !== true) {
      lines.push(`${prefix}: nothing is switched on. Select a worktree to set its switches.`);
    }
    for (const path of Array.isArray(row.pending) ? row.pending : []) {
      lines.push(`${prefix}: change detected in ${path}.`);
    }
    if (row.phase === "debouncing") {
      lines.push(`${prefix}: edits are settling; test selection is waiting.`);
    }
    if (row.running) {
      const targets = Array.isArray(row.running.targets) ? row.running.targets : [];
      if (targets.length) {
        for (const target of targets) lines.push(`${prefix}: spawning test ${target}.`);
      } else {
        lines.push(`${prefix}: spawning the full test suite.`);
      }
    }
    if (row.message && row.message !== NOTHING_ON) {
      lines.push(`${prefix}: ${row.message}.`);
    }
    const last = row.lastRun;
    if (last) {
      const targetText = Array.isArray(last.targets) && last.targets.length ? ` (${last.targets.join(", ")})` : "";
      lines.push(`${prefix}: ${last.status}${targetText} · ${last.summary}`);
    }
  }
  if (!lines.length) {
    lines.push(`Bughound is watching ${lanes.length} lane${lanes.length === 1 ? "" : "s"}. Select a lane or a failure dot to inspect its log.`);
  }
  return lines.join("\n");
}

async function loadLog(state, lane) {
  if (!lane) {
    logEl.textContent = activityMessages(state);
    return;
  }
  if (selectedFailure && selectedFailure.laneId === lane.id) return;
  const body = lane.lastRun;
  const logId = body ? `${body.at}-${state?.workspaces?.length ?? 0}` : "none";
  if (logId === lastLoadedLogId) return;
  lastLoadedLogId = logId;
  try {
    const answer = await ask("log", { workspace: lane.id });
    if (selectedFailure && selectedFailure.laneId === lane.id) return;
    logEl.textContent = answer.text || "No check output yet.";
  } catch (err) {
    logEl.textContent = err.message || "the log is not there yet";
  }
}

function buildLaneButtons(lanes) {
  const scrollTop = laneScrollEl.scrollTop;
  const nextIds = new Set(lanes.map((lane) => lane.id));
  for (const [id, button] of laneButtons) {
    if (!nextIds.has(id)) {
      button.remove();
      laneButtons.delete(id);
    }
  }
  laneScrollEl.style.gridTemplateRows = lanes.length ? `repeat(${lanes.length}, var(--lane-height))` : "none";
  lanes.forEach((lane, index) => {
    let button = laneButtons.get(lane.id);
    if (!button) {
      button = document.createElement("button");
      button.type = "button";
      button.className = "track-label";
      const name = document.createElement("span");
      name.className = "lane-name";
      const id = document.createElement("span");
      id.className = "track-id";
      const phase = document.createElement("span");
      phase.className = "lane-phase";
      button.append(name, id, phase);
      const laneId = lane.id;
      button.addEventListener("click", () => {
        selectedId = laneId;
        selectedFailure = null;
        lastLoadedLogId = null;
        render(currentState);
      });
      laneButtons.set(lane.id, button);
    }
    const [name, id, phase] = button.children;
    name.textContent = lane.name || lane.folder || lane.id;
    id.textContent = lane.threadId || lane.branch || lane.id;
    phase.textContent = phaseLabel(lane);
    button.style.gridRow = String(index + 1);
    button.setAttribute("aria-pressed", String(lane.id === selectedId));
    button.setAttribute("aria-label", `${lane.name}, ${phaseLabel(lane)}, latest result: ${lane.lastRun?.status || "no run yet"}`);
    laneScrollEl.insertBefore(button, plotEl);
  });
  laneScrollEl.scrollTop = scrollTop;
}

function buildFailureDots(lanes, nowMs) {
  plotTracksEl.querySelectorAll(".failure-dot").forEach((node) => node.remove());
  lanes.forEach((lane, index) => {
    for (const failure of Array.isArray(lane.failureEvents) ? lane.failureEvents : []) {
      const age = ageSeconds(failure.atMs, nowMs);
      if (age < 0 || age > visibleSeconds) continue;
      const dot = document.createElement("button");
      dot.type = "button";
      dot.className = "failure-dot";
      const position = Math.min(1, Math.max(0, 1 - age / visibleSeconds));
      dot.style.left = `clamp(13px, ${position * 100}%, calc(100% - 13px))`;
      dot.style.top = `${((index + 0.5) / Math.max(1, lanes.length)) * 100}%`;
      const label = `${lane.name}, failure detected at ${timeAgo(failure.atMs, nowMs)}: ${failure.nodeid}`;
      dot.title = label;
      dot.setAttribute("aria-label", label);
      dot.setAttribute("aria-pressed", String(Boolean(selectedFailure && selectedFailure.laneId === lane.id && selectedFailure.atMs === failure.atMs && selectedFailure.nodeid === failure.nodeid)));
      dot.addEventListener("click", () => {
        selectedId = lane.id;
        const log = failure.log || (Array.isArray(lane.failureEvents) ? lane.failureEvents.find((entry) => entry.atMs === failure.atMs && entry.log)?.log : "") || "";
        selectedFailure = { laneId: lane.id, atMs: failure.atMs, nodeid: failure.nodeid, log };
        render(currentState);
      });
      plotTracksEl.append(dot);
    }
  });
}

function windowStartMs(nowMs) {
  return nowMs - visibleSeconds * 1000;
}

function updateTicks(nowMs) {
  if (!ticksEl) return;
  for (let index = 0; index < 4; index += 1) {
    const secondsAgo = Math.round(visibleSeconds * (1 - index / 3));
    if (ticksEl.children[index]) ticksEl.children[index].textContent = secondsAgo === 0 ? "Now" : `-${secondsAgo}s`;
  }
}

function drawTimeline() {
  if (!currentState) return;
  const lanes = lanesFrom(currentState);
  const nowMs = currentState.nowMs || Date.now();
  const startMs = windowStartMs(nowMs);
  const spanMs = Math.max(1, visibleSeconds * 1000);
  updateTicks(nowMs);
  buildFailureDots(lanes, nowMs);
  const canvas = timelineArtEl;
  const context = canvas.getContext("2d");
  if (!context) return;
  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const scale = Math.min(window.devicePixelRatio || 1, 2);
  const width = Math.round(rect.width * scale);
  const height = Math.round(rect.height * scale);
  canvas.width = width;
  canvas.height = height;
  context.clearRect(0, 0, width, height);
  context.lineWidth = scale;

  for (let index = 0; index <= 3; index += 1) {
    const x = Math.min(width - scale, (index / 3) * width + scale / 2);
    context.strokeStyle = index === 3 ? "#596675" : "#30363d";
    context.beginPath();
    context.moveTo(x, 0);
    context.lineTo(x, height);
    context.stroke();
  }

  lanes.forEach((lane, index) => {
    const laneHeight = height / Math.max(1, lanes.length);
    const y = (index + 0.5) * laneHeight;
    context.fillStyle = lane.id === selectedId ? "#142339" : "#161b22";
    context.fillRect(0, y - 13 * scale, width, 26 * scale);
    for (const segment of Array.isArray(lane.timeline) ? lane.timeline : []) {
      const color = EVENT_COLORS[segment.kind];
      if (!color) continue;
      const clampedStart = Math.max(startMs, segment.startMs || 0);
      const clampedEnd = Math.min(nowMs, segment.endMs || 0);
      if (clampedEnd <= clampedStart) continue;
      const x = (clampedStart - startMs) / spanMs * width;
      const segmentWidth = Math.max(1, (clampedEnd - clampedStart) / spanMs * width - scale);
      context.fillStyle = color;
      context.fillRect(x, y - 4 * scale, segmentWidth, 8 * scale);
    }
  });
}

function render(state) {
  currentState = state;
  const lanes = lanesFrom(state);
  const row = selectedLane(state);
  if (row) selectedId = row.id;
  const nowMs = state?.nowMs || Date.now();

  buildLaneButtons(lanes);
  if (selectedFailure && !lanes.some((lane) => lane.id === selectedFailure.laneId)) selectedFailure = null;

  if (!row) {
    dotEl.className = "dot";
    metaEl.textContent = "the daemon knows no workspace for this board";
    pendingEl.hidden = true;
    runAllBtn.disabled = true;
    runFailedBtn.hidden = true;
    pauseBtn.textContent = state.paused ? "resume" : "pause";
    watchPythonEl.disabled = true;
    watchJavascriptEl.disabled = true;
    watchNoteEl.textContent = "select a worktree to set its switches";
    detailHeadingEl.textContent = "Bughound messages";
    logContextEl.textContent = "all lanes";
    logEl.textContent = activityMessages(state);
    if (state?.settings && document.activeElement !== setIntervalEl && document.activeElement !== setDebounceEl && document.activeElement !== setTimeoutEl && document.activeElement !== fullConfigEl) {
      setIntervalEl.value = state.settings.intervalMs;
      setDebounceEl.value = state.settings.debounceMs;
      setTimeoutEl.value = state.settings.timeoutS;
      fullConfigEl.checked = state.settings.fullOnConfigChange === true;
    }
    drawTimeline();
    return;
  }

  const phase = row.phase || "idle";
  const last = row.lastRun;
  dotEl.className = "dot" + (phase === "running" || phase === "debouncing" ? " work" : last?.status === "failed" ? " bad" : last?.status === "passed" ? " ok" : "");
  const watch = row.watch || { python: false, javascript: false };
  if (!watchSaving) {
    watchPythonEl.checked = watch.python === true;
    watchJavascriptEl.checked = watch.javascript === true;
  }
  watchPythonEl.disabled = false;
  watchJavascriptEl.disabled = false;
  watchNoteEl.textContent = describeWatch(watch);
  runAllBtn.disabled = watch.python !== true && watch.javascript !== true;
  const bits = [row.name || row.id];
  if (row.threadId) bits.push(row.threadId);
  if (watch.python !== true && watch.javascript !== true) bits.push("nothing switched on");
  if (row.lastScanAt) bits.push(`scanned ${new Date(row.lastScanAt).toLocaleTimeString()}`);
  if (last) bits.push(`${last.status} · ${last.summary} · ${new Date(last.at).toLocaleTimeString()}`);
  metaEl.textContent = bits.join(" · ");

  const pending = Array.isArray(row.pending) ? row.pending.slice(0, 8) : [];
  pendingEl.hidden = pending.length === 0;
  if (pending.length) pendingEl.textContent = pending.join("\n");

  if (state?.settings && document.activeElement !== setIntervalEl && document.activeElement !== setDebounceEl && document.activeElement !== setTimeoutEl && document.activeElement !== fullConfigEl) {
    setIntervalEl.value = state.settings.intervalMs;
    setDebounceEl.value = state.settings.debounceMs;
    setTimeoutEl.value = state.settings.timeoutS;
    fullConfigEl.checked = state.settings.fullOnConfigChange === true;
  }
  pauseBtn.textContent = state.paused ? "resume" : "pause";
  runFailedBtn.hidden = !last || last.status === "passed" || last.status === "no_tests";

  detailHeadingEl.textContent = "Selected check";
  if (selectedFailure) {
    logContextEl.textContent = `${row.name} · ${selectedFailure.nodeid} · ${timeAgo(selectedFailure.atMs, nowMs)}`;
    logEl.textContent = selectedFailure.log || "No check output yet.";
  } else {
    logContextEl.textContent = `${row.name} · ${row.threadId || "pytest"} · ${last ? new Date(last.at).toLocaleTimeString() : "Latest run"}`;
    void loadLog(state, row);
  }
  drawTimeline();
}

async function poll() {
  if (pollBusy || nonce === "") return;
  pollBusy = true;
  try {
    const state = await ask("state", selectedId ? { workspace: selectedId } : {});
    render(state);
  } catch (err) {
    dotEl.className = "dot bad";
    metaEl.textContent = err.message || "plugin unavailable";
  } finally {
    pollBusy = false;
    schedulePoll();
  }
}

function schedulePoll() {
  clearTimeout(pollTimer);
  const fast = currentState && lanesFrom(currentState).some((one) => one.phase === "running" || one.phase === "debouncing");
  pollTimer = setTimeout(() => void poll(), fast ? 1000 : 2500);
}

async function triggerRun(scope) {
  try {
    const answer = await ask("run", { scope, workspace: selectedId });
    if (answer && answer.queued === false && answer.reason) metaEl.textContent = answer.reason;
    await poll();
  } catch (err) {
    metaEl.textContent = err.message;
  }
}

function clampNumber(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function dragSplitter(handle, start, move) {
  handle.addEventListener("pointerdown", (event) => {
    event.preventDefault();
    try {
      handle.setPointerCapture(event.pointerId);
    } catch (error) {
      // synthetic pointer events have no capture target
    }
    handle.classList.add("dragging");
    const startX = event.clientX;
    const startY = event.clientY;
    const startValue = start();
    const onMove = (moveEvent) => move(startValue, moveEvent.clientX - startX, moveEvent.clientY - startY);
    const onEnd = () => {
      handle.classList.remove("dragging");
      handle.removeEventListener("pointermove", onMove);
      handle.removeEventListener("pointerup", onEnd);
      handle.removeEventListener("pointercancel", onEnd);
    };
    handle.addEventListener("pointermove", onMove);
    handle.addEventListener("pointerup", onEnd);
    handle.addEventListener("pointercancel", onEnd);
  });
}

let laneColumnPx = 180;
dragSplitter(laneSplitterEl, () => laneColumnPx, (start, delta) => {
  laneColumnPx = clampNumber(start + delta, 112, 320);
  document.documentElement.style.setProperty("--lane-column", `${laneColumnPx}px`);
  drawTimeline();
});

let timelineShare = 61.8;
let timelineHeightShare = 55;
dragSplitter(selectedSplitterEl, () => (panelMainEl.clientWidth < 720 ? timelineHeightShare : timelineShare), (start, deltaX, deltaY) => {
  if (panelMainEl.clientWidth < 720) {
    const height = Math.max(1, panelMainEl.clientHeight - 6);
    timelineHeightShare = clampNumber(start + (deltaY / height) * 100, 25, 80);
    document.documentElement.style.setProperty("--timeline-height", `${timelineHeightShare}%`);
  } else {
    const width = Math.max(1, panelMainEl.clientWidth - 6);
    timelineShare = clampNumber(start + (deltaX / width) * 100, 30, 80);
    document.documentElement.style.setProperty("--timeline-share", `${timelineShare}%`);
  }
  drawTimeline();
});

plotEl.addEventListener("wheel", (event) => {
  event.preventDefault();
  const next = Math.min(MAX_SECONDS, Math.max(MIN_SECONDS, Math.round(visibleSeconds * (event.deltaY > 0 ? 2 : 0.5))));
  if (next === visibleSeconds) return;
  visibleSeconds = next;
  drawTimeline();
}, { passive: false });

window.addEventListener("message", (event) => {
  if (event.source !== window.parent) return;
  const data = event.data;
  if (!data || typeof data !== "object") return;
  if (data.type === "pi-studio.host.hello") {
    nonce = typeof data.nonce === "string" ? data.nonce : "";
    pollBusy = false;
    void poll();
    return;
  }
  if (nonce === "" || data.nonce !== nonce) return;
  const settle = pending.get(data.id);
  if (!settle) return;
  pending.delete(data.id);
  if (data.type === "pi-studio.visual.result") settle.resolve(data.result);
  else if (data.type === "pi-studio.visual.error") settle.reject(new Error(data.error?.message || "the call failed"));
});

document.addEventListener("visibilitychange", () => {
  if (!document.hidden && nonce) void poll();
});

runAllBtn.addEventListener("click", () => void triggerRun("all"));
runFailedBtn.addEventListener("click", () => void triggerRun("failed"));
pauseBtn.addEventListener("click", async () => {
  try {
    await ask("toggle_pause");
    await poll();
  } catch (err) {
    metaEl.textContent = err.message;
  }
});
copyBtn.addEventListener("click", () => {
  navigator.clipboard?.writeText(logEl.textContent || "").catch(() => {});
});
saveSettingsBtn.addEventListener("click", async () => {
  const params = {};
  const interval = Number(setIntervalEl.value);
  const debounce = Number(setDebounceEl.value);
  const timeout = Number(setTimeoutEl.value);
  if (Number.isFinite(interval)) params.intervalMs = interval;
  if (Number.isFinite(debounce)) params.debounceMs = debounce;
  if (Number.isFinite(timeout)) params.timeoutS = timeout;
  params.fullOnConfigChange = fullConfigEl.checked;
  try {
    const answer = await ask("set_config", params);
    setNoteEl.textContent = answer.ok ? `saved: ${answer.settings.intervalMs}ms · ${answer.settings.debounceMs}ms · ${answer.settings.timeoutS}s · full on config: ${answer.settings.fullOnConfigChange}` : "not saved";
  } catch (err) {
    setNoteEl.textContent = err.message;
  }
});

async function saveWatch() {
  watchSaving = true;
  try {
    const answer = await ask("set_watch", { python: watchPythonEl.checked, javascript: watchJavascriptEl.checked });
    watchNoteEl.textContent = describeWatch(answer.watch);
  } catch (err) {
    watchNoteEl.textContent = err.message || "not saved";
  } finally {
    watchSaving = false;
  }
  await poll();
}

watchPythonEl.addEventListener("change", () => void saveWatch());
watchJavascriptEl.addEventListener("change", () => void saveWatch());

window.addEventListener("resize", drawTimeline);
