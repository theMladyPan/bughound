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
 * `state` is what the page reads, so the page polls it: every second while a run is in flight, every
 * two and a half otherwise. `run` queues a manual run (`scope` is "all" or "failed"), `toggle_pause`
 * pauses or resumes the watching, and `log` hands back the last output. Esc asks the board to close the
 * drawer, beside its own « button. The board adds the workspace this tab belongs to, so nothing here
 * names a folder.
 */
let nonce = "";
let nextId = 0;
const pending = new Map();

const dotEl = document.getElementById("dot");
const phaseEl = document.getElementById("phase");
const metaEl = document.getElementById("meta");
const pendingEl = document.getElementById("pending");
const logEl = document.getElementById("log");
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
const watchNoteEl = document.getElementById("watchNote");

let pollTimer = 0;
let pollBusy = false;
let currentState = null;
let lastLoadedLogId = null;
// A tick is on its way to the plugin: the next poll must not put the old value back under the hand.
let watchSaving = false;

function closeDrawer() {
  // The board's own « does the same thing by calling it directly; this is how Esc arrives.
  window.parent.postMessage({ type: "pi-studio.visual.close", nonce }, "*");
}

function ask(method, params = {}) {
  const id = (nextId += 1);
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    window.parent.postMessage({ type: "pi-studio.visual.request", nonce, id, method, params }, "*");
  });
}

async function loadLog(state, workspace) {
  const body = workspace?.lastRun;
  const logId = body ? `${body.at}-${state?.history?.length ?? 0}` : "none";
  if (logId === lastLoadedLogId) return;
  lastLoadedLogId = logId;
  try {
    const answer = await ask("log", workspace ? { workspace: workspace.id } : {});
    logEl.textContent = answer.text || "";
  } catch (err) {
    logEl.textContent = err.message || "the log is not there yet";
  }
}

function describeWatch(watch) {
  const on = [];
  if (watch?.python) on.push("python tests");
  if (watch?.javascript) on.push("javascript tests");
  return on.length ? `watching ${on.join(" and ")}` : "nothing is switched on for this folder: no scan, no run, no card";
}

function render(state) {
  currentState = state;
  const workspaces = Array.isArray(state?.workspaces) ? state.workspaces : [];
  const found = workspaces.find((one) => one.phase === "running") || workspaces.find((one) => one.phase === "debouncing") || workspaces[0];
  if (!found) {
    dotEl.className = "dot";
    phaseEl.textContent = "no workspace";
    metaEl.textContent = "the daemon knows no workspace for this board";
    pendingEl.hidden = true;
    runFailedBtn.hidden = true;
    return;
  }
  const runs = workspaces.filter((one) => one.lastRun);
  const last = runs.sort((a, b) => b.lastRun.at - a.lastRun.at)[0]?.lastRun;
  const phase = found.phase || "idle";
  phaseEl.textContent = phase;
  dotEl.className = "dot" + (phase === "running" || phase === "debouncing" ? " work" : last?.status === "failed" ? " bad" : last?.status === "passed" ? " ok" : "");
  const watch = found.watch || { python: false, javascript: false };
  if (!watchSaving) {
    watchPythonEl.checked = watch.python === true;
    watchJavascriptEl.checked = watch.javascript === true;
  }
  watchNoteEl.textContent = describeWatch(watch);
  runAllBtn.disabled = watch.python !== true && watch.javascript !== true;
  const bits = [found.name || found.id];
  if (found.folder) bits.push(found.folder);
  if (watch.python !== true && watch.javascript !== true) bits.push("nothing switched on");
  if (found.lastScanAt) bits.push(`scanned ${new Date(found.lastScanAt).toLocaleTimeString()}`);
  if (last) bits.push(`${last.status} · ${last.summary} · ${new Date(last.at).toLocaleTimeString()}`);
  metaEl.textContent = bits.join(" · ");
  const pending = (found.pending || []).slice(0, 8);
  pendingEl.hidden = pending.length === 0;
  if (pending.length) pendingEl.textContent = pending.join("\n");
  // The settings are the panel's own page of its timings: a poll fills them only while nobody is
  // typing in them, so a number being edited is never overwritten under the caret.
  if (state.settings && document.activeElement !== setIntervalEl && document.activeElement !== setDebounceEl && document.activeElement !== setTimeoutEl) {
    setIntervalEl.value = state.settings.intervalMs;
    setDebounceEl.value = state.settings.debounceMs;
    setTimeoutEl.value = state.settings.timeoutS;
  }
  pauseBtn.textContent = state.paused ? "resume" : "pause";
  runFailedBtn.hidden = !last || last.status === "passed" || last.status === "no_tests";
  void loadLog(state, found);
}

async function poll() {
  if (pollBusy || nonce === "") return;
  pollBusy = true;
  try {
    const state = await ask("state");
    render(state);
  } catch (err) {
    dotEl.className = "dot bad";
    phaseEl.textContent = err.message || "plugin unavailable";
  } finally {
    pollBusy = false;
    schedulePoll();
  }
}

function schedulePoll() {
  clearTimeout(pollTimer);
  const fast = currentState && (currentState.workspaces || []).some((one) => one.phase === "running" || one.phase === "debouncing");
  pollTimer = setTimeout(() => void poll(), fast ? 1000 : 2500);
}

async function triggerRun(scope) {
  try {
    const answer = await ask("run", { scope });
    if (answer && answer.queued === false && answer.reason) metaEl.textContent = answer.reason;
    await poll();
  } catch (err) {
    metaEl.textContent = err.message;
  }
}

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
  try {
    const answer = await ask("set_config", params);
    setNoteEl.textContent = answer.ok ? `saved: ${answer.settings.intervalMs}ms · ${answer.settings.debounceMs}ms · ${answer.settings.timeoutS}s` : "not saved";
  } catch (err) {
    setNoteEl.textContent = err.message;
  }
});

// A tick is saved the moment it is made: the settings page has no save button for the watch, because
// a folder that stops being watched should stop now, and a folder that starts should start now.
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

window.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    event.preventDefault();
    closeDrawer();
  } else if (!event.metaKey && !event.ctrlKey && !event.altKey) {
    if (event.key === "r") { event.preventDefault(); void triggerRun("all"); }
    else if (event.key === "f" && !runFailedBtn.hidden) { event.preventDefault(); void triggerRun("failed"); }
    else if (event.key === "p") { event.preventDefault(); pauseBtn.click(); }
  }
});
