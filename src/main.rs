//! An optional pi-studio plugin: a background worker that watches a workspace folder for file changes
//! and, when a file a test covers changes, immediately runs the targeted pytest for it.
//!
//! The daemon starts this program, speaks to it over its own pipes, and stops it when the daemon goes.
//! The words are one JSON object per line, LF at the end, in both directions: the line protocol every
//! language can write without a library. The daemon calls `initialize` first, and the plugin calls back
//! for what it needs.
//!
//! What it does: it asks the daemon which workspaces it knows (`workspace.list` hands back each one's
//! folder, which is the only way this plugin learns one), polls those folders for changes with pruned
//! recursive metadata snapshots, resolves each changed file to the test files covering it by scanning
//! Python imports, and runs `pytest` on just those. A panel asks it how the watch is going through the
//! daemon's fixed method `visual.request`, which this plugin answers from shared state rather than by
//! doing work: the watcher and the test runner are background threads, so a request is answered in
//! microseconds even while a run is in flight.
//!
//! Configuration arrives as registration `args`, the only way a plugin is told anything: `--interval-ms`
//! (default 2000), `--debounce-ms` (default 500), `--timeout-s` (default 60), `--workspace` (repeatable,
//! an id, a name or a folder; the default watches every non-scratch workspace), `--pytest` (the program
//! to run; otherwise `.venv/bin/pytest`, a venv's `python -m pytest`, or `pytest` on PATH),
//! `--pytest-arg` (repeatable fixed flags), `--node`, `--tsc`, and `--watch` (repeatable; `python` or
//! `javascript`, the side to switch on for the folders this plugin watches).
//!
//! What a folder is watched for is the panel's choice, one pair of switches per folder, both off to
//! begin with: a folder nobody switched anything on for is never walked, spawns nothing, and posts no
//! card. The type check rides with the JavaScript side.
//!
//! The methods a daemon answers are documented at the top of `server/plugins/service.ts`.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

/// What both ends say in `initialize`. A daemon that answers another number is one this plugin cannot
/// speak to.
const PROTOCOL_VERSION: i64 = 1;

/// What this plugin calls itself, in the handshake and in the daemon's log.
const PLUGIN_NAME: &str = "bughound";
const PLUGIN_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The codes a refusal carries, as the daemon's `protocol.ts` spells them.
const PARSE_ERROR: i64 = -32700;
const INVALID_REQUEST: i64 = -32600;
const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;

/// How far the test search walks from a changed file through the import graph before it gives up.
const MAX_HOPS: usize = 16;
/// The most a run's output keeps in memory, first and last together. More than that is truncated.
const OUTPUT_CAP: usize = 64 * 1024;

// ---------- the wire ----------

/// A refusal on the wire: the code a caller can act on, and the reason in words.
#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

impl RpcError {
    fn new(code: i64, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }
}

/// One frame, read as the kind of frame it is. This is the daemon's `parseMessage` seen from the plugin
/// side, down to the wording of its refusals.
#[derive(Debug)]
enum Incoming {
    /// A call of the daemon's, which wants an answer.
    Request {
        id: Value,
        method: String,
        params: Value,
    },
    /// Something the daemon said with no answer wanted: `shutdown`, or an `event`.
    Notification { method: String, params: Value },
    /// The answer to a call this plugin made.
    Response {
        id: Value,
        result: Result<Value, RpcError>,
    },
    /// A frame that could not be read, with the id it named when it named one.
    Invalid { id: Option<Value>, error: RpcError },
}

fn classify(line: &str) -> Incoming {
    let invalid = |id: Option<Value>, message: &str| Incoming::Invalid {
        id,
        error: RpcError::new(INVALID_REQUEST, message),
    };
    let bag: Value = match serde_json::from_str(line) {
        Ok(bag) => bag,
        Err(_) => {
            return Incoming::Invalid {
                id: None,
                error: RpcError::new(PARSE_ERROR, "the frame is not JSON"),
            }
        }
    };
    let Some(frame) = bag.as_object() else {
        return invalid(None, "a frame must be one JSON object");
    };
    if frame.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return invalid(None, "every frame must say \"jsonrpc\": \"2.0\"");
    }
    let id = match frame.get("id") {
        Some(value) if value.is_string() || value.is_number() => Some(value.clone()),
        Some(_) => return invalid(None, "an id must be a string or a number"),
        None => None,
    };
    let params = frame.get("params").cloned().unwrap_or(Value::Null);
    if let Some(method) = frame
        .get("method")
        .and_then(Value::as_str)
        .filter(|method| !method.trim().is_empty())
    {
        return match id {
            Some(id) => Incoming::Request {
                id,
                method: method.to_string(),
                params,
            },
            None => Incoming::Notification {
                method: method.to_string(),
                params,
            },
        };
    }
    let Some(id) = id else {
        return invalid(None, "a frame is a request, a response or a notification");
    };
    if let Some(error) = frame.get("error") {
        return Incoming::Response {
            id,
            result: Err(error_of(error)),
        };
    }
    if let Some(result) = frame.get("result") {
        return Incoming::Response {
            id,
            result: Ok(result.clone()),
        };
    }
    invalid(Some(id), "a response carries a result or an error")
}

/// A refusal as the three things a caller can act on, whatever the daemon actually wrote.
fn error_of(value: &Value) -> RpcError {
    let Some(error) = value.as_object() else {
        return RpcError::new(-32603, "the daemon answered an error that is not an object");
    };
    let code = error.get("code").and_then(Value::as_i64).unwrap_or(-32603);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("the daemon answered an error with no message");
    RpcError::new(code, message)
}

/// What this plugin says and hears. One message out, one line in: the framing belongs to the writing
/// half, so a test drives the plugin without a pipe.
trait Transport {
    fn write(&mut self, message: Value) -> io::Result<()>;
    /// One line from the daemon, or `None` when its input is closed. Unparsed, so this plugin can
    /// answer a line it cannot read the way the protocol says to.
    fn read_line(&mut self) -> io::Result<Option<String>>;
    /// One line if it arrives within `timeout`, `Some(None)` on a closed input, and `None` on a
    /// timeout — the main loop drains cards the runner queued between lines, so it cannot wait
    /// forever for a line that may never come.
    fn read_line_timeout(&mut self, _timeout: Duration) -> io::Result<Option<Option<String>>> {
        Ok(Some(self.read_line()?))
    }
}

/// The real pipes: stdout carries frames and nothing else, so a diagnosis goes to stderr where the
/// daemon keeps it. Stdin is read on its own thread into a channel, because a blocking read on the
/// main thread would hold cards the runner queued until the daemon next spoke.
struct StdioTransport {
    stdin: std::sync::mpsc::Receiver<Option<String>>,
}

impl StdioTransport {
    fn new() -> Self {
        let (sender, stdin) = std::sync::mpsc::channel::<Option<String>>();
        std::thread::spawn(move || {
            let mut input = io::stdin().lock();
            loop {
                let mut line = String::new();
                match input.read_line(&mut line) {
                    Ok(0) => {
                        let _ = sender.send(None);
                        return;
                    }
                    Ok(_) => {
                        let line = line.strip_suffix('\n').unwrap_or(&line);
                        let line = line.strip_suffix('\r').unwrap_or(line);
                        if sender.send(Some(line.to_string())).is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        let _ = sender.send(None);
                        return;
                    }
                }
            }
        });
        Self { stdin }
    }
}

impl Transport for StdioTransport {
    fn write(&mut self, message: Value) -> io::Result<()> {
        let mut stdout = io::stdout().lock();
        let mut line = serde_json::to_string(&message).map_err(io::Error::other)?;
        line.push('\n');
        stdout.write_all(line.as_bytes())?;
        stdout.flush()
    }

    fn read_line(&mut self) -> io::Result<Option<String>> {
        Ok(self.stdin.recv().unwrap_or(None))
    }

    fn read_line_timeout(&mut self, timeout: Duration) -> io::Result<Option<Option<String>>> {
        match self.stdin.recv_timeout(timeout) {
            Ok(line) => Ok(Some(line)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(None),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(Some(None)),
        }
    }
}

// ---------- the plugin ----------

/// This plugin: the pipe it speaks on, the id of the next call it makes, and what it has been told.
/// The watcher and the runner are threads of their own, so a frame the daemon sends is never held up
/// behind a test run: this thread answers `visual.request` from shared state and nothing else.
struct Bughound<T: Transport> {
    transport: T,
    next_id: i64,
    stashed: HashMap<String, Result<Value, RpcError>>,
    settings_path: PathBuf,
    shared: Arc<(Mutex<Shared>, Condvar)>,
    /// Set once the daemon said `shutdown`, or told this plugin something it cannot answer.
    stopping: bool,
}

impl<T: Transport> Bughound<T> {
    fn new(transport: T, config: Config, settings_path: PathBuf) -> Self {
        Self {
            transport,
            next_id: 1,
            stashed: HashMap::new(),
            settings_path,
            shared: Arc::new((Mutex::new(Shared::new(config)), Condvar::new())),
            stopping: false,
        }
    }

    fn run(&mut self) -> io::Result<()> {
        self.handshake()?;
        if self.stopping {
            return Ok(());
        }
        self.learn_workspaces()?;
        self.subscribe()?;
        let coordinator = self.spawn_coordinator();
        let runner = self.spawn_runner();
        while !self.stopping {
            self.drain_notes()?;
            if self.stopping {
                break;
            }
            let Some(maybe_line) = self.transport.read_line_timeout(Duration::from_millis(50))? else {
                continue;
            };
            let Some(line) = maybe_line else {
                break;
            };
            if line.trim().is_empty() {
                continue;
            }
            self.handle(classify(&line))?;
        }
        // The daemon is going away: tell the threads, and give the runner its moment to kill a run.
        stop(&self.shared);
        let _ = coordinator.join();
        let _ = runner.join();
        Ok(())
    }

    /// Wait for the call the daemon makes first and answer it.
    fn handshake(&mut self) -> io::Result<()> {
        while !self.stopping {
            let Some(line) = self.next_line()? else {
                self.stopping = true;
                break;
            };
            let Incoming::Request { id, method, params } = classify(&line) else {
                continue;
            };
            if method != "initialize" {
                let message = format!("the first call must be initialize, not {method}");
                self.refuse(Some(id), RpcError::new(INVALID_REQUEST, &message))?;
                continue;
            }
            self.answer(id, &method, params)?;
            return Ok(());
        }
        Ok(())
    }

    /// Ask for the one event this plugin reads, so a settled run on a board is a reason to rescan it.
    fn subscribe(&mut self) -> io::Result<()> {
        self.call("events.subscribe", json!({ "types": ["thread.settled"] }))?;
        Ok(())
    }

    /// Ask which workspaces the daemon knows, and watch the ones the configuration allows.
    fn learn_workspaces(&mut self) -> io::Result<()> {
        if let Some(list) = self.call("workspace.list", json!({}))? {
            apply_workspace_list(&self.shared, &list);
        }
        Ok(())
    }

    fn spawn_coordinator(&self) -> std::thread::JoinHandle<()> {
        let shared = Arc::clone(&self.shared);
        std::thread::spawn(move || coordinator(shared))
    }

    fn spawn_runner(&self) -> std::thread::JoinHandle<()> {
        let shared = Arc::clone(&self.shared);
        std::thread::spawn(move || runner(shared))
    }

    /// One message that is not the answer a caller is waiting for, or that answer handed back to it.
    fn handle(
        &mut self,
        incoming: Incoming,
    ) -> io::Result<Option<(Value, Result<Value, RpcError>)>> {
        match incoming {
            Incoming::Request { id, method, params } => {
                self.answer(id, &method, params)?;
                Ok(None)
            }
            Incoming::Notification { method, params } => {
                self.absorb(&method, params);
                Ok(None)
            }
            Incoming::Response { id, result } => Ok(Some((id, result))),
            Incoming::Invalid { id, error } => {
                self.refuse(id, error)?;
                Ok(None)
            }
        }
    }

    /// Something the daemon said with no answer wanted.
    fn absorb(&mut self, method: &str, params: Value) {
        match method {
            "shutdown" => {
                self.stopping = true;
                stop(&self.shared);
            }
            "event" => {
                // A run that ended may have edited the folder, so the coordinator rescans at once
                // rather than at its next interval.
                if params.get("type").and_then(Value::as_str) == Some("thread.settled") {
                    wake(&self.shared);
                }
            }
            other => {
                eprintln!("bughound: the daemon said {other}, which this plugin does not read")
            }
        }
    }

    /// Answer one call of the daemon's. `initialize` is the only method this plugin has to answer for
    /// itself; `visual.request` is what its panel asks through the board.
    fn answer(&mut self, id: Value, method: &str, params: Value) -> io::Result<()> {
        match method {
            "initialize" => match initialize_answer(&params) {
                Ok(answer) => self.send(json!({ "jsonrpc": "2.0", "id": id, "result": answer })),
                Err(error) => {
                    self.stopping = true;
                    self.refuse(Some(id), error)
                }
            },
            "visual.request" => match self.handle_visual(&params) {
                Ok(answer) => self.send(json!({ "jsonrpc": "2.0", "id": id, "result": answer })),
                Err(error) => self.refuse(Some(id), error),
            },
            other => {
                let message =
                    format!("{other} is not a method this plugin answers; it answers initialize");
                self.refuse(Some(id), RpcError::new(METHOD_NOT_FOUND, &message))
            }
        }
    }

    /// A call this plugin makes. `None` is an answer the daemon refused, which is said on stderr and
    /// left for the caller to do without. An answer to a call made while this one was waiting is kept
    /// rather than dropped, so a nested call cannot swallow it.
    fn call(&mut self, method: &str, params: Value) -> io::Result<Option<Value>> {
        let id = json!(self.next_id);
        self.next_id += 1;
        self.send(json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))?;
        loop {
            if let Some(result) = self.stashed.remove(&id.to_string()) {
                return match result {
                    Ok(value) => Ok(Some(value)),
                    Err(error) => {
                        eprintln!(
                            "bughound: the daemon refused {method}: {} (code {})",
                            error.message, error.code
                        );
                        Ok(None)
                    }
                };
            }
            let Some(line) = self.next_line()? else {
                eprintln!("bughound: the daemon closed its input while answering {method}");
                self.stopping = true;
                return Ok(None);
            };
            let Some((answer_id, result)) = self.handle(classify(&line))? else {
                continue;
            };
            if answer_id != id {
                self.stashed.insert(answer_id.to_string(), result);
                continue;
            }
            return match result {
                Ok(value) => Ok(Some(value)),
                Err(error) => {
                    eprintln!(
                        "bughound: the daemon refused {method}: {} (code {})",
                        error.message, error.code
                    );
                    Ok(None)
                }
            };
        }
    }

    /// Cards the runner queued while this thread was answering something else, written in order.
    fn drain_notes(&mut self) -> io::Result<()> {
        let pending: Vec<Value> = {
            let (lock, _) = &*self.shared;
            let mut shared = lock.lock().unwrap();
            shared.notes.drain(..).collect()
        };
        for params in pending {
            self.call("cards.note", params)?;
        }
        let removals: Vec<Value> = {
            let (lock, _) = &*self.shared;
            let mut shared = lock.lock().unwrap();
            shared.removals.drain(..).collect()
        };
        for params in removals {
            self.call("cards.remove", params)?;
        }
        Ok(())
    }

    /// What a panel asks, in the plugin's own words, or why the plugin does not answer it.
    fn handle_visual(&mut self, params: &Value) -> Result<Value, RpcError> {
        let method = params.get("method").and_then(Value::as_str).unwrap_or("");
        let inner = params.get("params").cloned().unwrap_or(Value::Null);
        let workspace = params.get("workspace").and_then(Value::as_str).unwrap_or("");
        // A workspace asked for that this plugin does not know may have opened after it started, so
        // ask the daemon again before answering without it.
        if !workspace.is_empty() && !knows(&self.shared, workspace) {
            if let Some(list) = self.call("workspace.list", json!({})).unwrap_or(None) {
                apply_workspace_list(&self.shared, &list);
            }
        }
        match method {
            "state" => Ok(state_snapshot(
                &self.shared,
                workspace,
                inner.get("workspace").and_then(Value::as_str).filter(|lane| !lane.is_empty()),
            )),
            "log" => {
                let workspace = inner
                    .get("workspace")
                    .and_then(Value::as_str)
                    .filter(|lane| !lane.is_empty())
                    .unwrap_or(workspace);
                Ok(json!({ "text": last_output(&self.shared, workspace) }))
            }
            "toggle_pause" => {
                let (lock, _) = &*self.shared;
                let mut shared = lock.lock().unwrap();
                shared.paused = !shared.paused;
                let paused = shared.paused;
                drop(shared);
                wake(&self.shared);
                Ok(json!({ "paused": paused }))
            }
            "run" => {
                let scope = inner.get("scope").and_then(Value::as_str).unwrap_or("all");
                let workspace = inner
                    .get("workspace")
                    .and_then(Value::as_str)
                    .filter(|lane| !lane.is_empty())
                    .unwrap_or(workspace);
                Ok(queue_manual_run(&self.shared, scope, workspace))
            }
            "set_config" => self.set_config(&inner),
            "set_watch" => self.set_watch(&inner, workspace),
            other => Err(RpcError::new(
                METHOD_NOT_FOUND,
                &format!(
                    "{other} is not something this panel asks for; it asks for state, log, toggle_pause, run, set_config and set_watch"
                ),
            )),
        }
    }

    /// The panel's own settings page, the git graph's in miniature: the watch timings it edits are
    /// written to this plugin's own state file, so they survive the daemon and the plugin together.
    /// Registration args still win over the file on the next start.
    fn set_config(&mut self, inner: &Value) -> Result<Value, RpcError> {
        let interval = inner.get("intervalMs").and_then(Value::as_u64);
        let debounce = inner.get("debounceMs").and_then(Value::as_u64);
        let timeout = inner.get("timeoutS").and_then(Value::as_u64);
        let full_on_config_change = inner.get("fullOnConfigChange").and_then(Value::as_bool);
        if interval.is_none() && debounce.is_none() && timeout.is_none() && full_on_config_change.is_none() {
            return Err(RpcError::new(
                INVALID_PARAMS,
                "set_config asks for intervalMs, debounceMs, timeoutS or fullOnConfigChange",
            ));
        }
        let (lock, _) = &*self.shared;
        let mut shared = lock.lock().unwrap();
        if let Some(value) = interval {
            shared.config.interval_ms = value.max(250);
        }
        if let Some(value) = debounce {
            shared.config.debounce_ms = value;
        }
        if let Some(value) = timeout {
            shared.config.timeout_s = value.max(1);
        }
        if let Some(value) = full_on_config_change {
            shared.config.full_on_config_change = value;
        }
        let saved = Settings::from_config(&shared.config);
        if let Err(error) = saved.write(&self.settings_path) {
            eprintln!("bughound: the settings could not be written: {error}");
            return Err(RpcError::new(-32603, "the settings could not be written"));
        }
        drop(shared);
        wake(&self.shared);
        Ok(json!({ "ok": true, "settings": settings_view(&self.shared) }))
    }

    /// What one folder is watched for, as the panel's own switches set it. Switching a side off stops
    /// its scans and runs; switching one on re-reads the folder from scratch, because the files it now
    /// cares about were never in the snapshot.
    fn set_watch(&mut self, inner: &Value, workspace: &str) -> Result<Value, RpcError> {
        let python = inner.get("python").and_then(Value::as_bool);
        let javascript = inner.get("javascript").and_then(Value::as_bool);
        if python.is_none() && javascript.is_none() {
            return Err(RpcError::new(INVALID_PARAMS, "set_watch asks for python, javascript or both"));
        }
        if workspace.is_empty() {
            return Err(RpcError::new(INVALID_PARAMS, "set_watch is asked for one workspace"));
        }
        let watch = {
            let (lock, _) = &*self.shared;
            let mut shared = lock.lock().unwrap();
            let workspace_id = shared
                .workspaces
                .get(workspace)
                .map(|tracker| tracker.workspace_id.clone())
                .filter(|id| shared.workspaces.values().any(|tracker| tracker.workspace_id == *id))
                .unwrap_or_else(|| workspace.to_string());
            if !shared
                .workspaces
                .values()
                .any(|tracker| tracker.workspace_id == workspace_id)
            {
                return Err(RpcError::new(INVALID_PARAMS, "this plugin does not know that workspace"));
            }
            let entry = shared.config.watch.entry(workspace_id.clone()).or_default();
            if let Some(value) = python {
                entry.python = value;
            }
            if let Some(value) = javascript {
                entry.javascript = value;
            }
            let watch = *entry;
            let saved = Settings::from_config(&shared.config);
            if let Err(error) = saved.write(&self.settings_path) {
                eprintln!("bughound: the settings could not be written: {error}");
                return Err(RpcError::new(-32603, "the settings could not be written"));
            }
            for tracker in shared
                .workspaces
                .values_mut()
                .filter(|tracker| tracker.workspace_id == workspace_id)
            {
                // The next scan is a baseline again: a side just switched on has files no scan saw.
                tracker.initialized = false;
                tracker.snapshot.clear();
                tracker.pending.clear();
                tracker.last_change = None;
                tracker.phase = if watch.any() { "idle".to_string() } else { "off".to_string() };
                tracker.message = if watch.any() { String::new() } else { NOTHING_ON.to_string() };
                set_timeline_kind(tracker, "idle", now_ms());
            }
            watch
        };
        wake(&self.shared);
        Ok(json!({ "ok": true, "watch": watch.view(), "settings": settings_view(&self.shared) }))
    }

    fn send(&mut self, message: Value) -> io::Result<()> {
        self.transport.write(message)
    }

    fn refuse(&mut self, id: Option<Value>, error: RpcError) -> io::Result<()> {
        self.send(json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": error.code, "message": error.message },
        }))
    }

    /// One line the plugin has to read, skipping the blank ones a chatty tool leaves behind.
    fn next_line(&mut self) -> io::Result<Option<String>> {
        loop {
            match self.transport.read_line()? {
                None => return Ok(None),
                Some(line) if line.trim().is_empty() => continue,
                Some(line) => return Ok(Some(line)),
            }
        }
    }
}

// ---------- what the plugin reads and writes ----------

/// What this plugin says about itself, or why the daemon at the other end is one it cannot speak to.
fn initialize_answer(params: &Value) -> Result<Value, RpcError> {
    if params.get("protocolVersion").and_then(Value::as_i64) != Some(PROTOCOL_VERSION) {
        return Err(RpcError::new(
            INVALID_PARAMS,
            &format!("this plugin speaks protocol {PROTOCOL_VERSION}"),
        ));
    }
    Ok(json!({
        "protocolVersion": PROTOCOL_VERSION,
        "name": PLUGIN_NAME,
        "version": PLUGIN_VERSION,
    }))
}

#[derive(Clone, Debug)]
struct ParsedWorktree {
    folder: PathBuf,
    branch: Option<String>,
}

/// One worktree belonging to a workspace: the checked-out folder and the display identity that lets
/// the board pair a failure card with the thread that made it.
#[derive(Clone, Debug)]
struct WorktreeInfo {
    folder: PathBuf,
    branch: String,
    thread_id: String,
    is_root: bool,
}

/// Parse `git worktree list --porcelain` output into folders and branches.
fn parse_worktree_list(output: &str) -> Vec<ParsedWorktree> {
    let mut worktrees = Vec::new();
    let mut folder: Option<PathBuf> = None;
    let mut branch: Option<String> = None;
    for line in output.lines() {
        if let Some(path) = line.strip_prefix("worktree ") {
            if let Some(folder) = folder.take() {
                worktrees.push(ParsedWorktree {
                    folder,
                    branch: branch.take(),
                });
            }
            folder = Some(PathBuf::from(path.trim()));
            branch = None;
        } else if let Some(name) = line.strip_prefix("branch ") {
            let name = name.strip_prefix("refs/heads/").unwrap_or(name);
            branch = Some(name.trim().to_string());
        }
    }
    if let Some(folder) = folder {
        worktrees.push(ParsedWorktree { folder, branch });
    }
    worktrees
}

/// Ask Git which worktrees belong to this workspace folder. A folder that is not a Git checkout has
/// no extra lanes: the caller falls back to the root lane alone.
fn git_worktree_infos(workspace_id: &str, folder: &Path, workspace_name: &str) -> Option<Vec<WorktreeInfo>> {
    if folder.as_os_str().is_empty() {
        return None;
    }
    let output = Command::new("git")
        .args(["-C"])
        .arg(folder)
        .arg("worktree")
        .arg("list")
        .arg("--porcelain")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let parsed = parse_worktree_list(&text);
    if parsed.is_empty() {
        return None;
    }
    let canonical_folder = folder.canonicalize().unwrap_or_else(|_| folder.to_path_buf());
    let mut infos = Vec::new();
    let mut saw_root = false;
    for entry in parsed {
        let is_root = entry.folder.canonicalize().unwrap_or_else(|_| entry.folder.clone())
            == canonical_folder;
        if is_root {
            saw_root = true;
        }
        let branch = entry.branch.unwrap_or_default();
        let thread_id = if is_root {
            if branch.is_empty() {
                workspace_id.to_string()
            } else {
                branch.clone()
            }
        } else if let Some(name) = entry.folder.file_name().and_then(|name| name.to_str()) {
            if entry.folder.components().any(|component| component.as_os_str() == ".agents") {
                name.to_string()
            } else if let Some((_, suffix)) = branch.rsplit_once('-') {
                if suffix.len() == 8 && suffix.chars().all(|character| character.is_ascii_hexdigit()) {
                    suffix.to_string()
                } else {
                    name.to_string()
                }
            } else {
                name.to_string()
            }
        } else if branch.is_empty() {
            "worktree".to_string()
        } else {
            branch.clone()
        };
        let _ = workspace_name;
        infos.push(WorktreeInfo {
            folder: if is_root {
                folder.to_path_buf()
            } else {
                entry.folder
            },
            branch,
            thread_id,
            is_root,
        });
    }
    if !saw_root {
        infos.insert(
            0,
            WorktreeInfo {
                folder: folder.to_path_buf(),
                branch: infos.first().map(|info| info.branch.clone()).unwrap_or_default(),
                thread_id: infos.first().map(|info| info.thread_id.clone()).unwrap_or_else(|| workspace_id.to_string()),
                is_root: true,
            },
        );
    }
    Some(infos)
}

fn root_worktree_info(folder: &Path, workspace_id: &str) -> WorktreeInfo {
    WorktreeInfo {
        folder: folder.to_path_buf(),
        branch: String::new(),
        thread_id: workspace_id.to_string(),
        is_root: true,
    }
}

fn worktrees_for_workspace(workspace_id: &str, folder: &Path, workspace_name: &str) -> Vec<WorktreeInfo> {
    git_worktree_infos(workspace_id, folder, workspace_name)
        .unwrap_or_else(|| vec![root_worktree_info(folder, workspace_id)])
}

fn worktree_track_id(workspace_id: &str, info: &WorktreeInfo) -> String {
    if info.is_root {
        workspace_id.to_string()
    } else {
        format!("{}:{}", workspace_id, info.thread_id)
    }
}

fn worktree_card_key(workspace_id: &str, info: &WorktreeInfo) -> String {
    if info.is_root {
        format!("bughound:{workspace_id}")
    } else {
        format!("bughound:{workspace_id}:{}", info.thread_id)
    }
}

fn worktree_display_name(workspace_name: &str, info: &WorktreeInfo) -> String {
    if info.is_root {
        workspace_name.to_string()
    } else if !info.branch.is_empty() {
        info.branch.clone()
    } else {
        info.thread_id.clone()
    }
}

fn insert_workspace_trackers(
    shared: &mut Shared,
    workspace_id: &str,
    workspace_name: &str,
    worktrees: &[WorktreeInfo],
    watched: bool,
) {
    let defaults = shared.config.watch_defaults;
    if defaults.any() {
        shared.config.watch.entry(workspace_id.to_string()).or_insert(defaults);
    }
    for info in worktrees {
        let key = worktree_track_id(workspace_id, info);
        if shared.workspaces.contains_key(&key) {
            continue;
        }
        let card_key = worktree_card_key(workspace_id, info);
        let mut tracker = WorkspaceTracker::new(
            workspace_id,
            &key,
            worktree_display_name(workspace_name, info),
            info.folder.clone(),
            watched,
            info.thread_id.clone(),
            info.branch.clone(),
            info.is_root,
            card_key,
        );
        if !watch_of(shared, &key).any() {
            tracker.phase = "off".to_string();
            tracker.message = NOTHING_ON.to_string();
            tracker.timeline_kind = Some("idle".to_string());
            tracker.timeline_since = now_ms();
        }
        shared.workspaces.insert(key, tracker);
    }
}

/// Refresh the worktree lanes for one workspace from Git. Discovery runs outside the shared lock;
/// only the cheap insert/remove touches it. A failed Git call leaves the old lanes alone.
fn refresh_workspace_worktrees(hub: &Hub, workspace_id: String, folder: PathBuf, workspace_name: String) {
    let Some(infos) = git_worktree_infos(&workspace_id, &folder, &workspace_name) else {
        return;
    };
    let (lock, _) = &**hub;
    let mut shared = lock.lock().unwrap();
    let watched = shared
        .workspaces
        .values()
        .find(|tracker| tracker.workspace_id == workspace_id && tracker.is_root)
        .map(|tracker| tracker.watched)
        .unwrap_or(false);
    let present: BTreeSet<String> = infos
        .iter()
        .map(|info| worktree_track_id(&workspace_id, info))
        .collect();
    // A lane whose worktree vanished takes its failure card off with it: the tracker is gone, so
    // the runner can never remove that card later.
    let stale_removals: Vec<Value> = shared
        .workspaces
        .values()
        .filter(|tracker| {
            tracker.workspace_id == workspace_id
                && !tracker.is_root
                && !present.contains(&tracker.id)
                && tracker.card_posted
        })
        .map(|tracker| {
            json!({
                "workspace": tracker.workspace_id,
                "key": tracker.card_key,
            })
        })
        .collect();
    shared.removals.extend(stale_removals);
    shared.workspaces.retain(|key, tracker| {
        !(tracker.workspace_id == workspace_id && !tracker.is_root && !present.contains(key))
    });
    insert_workspace_trackers(&mut shared, &workspace_id, &workspace_name, &infos, watched);
}

/// Which workspaces this plugin watches, merged from the daemon's own list. Scratch and missing
/// workspaces are always skipped: they are not folders a human edits. A `--workspace` allowlist narrows
/// further, matching ids, names and folders as they are spelled.
fn apply_workspace_list(shared: &Arc<(Mutex<Shared>, Condvar)>, list: &Value) {
    let Some(rows) = list.get("workspaces").and_then(Value::as_array) else {
        return;
    };
    let allow = {
        let (lock, _) = &**shared;
        lock.lock().unwrap().config.workspaces.clone()
    };
    let mut additions: Vec<(String, String, Vec<WorktreeInfo>, bool)> = Vec::new();
    for row in rows {
        let Some(id) = row.get("id").and_then(Value::as_str) else {
            continue;
        };
        let name = row
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or(id)
            .to_string();
        let folder = row
            .get("folder")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let scratch = row.get("scratch").and_then(Value::as_bool) == Some(true);
        let missing = row.get("missing").and_then(Value::as_bool) == Some(true);
        let allowed = allow.is_empty()
            || allow
                .iter()
                .any(|one| one == id || one == &name || one == &folder);
        let watched = !scratch && !missing && !folder.is_empty() && allowed;
        let worktrees = if watched {
            worktrees_for_workspace(id, Path::new(&folder), &name)
        } else {
            vec![root_worktree_info(Path::new(&folder), id)]
        };
        additions.push((id.to_string(), name, worktrees, watched));
    }
    let (lock, _) = &**shared;
    let mut shared = lock.lock().unwrap();
    for (id, name, worktrees, watched) in additions {
        let already = shared
            .workspaces
            .values()
            .any(|tracker| tracker.workspace_id == id && tracker.is_root);
        if already {
            continue;
        }
        // A `--watch` arg is what the registration asked for, so it seeds every folder the panel has not
        // spoken for. Without one, a folder is watched for nothing until a switch is ticked.
        insert_workspace_trackers(&mut shared, &id, &name, &worktrees, watched);
    }
}

fn knows(shared: &Arc<(Mutex<Shared>, Condvar)>, workspace: &str) -> bool {
    let (lock, _) = &**shared;
    let shared = lock.lock().unwrap();
    shared.workspaces.contains_key(workspace)
        || shared
            .workspaces
            .values()
            .any(|tracker| tracker.workspace_id == workspace)
}

/// What one folder is watched for. A folder the panel has not spoken for is watched for nothing.
fn watch_of(shared: &Shared, workspace: &str) -> Watch {
    let workspace_id = shared
        .workspaces
        .get(workspace)
        .map(|tracker| tracker.workspace_id.as_str())
        .unwrap_or_else(|| workspace.split(':').next().unwrap_or(workspace));
    shared.config.watch.get(workspace_id).copied().unwrap_or_default()
}

// ---------- configuration ----------

#[derive(Clone, Debug, Default)]
struct Config {
    interval_ms: u64,
    debounce_ms: u64,
    timeout_s: u64,
    full_on_config_change: bool,
    workspaces: Vec<String>,
    pytest: Option<String>,
    pytest_args: Vec<String>,
    node: Option<String>,
    tsc: Option<String>,
    /// What each folder is watched for, by workspace id. A folder that is not in here is watched for
    /// nothing at all: no scan, no run, no card. The type check rides with the JavaScript side.
    watch: BTreeMap<String, Watch>,
    /// The sides a `--watch` arg named: those are switched on for every folder this plugin watches,
    /// wherever the panel has not said otherwise. Without an arg, and without the panel, nothing is.
    watch_defaults: Watch,
}

/// What a folder's phase and its panel line say while nothing is switched on for it.
const NOTHING_ON: &str = "nothing is switched on for this folder";

/// The two things bughound can do for a folder, both off until the panel switches one on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Watch {
    python: bool,
    javascript: bool,
}

impl Watch {
    fn any(&self) -> bool {
        self.python || self.javascript
    }

    /// This watch with one more side switched on, named the way a `--watch` arg names it.
    fn plus(self, side: &str, flag: &str) -> Result<Watch, String> {
        match side {
            "python" => Ok(Watch {
                python: true,
                ..self
            }),
            "javascript" | "node" => Ok(Watch {
                javascript: true,
                ..self
            }),
            other => Err(format!("{flag} names python or javascript, not {other}")),
        }
    }

    /// The files this setting makes worth watching: the sources of a switched-on side, and the
    /// configuration files its tools read, whose change is a whole-suite run.
    fn wants(&self, name: &str) -> bool {
        (self.python && (name.ends_with(".py") || matches!(name, "pyproject.toml" | "pytest.ini" | "tox.ini" | "setup.cfg")))
            || (self.javascript && (has_jsts_extension(name) || matches!(name, "tsconfig.json" | "package.json")))
    }

    fn read(value: &Value) -> Watch {
        Watch {
            python: value.get("python").and_then(Value::as_bool).unwrap_or(false),
            javascript: value.get("javascript").and_then(Value::as_bool).unwrap_or(false),
        }
    }

    fn view(&self) -> Value {
        json!({ "python": self.python, "javascript": self.javascript })
    }
}

impl Config {
    fn with_defaults() -> Self {
        Self {
            interval_ms: 2000,
            debounce_ms: 500,
            timeout_s: 60,
            ..Config::default()
        }
    }
}

/// What the registration said, before what the panel saved adds to it: the three timings as options,
/// so a file the panel wrote and an arg the registration gave can be told apart.
#[derive(Clone, Debug, Default)]
struct Given {
    interval_ms: Option<u64>,
    debounce_ms: Option<u64>,
    timeout_s: Option<u64>,
    workspaces: Vec<String>,
    pytest: Option<String>,
    pytest_args: Vec<String>,
    node: Option<String>,
    tsc: Option<String>,
    settings_path: Option<String>,
    watch_defaults: Watch,
}

/// The timings the panel's settings page last wrote, and what it switched the watch on for, if it
/// wrote any.
#[derive(Clone, Debug, Default)]
struct Settings {
    interval_ms: Option<u64>,
    debounce_ms: Option<u64>,
    timeout_s: Option<u64>,
    full_on_config_change: Option<bool>,
    watch: BTreeMap<String, Watch>,
}

impl Settings {
    fn read(path: &Path) -> Settings {
        let Ok(text) = std::fs::read_to_string(path) else {
            return Settings::default();
        };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            return Settings::default();
        };
        Settings {
            interval_ms: value.get("intervalMs").and_then(Value::as_u64),
            debounce_ms: value.get("debounceMs").and_then(Value::as_u64),
            timeout_s: value.get("timeoutS").and_then(Value::as_u64),
            full_on_config_change: value.get("fullOnConfigChange").and_then(Value::as_bool),
            watch: value
                .get("watch")
                .and_then(Value::as_object)
                .map(|rows| rows.iter().map(|(id, one)| (id.clone(), Watch::read(one))).collect())
                .unwrap_or_default(),
        }
    }

    fn write(&self, path: &Path) -> Result<(), io::Error> {
        if let Some(folder) = path.parent() {
            std::fs::create_dir_all(folder)?;
        }
        let watch: serde_json::Map<String, Value> = self
            .watch
            .iter()
            .map(|(id, one)| (id.clone(), one.view()))
            .collect();
        let text = json!({
            "intervalMs": self.interval_ms,
            "debounceMs": self.debounce_ms,
            "timeoutS": self.timeout_s,
            "fullOnConfigChange": self.full_on_config_change.unwrap_or(false),
            "watch": watch,
        });
        std::fs::write(path, format!("{}\n", text))
    }

    /// The settings as they stand, so one write never loses what the other half of them said.
    fn from_config(config: &Config) -> Settings {
        Settings {
            interval_ms: Some(config.interval_ms),
            debounce_ms: Some(config.debounce_ms),
            timeout_s: Some(config.timeout_s),
            full_on_config_change: Some(config.full_on_config_change),
            watch: config.watch.clone(),
        }
    }
}

/// Where the settings file lives: the daemon's own state folder, unless `--settings` said elsewhere.
fn default_settings_path() -> PathBuf {
    if let Some(folder) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(folder).join("pi-studio").join("bughound.json");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".local").join("state").join("pi-studio").join("bughound.json");
    }
    std::env::temp_dir().join("bughound.json")
}

/// Registration `args` as configuration, or why one of them cannot be used. An unknown flag is an
/// error rather than a guess: the daemon reports it beside the plugins that did start.
#[allow(dead_code)]
fn parse_args(args: &[String]) -> Result<Config, String> {
    resolve(parse_given(args)?, None)
}

fn parse_given(args: &[String]) -> Result<Given, String> {
    let mut given = Given::default();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let (flag, inline) = match arg.split_once('=') {
            Some((flag, value)) => (flag, Some(value.to_string())),
            None => (arg.as_str(), None),
        };
        let value = |i: &mut usize| -> Result<String, String> {
            if let Some(value) = &inline {
                return Ok(value.clone());
            }
            *i += 1;
            args.get(*i).cloned().ok_or_else(|| format!("{flag} needs a value"))
        };
        match flag {
            "--interval-ms" => {
                given.interval_ms = Some(parse_u64(&value(&mut i)?, flag)?);
            }
            "--debounce-ms" => {
                given.debounce_ms = Some(parse_u64(&value(&mut i)?, flag)?);
            }
            "--timeout-s" => {
                given.timeout_s = Some(parse_u64(&value(&mut i)?, flag)?);
            }
            "--workspace" => {
                given.workspaces.push(value(&mut i)?);
            }
            "--pytest" => {
                given.pytest = Some(value(&mut i)?);
            }
            "--pytest-arg" => {
                given.pytest_args.push(value(&mut i)?);
            }
            "--settings" => {
                given.settings_path = Some(value(&mut i)?);
            }
            "--node" => {
                given.node = Some(value(&mut i)?);
            }
            "--tsc" => {
                given.tsc = Some(value(&mut i)?);
            }
            "--watch" => {
                given.watch_defaults = given.watch_defaults.plus(&value(&mut i)?, flag)?;
            }
            _ => return Err(format!("unknown argument {flag}")),
        }
        i += 1;
    }
    Ok(given)
}

/// Defaults, then what the panel saved, then what the registration said: the last word wins.
fn resolve(given: Given, saved: Option<Settings>) -> Result<Config, String> {
    let mut config = Config::with_defaults();
    if let Some(saved) = saved {
        if let Some(value) = saved.interval_ms {
            config.interval_ms = value;
        }
        if let Some(value) = saved.debounce_ms {
            config.debounce_ms = value;
        }
        if let Some(value) = saved.timeout_s {
            config.timeout_s = value;
        }
        if let Some(value) = saved.full_on_config_change {
            config.full_on_config_change = value;
        }
        config.watch = saved.watch;
    }
    if let Some(value) = given.interval_ms {
        config.interval_ms = value;
    }
    if let Some(value) = given.debounce_ms {
        config.debounce_ms = value;
    }
    if let Some(value) = given.timeout_s {
        config.timeout_s = value;
    }
    config.workspaces = given.workspaces;
    config.pytest = given.pytest;
    config.pytest_args = given.pytest_args;
    config.node = given.node;
    config.tsc = given.tsc;
    config.watch_defaults = given.watch_defaults;
    if config.interval_ms < 250 {
        config.interval_ms = 250;
    }
    Ok(config)
}

fn parse_u64(text: &str, flag: &str) -> Result<u64, String> {
    text.parse::<u64>()
        .map_err(|_| format!("{flag} is not a number"))
}

// ---------- the shared watch state ----------

#[derive(Clone, Debug)]
struct Failure {
    nodeid: String,
    summary: String,
}

const TIMELINE_RETENTION_MS: u64 = 30 * 60 * 1000;

#[derive(Clone, Debug)]
struct TimelineInterval {
    start_ms: u64,
    end_ms: u64,
    kind: String,
}

#[derive(Clone, Debug)]
struct TimelineFailure {
    at_ms: u64,
    nodeid: String,
    summary: String,
    log: Arc<String>,
}

#[derive(Clone, Debug)]
struct LastRun {
    at: u64,
    status: String,
    summary: String,
    duration_ms: u64,
    targets: Vec<String>,
    failures: Vec<Failure>,
}

struct RunningView {
    targets: Vec<String>,
    started_at: u64,
}

/// One workspace under watch: its folder, its snapshot, and what the last change and the last run said.
struct WorkspaceTracker {
    id: String,
    workspace_id: String,
    card_key: String,
    thread_id: String,
    branch: String,
    is_root: bool,
    name: String,
    folder: PathBuf,
    watched: bool,
    initialized: bool,
    card_posted: bool,
    snapshot: HashMap<PathBuf, (u64, u64, u64)>,
    pending: BTreeSet<PathBuf>,
    last_change: Option<Instant>,
    last_change_at: u64,
    phase: String,
    message: String,
    running: Option<RunningView>,
    last_run: Option<LastRun>,
    last_scan_at: Option<u64>,
    last_output: String,
    timeline: Vec<TimelineInterval>,
    timeline_kind: Option<String>,
    timeline_since: u64,
    failure_events: Vec<TimelineFailure>,
}

impl WorkspaceTracker {
    fn new(
        workspace_id: &str,
        id: &str,
        name: String,
        folder: PathBuf,
        watched: bool,
        thread_id: String,
        branch: String,
        is_root: bool,
        card_key: String,
    ) -> Self {
        Self {
            id: id.to_string(),
            workspace_id: workspace_id.to_string(),
            card_key,
            thread_id,
            branch,
            is_root,
            name,
            folder,
            watched,
            initialized: false,
            card_posted: false,
            snapshot: HashMap::new(),
            pending: BTreeSet::new(),
            last_change: None,
            last_change_at: 0,
            phase: if watched { "idle".to_string() } else { "unwatched".to_string() },
            message: String::new(),
            running: None,
            last_run: None,
            last_scan_at: None,
            last_output: String::new(),
            timeline: Vec::new(),
            timeline_kind: Some("idle".to_string()),
            timeline_since: now_ms(),
            failure_events: Vec::new(),
        }
    }
}

/// A run the coordinator or the panel asked for: specific test targets, or the whole suite.
/// The JavaScript side rides beside the Python one, so one job may run both suites.
struct Job {
    workspace: String,
    targets: Vec<PathBuf>,
    full: bool,
    js_targets: Vec<PathBuf>,
    js_full: bool,
    js_typecheck: bool,
    reason: String,
}

struct Shared {
    config: Config,
    workspaces: BTreeMap<String, WorkspaceTracker>,
    paused: bool,
    stopping: bool,
    jobs: VecDeque<Job>,
    notes: VecDeque<Value>,
    removals: VecDeque<Value>,
    last_worktree_refresh: u64,
}

impl Shared {
    fn new(config: Config) -> Self {
        Self {
            config,
            workspaces: BTreeMap::new(),
            paused: false,
            stopping: false,
            jobs: VecDeque::new(),
            notes: VecDeque::new(),
            removals: VecDeque::new(),
            last_worktree_refresh: 0,
        }
    }
}

type Hub = Arc<(Mutex<Shared>, Condvar)>;

fn wake(hub: &Hub) {
    hub.1.notify_all();
}

fn stop(hub: &Hub) {
    let (lock, _) = &**hub;
    lock.lock().unwrap().stopping = true;
    wake(hub);
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

fn prune_timeline(tracker: &mut WorkspaceTracker, now: u64) {
    let cutoff = now.saturating_sub(TIMELINE_RETENTION_MS);
    tracker.timeline.retain(|interval| interval.end_ms >= cutoff);
    tracker.failure_events.retain(|failure| failure.at_ms >= cutoff);
}

fn record_timeline_interval(tracker: &mut WorkspaceTracker, start_ms: u64, end_ms: u64, kind: &str) {
    if end_ms <= start_ms {
        return;
    }
    tracker.timeline.push(TimelineInterval {
        start_ms,
        end_ms,
        kind: kind.to_string(),
    });
    prune_timeline(tracker, end_ms);
}

fn set_timeline_kind(tracker: &mut WorkspaceTracker, kind: &str, now: u64) {
    if kind == "off" {
        tracker.timeline_kind = None;
        return;
    }
    if tracker.timeline_kind.as_deref() == Some(kind) {
        if tracker.timeline_since == 0 {
            tracker.timeline_since = now;
        }
        return;
    }
    if let Some(previous) = tracker.timeline_kind.take() {
        record_timeline_interval(tracker, tracker.timeline_since, now, &previous);
    }
    tracker.timeline_kind = Some(kind.to_string());
    tracker.timeline_since = now;
}

fn timeline_events_json(tracker: &WorkspaceTracker, now: u64) -> Vec<Value> {
    let cutoff = now.saturating_sub(TIMELINE_RETENTION_MS);
    let mut events: Vec<Value> = tracker
        .timeline
        .iter()
        .filter(|interval| interval.end_ms >= cutoff)
        .map(|interval| {
            json!({
                "startMs": interval.start_ms,
                "endMs": interval.end_ms,
                "kind": interval.kind,
            })
        })
        .collect();
    if let Some(kind) = tracker.timeline_kind.as_deref() {
        if kind != "off" && tracker.timeline_since < now {
            events.push(json!({
                "startMs": tracker.timeline_since,
                "endMs": now,
                "kind": kind,
            }));
        }
    }
    events
}

fn timeline_failures_json(tracker: &WorkspaceTracker, now: u64, include_logs: bool) -> Vec<Value> {
    let cutoff = now.saturating_sub(TIMELINE_RETENTION_MS);
    // Every failure of one run shares one log. Only the first timeline dot that run carries it in
    // full, so a state poll is not forced to resend the same output for each node id.
    let mut logged_runs: HashSet<u64> = HashSet::new();
    tracker
        .failure_events
        .iter()
        .filter(|failure| failure.at_ms >= cutoff)
        .map(|failure| {
            let log = if include_logs && logged_runs.insert(failure.at_ms) {
                failure.log.as_ref().clone()
            } else {
                String::new()
            };
            json!({
                "atMs": failure.at_ms,
                "nodeid": failure.nodeid,
                "summary": failure.summary,
                "log": log,
            })
        })
        .collect()
}

/// One manual or watched run, queued if its workspace has no run waiting already.
fn push_job(hub: &Hub, job: Job) {
    {
        let (lock, _) = &**hub;
        let mut shared = lock.lock().unwrap();
        if let Some(waiting) = shared.jobs.iter_mut().find(|waiting| waiting.workspace == job.workspace) {
            merge_into(waiting, job);
        } else {
            shared.jobs.push_back(job);
        }
    }
    wake(hub);
}

/// One job absorbed into another: the whole side that asked for everything narrows the waiting job
/// to that side, and targets only accumulate while their side has no full suite waiting.
fn merge_into(waiting: &mut Job, job: Job) {
    if job.full {
        waiting.full = true;
        waiting.targets.clear();
    } else if !waiting.full {
        for target in job.targets {
            if !waiting.targets.contains(&target) {
                waiting.targets.push(target);
            }
        }
    }
    if job.js_full {
        waiting.js_full = true;
        waiting.js_targets.clear();
    } else if !waiting.js_full {
        for target in job.js_targets {
            if !waiting.js_targets.contains(&target) {
                waiting.js_targets.push(target);
            }
        }
    }
    waiting.js_typecheck = waiting.js_typecheck || job.js_typecheck;
    waiting.reason = job.reason;
}

// ---------- the coordinator: scan, debounce, resolve, dispatch ----------

fn coordinator(hub: Hub) {
    loop {
        // Wait for the next scan, or for anything that should shorten it.
        {
            let (lock, cvar) = &*hub;
            let guard = lock.lock().unwrap();
            let interval = Duration::from_millis(guard.config.interval_ms);
            let (shared, _) = cvar.wait_timeout(guard, interval).unwrap();
            if shared.stopping {
                return;
            }
        }
        let roots: Vec<(String, PathBuf, String)> = {
            let (lock, _) = &*hub;
            let mut shared = lock.lock().unwrap();
            let should_refresh = now_ms().saturating_sub(shared.last_worktree_refresh) > 5_000;
            if should_refresh {
                shared.last_worktree_refresh = now_ms();
                shared
                    .workspaces
                    .values()
                    .filter(|tracker| tracker.is_root)
                    .map(|tracker| {
                        (
                            tracker.workspace_id.clone(),
                            tracker.folder.clone(),
                            tracker.name.clone(),
                        )
                    })
                    .collect()
            } else {
                Vec::new()
            }
        };
        for (workspace_id, folder, workspace_name) in roots {
            refresh_workspace_worktrees(&hub, workspace_id, folder, workspace_name);
        }
        // Snapshot the folders without the lock in hand: a walk is the slow part. Only a folder the
        // panel switched something on for is walked; the rest cost one check per scan.
        let watched: Vec<(String, PathBuf, Watch)> = {
            let (lock, _) = &*hub;
            let shared = lock.lock().unwrap();
            shared
                .workspaces
                .values()
                .filter(|one| one.watched)
                .map(|one| (one.id.clone(), one.folder.clone(), watch_of(&shared, &one.id)))
                .collect()
        };
        for (id, folder, watch) in watched {
            if !watch.any() {
                let (lock, _) = &*hub;
                let mut shared = lock.lock().unwrap();
                if let Some(tracker) = shared.workspaces.get_mut(&id) {
                    if tracker.phase != "off" {
                        tracker.phase = "off".to_string();
                        tracker.message = NOTHING_ON.to_string();
                        tracker.pending.clear();
                    }
                }
                continue;
            }
            let scan_started_at = now_ms();
            let snapshot = scan(&folder, watch);
            let pending: Vec<PathBuf> = {
                let (lock, _) = &*hub;
                let mut shared = lock.lock().unwrap();
                if shared.stopping {
                    return;
                }
                let Some(tracker) = shared.workspaces.get_mut(&id) else {
                    continue;
                };
                tracker.last_scan_at = Some(now_ms());
                if !tracker.initialized {
                    // The baseline: the first scan says what is there, and runs nothing for it.
                    tracker.snapshot = snapshot;
                    tracker.initialized = true;
                    continue;
                }
                let changed = diff(&tracker.snapshot, &snapshot);
                tracker.snapshot = snapshot;
                if !changed.is_empty() {
                    tracker.pending.extend(changed);
                    tracker.last_change = Some(Instant::now());
                    tracker.last_change_at = now_ms();
                    if tracker.running.is_none() {
                        tracker.phase = "debouncing".to_string();
                        set_timeline_kind(tracker, "scan", scan_started_at);
                        set_timeline_kind(tracker, "settle", now_ms());
                    }
                }
                tracker.pending.iter().cloned().collect()
            };
            if pending.is_empty() {
                continue;
            }
            // A pause holds the run, not the watch: the pending set stays for the resume.
            let (debounced, paused, busy, full_on_config_change) = {
                let (lock, _) = &*hub;
                let shared = lock.lock().unwrap();
                let tracker = &shared.workspaces[&id];
                let debounced = tracker
                    .last_change
                    .map(|at| at.elapsed() >= Duration::from_millis(shared.config.debounce_ms))
                    .unwrap_or(true);
                (
                    debounced,
                    shared.paused,
                    tracker.running.is_some(),
                    shared.config.full_on_config_change,
                )
            };
            if !debounced || paused || busy {
                continue;
            }
            // Resolve without the lock: reading every file is the slow part. The Python and the
            // JavaScript sides resolve independently, and only the side the panel switched on is read.
            let resolution = if watch.python {
                resolve_targets(&folder, &pending.iter().cloned().collect(), full_on_config_change)
            } else {
                Resolution::NoTests
            };
            let js_resolution = if watch.javascript {
                resolve_jsts(&folder, &pending.iter().cloned().collect(), full_on_config_change)
            } else {
                Resolution::NoTests
            };
            let no_tests = matches!(resolution, Resolution::NoTests) && matches!(js_resolution, Resolution::NoTests);
            let dispatched = {
                let (lock, _) = &*hub;
                let mut shared = lock.lock().unwrap();
                // Read before the tracker borrows the shared map: a type check runs with every
                // affected-test run, because a type error is the break the user meets first.
                let has_tsc = choose_tsc(&folder, &shared.config).is_some();
                let Some(tracker) = shared.workspaces.get_mut(&id) else {
                    continue;
                };
                // Only the files that were resolved leave the pending set: an edit made during the
                // resolution is still pending and earns its own run.
                for file in &pending {
                    tracker.pending.remove(file);
                }
                if tracker.pending.is_empty() {
                    tracker.last_change = None;
                    if tracker.running.is_none() {
                        tracker.phase = "idle".to_string();
                    }
                }
                let mut job = Job {
                    workspace: id.clone(),
                    targets: vec![],
                    full: false,
                    js_targets: vec![],
                    js_full: false,
                    js_typecheck: false,
                    reason: String::new(),
                };
                match resolution {
                    Resolution::Full => {
                        tracker.message = "suite configuration changed: running everything".to_string();
                        job.full = true;
                        job.reason = "config changed".to_string();
                    }
                    Resolution::Targets(targets) => {
                        tracker.message = format!("{} test file(s) cover the change", targets.len());
                        job.targets = targets;
                        job.reason = "files changed".to_string();
                    }
                    Resolution::NoTests => {}
                }
                // A type check runs with every affected-test run, because a type error is the break
                // the user meets first; a config change turns it into a whole-suite run with the tests.
                match js_resolution {
                    Resolution::Full => {
                        job.js_full = true;
                        job.js_typecheck = has_tsc;
                        job.reason = "config changed".to_string();
                    }
                    Resolution::Targets(targets) => {
                        job.js_targets = targets;
                        job.js_typecheck = has_tsc;
                        if job.reason.is_empty() {
                            job.reason = "files changed".to_string();
                        }
                    }
                    Resolution::NoTests => {}
                }
                if !no_tests {
                    push_job_locked(&mut shared, id.clone(), job);
                } else {
                    tracker.phase = "no_tests".to_string();
                    set_timeline_kind(tracker, "idle", now_ms());
                    tracker.message = format!("no tests cover {}", relative_names(&pending));
                    eprintln!("bughound: {id}: no tests cover {}", relative_names(&pending));
                }
                !no_tests
            };
            if dispatched {
                wake(&hub);
            }
        }
    }
}

/// A resolved job, pushed while the caller already holds the lock.
fn push_job_locked(shared: &mut Shared, workspace: String, job: Job) {
    if let Some(waiting) = shared.jobs.iter_mut().find(|waiting| waiting.workspace == workspace) {
        merge_into(waiting, job);
    } else {
        shared.jobs.push_back(job);
    }
}

fn relative_names(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .take(4)
        .map(|path| path.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(", ")
}

// ---------- scanning ----------

/// Directories the walk never enters: version control, pi-studio's own worktrees beside the folder,
/// virtual environments, caches and build output. Entering any of them is how a watcher spends its
/// visit on files nobody edits.
const PRUNED: &[&str] = &[
    ".git", ".hg", ".svn", ".agents", ".pi-studio", ".venv", "venv", "env", ".tox", ".nox", ".mypy_cache",
    ".pytest_cache", ".ruff_cache", "__pycache__", "node_modules", "target", "dist", "build", ".eggs",
];

/// A snapshot of one folder: every file the panel switched the watch on for, as (mtime seconds, mtime
/// nanos, length). A folder with nothing switched on is not walked at all.
fn scan(folder: &Path, watch: Watch) -> HashMap<PathBuf, (u64, u64, u64)> {
    let mut out = HashMap::new();
    if !watch.any() {
        return out;
    }
    let mut stack = vec![folder.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if PRUNED.contains(&name.as_str()) || name.ends_with(".egg-info") {
                    continue;
                }
                stack.push(path);
            } else if file_type.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if !watch.wants(&name) {
                    continue;
                }
                if let Ok(meta) = entry.metadata() {
                    let modified = meta.modified().ok();
                    let (secs, nanos) = modified
                        .and_then(|at| at.duration_since(UNIX_EPOCH).ok())
                        .map(|elapsed| (elapsed.as_secs(), elapsed.subsec_nanos() as u64))
                        .unwrap_or((0, 0));
                    if let Ok(relative) = path.strip_prefix(folder) {
                        out.insert(relative.to_path_buf(), (secs, nanos, meta.len()));
                    }
                }
            }
        }
    }
    out
}

fn diff(old: &HashMap<PathBuf, (u64, u64, u64)>, new: &HashMap<PathBuf, (u64, u64, u64)>) -> Vec<PathBuf> {
    let mut changed: Vec<PathBuf> = new
        .iter()
        .filter(|(path, fingerprint)| old.get(*path) != Some(*fingerprint))
        .map(|(path, _)| path.clone())
        .collect();
    for path in old.keys() {
        if !new.contains_key(path) {
            changed.push(path.clone());
        }
    }
    changed.sort();
    changed
}

// ---------- resolving a change to the tests that cover it ----------

enum Resolution {
    /// Configuration changed: run the whole suite.
    Full,
    /// These test files, and nothing else.
    Targets(Vec<PathBuf>),
    /// Nothing covers it: say so, and spawn nothing.
    NoTests,
}

/// A file pytest collects: `test_*.py` or `*_test.py`, beside a `conftest.py` it never collects.
fn is_test_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|one| one.to_str()) else {
        return false;
    };
    name.ends_with(".py") && (name.starts_with("test_") || name.ends_with("_test.py"))
}

fn resolve_targets(folder: &Path, changed: &BTreeSet<PathBuf>, full_on_config_change: bool) -> Resolution {
    let mut targets: BTreeSet<PathBuf> = BTreeSet::new();
    let mut seeds: BTreeSet<PathBuf> = BTreeSet::new();
    let mut full = false;
    for path in changed {
        let name = path.file_name().and_then(|one| one.to_str()).unwrap_or("");
        if matches!(name, "pyproject.toml" | "pytest.ini" | "tox.ini" | "setup.cfg") {
            full = full_on_config_change;
        } else if name == "conftest.py" {
            match path.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => {
                    targets.insert(parent.to_path_buf());
                }
                _ => full = full_on_config_change,
            }
        } else if name.ends_with(".py") {
            seeds.insert(path.clone());
        }
    }
    if full {
        return Resolution::Full;
    }
    if !seeds.is_empty() {
        let dependents = dependents_of(folder);
        let mut visited: HashSet<PathBuf> = HashSet::new();
        let mut frontier: Vec<PathBuf> = seeds.into_iter().collect();
        let mut hops = 0;
        while !frontier.is_empty() && hops < MAX_HOPS {
            hops += 1;
            let mut next: Vec<PathBuf> = Vec::new();
            for file in frontier {
                if !visited.insert(file.clone()) {
                    continue;
                }
                if is_test_file(&file) {
                    targets.insert(file.clone());
                }
                if let Some(importers) = dependents.get(&file) {
                    for importer in importers {
                        if !visited.contains(importer) {
                            next.push(importer.clone());
                        }
                    }
                }
            }
            frontier = next;
        }
    }
    if targets.is_empty() {
        Resolution::NoTests
    } else {
        Resolution::Targets(targets.into_iter().collect())
    }
}

/// The reverse graph of Python imports under one folder: a file and every file that imports it.
fn dependents_of(folder: &Path) -> HashMap<PathBuf, BTreeSet<PathBuf>> {
    let files = python_files(folder);
    let modules = module_map(&files);
    let mut dependents: HashMap<PathBuf, BTreeSet<PathBuf>> = HashMap::new();
    for file in &files {
        let Ok(text) = std::fs::read_to_string(folder.join(file)) else {
            continue;
        };
        for dep in imports_of(&text, file) {
            match module_file(&dep, &modules) {
                Some(target) if target != *file => {
                    dependents.entry(target).or_default().insert(file.clone());
                }
                _ => {}
            }
        }
    }
    dependents
}

fn python_files(folder: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![folder.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if PRUNED.contains(&name.as_str()) || name.ends_with(".egg-info") {
                    continue;
                }
                stack.push(path);
            } else if file_type.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".py") {
                    if let Ok(relative) = path.strip_prefix(folder) {
                        out.push(relative.to_path_buf());
                    }
                }
            }
        }
    }
    out.sort();
    out
}

/// Every dotted name a file may be imported by, from where it sits: at the root, under `src/` or
/// under `lib/` — the three layouts a Python project actually ships.
fn module_names(path: &Path) -> Vec<String> {
    let relative = path.to_string_lossy().replace('\\', "/");
    let stem = relative.strip_suffix(".py").unwrap_or(&relative);
    let stem = stem.strip_suffix("/__init__").unwrap_or(stem);
    let mut names = vec![stem.replace('/', ".")];
    for root in ["src/", "lib/"] {
        if let Some(rest) = stem.strip_prefix(root) {
            names.push(rest.replace('/', "."));
        }
    }
    names
}

fn module_map(files: &[PathBuf]) -> HashMap<String, PathBuf> {
    let mut map = HashMap::new();
    for file in files {
        for name in module_names(file) {
            map.entry(name).or_insert_with(|| file.clone());
        }
    }
    map
}

fn module_file(dep: &str, modules: &HashMap<String, PathBuf>) -> Option<PathBuf> {
    if let Some(file) = modules.get(dep) {
        return Some(file.clone());
    }
    // `from a.b import x` names a.b.x when x is a submodule, and a.b when it is not.
    dep.rsplit_once('.')
        .and_then(|(parent, _)| modules.get(parent).cloned())
}

/// The dotted names one Python file imports, resolved against where it sits for relative imports.
fn imports_of(text: &str, importer: &Path) -> Vec<String> {
    let mut out = Vec::new();
    for line in logical_lines(text) {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // A comment after an import is a comment.
        let line = line.split('#').next().unwrap_or(line).trim();
        if let Some(rest) = line.strip_prefix("import ") {
            for part in rest.split(',') {
                let part = part.trim().split_whitespace().next().unwrap_or("");
                if !part.is_empty() {
                    out.push(part.to_string());
                    if let Some((parent, _)) = part.rsplit_once('.') {
                        out.push(parent.to_string());
                    }
                }
            }
        } else if let Some(rest) = line.strip_prefix("from ") {
            let Some((module, _)) = rest.split_once(" import ") else {
                continue;
            };
            let module = module.trim();
            let dots = module.len() - module.trim_start_matches('.').len();
            let module = module.trim_start_matches('.');
            let absolute = if dots == 0 {
                module.to_string()
            } else {
                let base = package_of(importer, dots);
                if module.is_empty() {
                    base
                } else if base.is_empty() {
                    module.to_string()
                } else {
                    format!("{base}.{module}")
                }
            };
            if !absolute.is_empty() {
                out.push(absolute.clone());
                // `from a.b import x, y`: x and y may each be a submodule a.b.x.
                if let Some((_, names)) = rest.split_once(" import ") {
                    for name in names.trim().trim_matches(['(', ')']).split(',') {
                        let name = name.trim().split_whitespace().next().unwrap_or("");
                        if !name.is_empty() && name != "*" {
                            out.push(format!("{absolute}.{name}"));
                        }
                    }
                }
            }
        }
    }
    out
}

/// The package a relative import starts from: `from .` is the file's own directory as a package,
/// `from ..` is one up, and a package's own `__init__.py` sits in its own package already.
fn package_of(importer: &Path, dots: usize) -> String {
    let mut dir: Vec<&str> = importer
        .parent()
        .map(|parent| parent.to_str().unwrap_or("").split('/').filter(|one| !one.is_empty()).collect())
        .unwrap_or_default();
    for _ in 1..dots {
        dir.pop();
    }
    dir.join(".")
}

/// Import statements with their continuations joined: a backslash or an open parenthesis keeps the
/// statement going onto the next line.
fn logical_lines(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut held = String::new();
    for raw in text.lines() {
        let mut line = raw.to_string();
        if line.ends_with('\\') {
            line.pop();
            held.push_str(&line);
            held.push(' ');
            continue;
        }
        held.push_str(&line);
        let open = held.matches('(').count();
        let close = held.matches(')').count();
        if open > close {
            held.push(' ');
            continue;
        }
        out.push(std::mem::take(&mut held));
    }
    if !held.trim().is_empty() {
        out.push(held);
    }
    out
}

// ---------- the JavaScript and TypeScript side ----------

/// Every extension this plugin understands for Node work, TypeScript first: a change to any of them
/// is worth a run, and a specifier may land on any of them.
const JSTS_EXTS: &[&str] = &["ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs"];

fn has_jsts_extension(name: &str) -> bool {
    name.rsplit('.')
        .next()
        .map(|ext| ext != name && JSTS_EXTS.contains(&ext))
        .unwrap_or(false)
}

/// A file `node --test` collects: `test.*`, `test-*.*`, `*-test.*`, or `*.test.*`.
fn is_jsts_test_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|one| one.to_str()) else {
        return false;
    };
    if !has_jsts_extension(name) {
        return false;
    }
    let stem = name.rsplit_once('.').map(|(stem, _)| stem).unwrap_or(name);
    stem == "test" || stem.starts_with("test-") || stem.ends_with("-test") || stem.ends_with(".test")
}

/// Every JavaScript and TypeScript file under one folder: the walk never follows a link and never
/// enters a pruned directory, exactly like the Python one.
fn jsts_files(folder: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![folder.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_symlink() {
                continue;
            }
            if file_type.is_dir() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if PRUNED.contains(&name.as_str()) || name.ends_with(".egg-info") {
                    continue;
                }
                stack.push(path);
            } else if file_type.is_file() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if has_jsts_extension(&name) {
                    if let Ok(relative) = path.strip_prefix(folder) {
                        out.push(relative.to_path_buf());
                    }
                }
            }
        }
    }
    out.sort();
    out
}

/// One file's relative import specifiers, from a naive read: comments and string contents are lexed
/// just enough that `import` written inside them is never mistaken for a statement. Regex literals
/// are not lexed, so a `/` inside one can begin a "comment" — a rare miss, never a crash.
///
/// Every comparison is against bytes: the scan steps one byte at a time, so a slice taken at that
/// offset would land inside a letter of any script that is not ASCII.
fn jsts_imports_of(text: &str) -> Vec<String> {
    let clean = strip_comments(text);
    let bytes = clean.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let mut keyword: Option<&str> = None;
        for candidate in ["import", "export", "require"] {
            if boundary_open(&clean, i) && bytes[i..].starts_with(candidate.as_bytes()) && boundary_close(&clean, i + candidate.len()) {
                keyword = Some(candidate);
                break;
            }
        }
        let Some(keyword) = keyword else {
            i += 1;
            continue;
        };
        let after = i + keyword.len();
        if keyword == "import" || keyword == "require" {
            let mut paren = after;
            skip_space(&clean, &mut paren);
            // `import("...")` and `require("...")`: a specifier in parentheses, possibly on a
            // later line than the keyword.
            if bytes.get(paren) == Some(&b'(') {
                let mut at = paren + 1;
                skip_space(&clean, &mut at);
                if let Some((spec, _)) = read_js_string(&clean, at) {
                    out.push(spec);
                }
                i = after;
                continue;
            }
        }
        // The statement that follows, until a semicolon or the next statement. Inside it: a string
        // right after `import` (the bare form), or the string after `from` — which also catches
        // `import type {X} from "..."` and `export * from "...".
        let end = statement_end(&clean, after);
        let statement = &clean[after..end];
        let mut at = 0;
        skip_space_in(statement, &mut at);
        if let Some((spec, _)) = read_js_string(statement, at) {
            out.push(spec);
        } else if let Some(from_at) = word_offset(statement, "from") {
            let mut at = from_at + 4;
            skip_space_in(statement, &mut at);
            if let Some((spec, _)) = read_js_string(statement, at) {
                out.push(spec);
            }
        }
        i = end;
    }
    out
}

/// Whether a keyword at `at` begins a word here: the character before it is not part of a name.
fn boundary_open(text: &str, at: usize) -> bool {
    text
        .as_bytes()
        .get(at.wrapping_sub(1))
        .map(|ch| !ch.is_ascii_alphanumeric() && *ch != b'_' && *ch != b'$')
        .unwrap_or(true)
}

/// Whether a keyword ending at `at` ends a word here.
fn boundary_close(text: &str, at: usize) -> bool {
    text.as_bytes()
        .get(at)
        .map(|ch| !ch.is_ascii_alphanumeric() && *ch != b'_' && *ch != b'$')
        .unwrap_or(true)
}

fn skip_space(text: &str, at: &mut usize) {
    let bytes = text.as_bytes();
    while bytes.get(*at).is_some_and(|ch| ch.is_ascii_whitespace()) {
        *at += 1;
    }
}

fn skip_space_in(text: &str, at: &mut usize) {
    skip_space(text, at);
}

/// The first offence of a word before `at`, or the offset past it.
fn word_offset(text: &str, word: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i + word.len() <= bytes.len() {
        if boundary_open(text, i) && bytes[i..].starts_with(word.as_bytes()) && boundary_close(text, i + word.len()) {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// The string literal that opens at `at`, if one does: single or double quotes, backslash escapes
/// taken as the plain character, and no newline inside.
fn read_js_string(text: &str, at: usize) -> Option<(String, usize)> {
    let bytes = text.as_bytes();
    let quote = *bytes.get(at)?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    let mut value = String::new();
    let mut i = at + 1;
    while let Some(&ch) = bytes.get(i) {
        if ch == b'\\' {
            i += 1;
            if let Some(next) = text[i..].chars().next() {
                value.push(next);
                i += next.len_utf8();
            }
            continue;
        }
        if ch == quote {
            return Some((value, i + 1));
        }
        if ch == b'\n' {
            return None;
        }
        // A specifier is text, not bytes: a name with a letter outside ASCII is kept whole.
        let one = text[i..].chars().next()?;
        value.push(one);
        i += one.len_utf8();
    }
    None
}

/// Where the current `import`/`export` statement ends: at the next semicolon, or at the next line a
/// new statement seems to start at. Semicolon-less code ends at that line, which keeps the next
/// statement for the next pass of the outer loop.
fn statement_end(text: &str, from: usize) -> usize {
    let bytes = text.as_bytes();
    for i in from..bytes.len() {
        if bytes[i] == b';' {
            return i;
        }
        if bytes[i] == b'\n' {
            let mut at = i + 1;
            skip_space(&text, &mut at);
            for candidate in ["import", "export", "require", "const", "let", "var", "function", "class"] {
                if boundary_open(&text, at)
                    && bytes[at..].starts_with(candidate.as_bytes())
                    && boundary_close(&text, at + candidate.len())
                {
                    return i;
                }
            }
        }
    }
    bytes.len()
}

/// Comments removed, their newlines kept so the rest keeps its shape for the statement scanner. The
/// bytes are copied as they are: reading one at a time as a character would split every letter
/// outside ASCII into several, and scramble a specifier that carries one.
fn strip_comments(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let ch = bytes[i];
        if let Some(open) = quote {
            if ch == b'\\' && i + 1 < bytes.len() {
                // The escape and the whole character it escapes, never half of one.
                let escaped = text[i + 1..].chars().next().map(char::len_utf8).unwrap_or(1);
                out.extend_from_slice(&bytes[i..i + 1 + escaped]);
                i += 1 + escaped;
                continue;
            }
            out.push(ch);
            if ch == open {
                quote = None;
            }
            i += 1;
            continue;
        }
        if ch == b'"' || ch == b'\'' || ch == b'`' {
            quote = Some(ch);
            out.push(ch);
            i += 1;
            continue;
        }
        if ch == b'/' && bytes.get(i + 1) == Some(&b'/') {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if ch == b'/' && bytes.get(i + 1) == Some(&b'*') {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                if bytes[i] == b'\n' {
                    out.push(b'\n');
                }
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        out.push(ch);
        i += 1;
    }
    // Every cut above lands between characters, so these are the file's own bytes.
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

/// A relative specifier resolved against its importer: the literal path, then with each known
/// extension added, then as a directory with `index.<ext>`. TypeScript's ESM convention also maps a
/// `.js` specifier back onto its `.ts` file when no plain JavaScript file is there.
fn resolve_jsts_specifier(importer: &Path, spec: &str, known: &std::collections::HashSet<PathBuf>) -> Option<PathBuf> {
    if spec != "." && spec != ".." && !spec.starts_with("./") && !spec.starts_with("../") {
        return None;
    }
    let mut parts: Vec<&str> = importer
        .parent()
        .map(|parent| parent.to_str().unwrap_or("").split('/').filter(|one| !one.is_empty()).collect())
        .unwrap_or_default();
    for segment in spec.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                if parts.pop().is_none() {
                    // It escapes the folder: not a file of this workspace.
                    return None;
                }
            }
            other => parts.push(other),
        }
    }
    let base = parts.join("/");
    let mut candidates: Vec<String> = vec![base.clone()];
    for ext in JSTS_EXTS {
        candidates.push(format!("{base}.{ext}"));
    }
    for ext in JSTS_EXTS {
        candidates.push(format!("{base}/index.{ext}"));
    }
    // TypeScript's ESM spelling: `import "./calc.js"` meaning `calc.ts`.
    if let Some((stem, ext)) = base.rsplit_once('.') {
        let twin = match ext {
            "js" => Some("ts"),
            "jsx" => Some("tsx"),
            "mjs" => Some("mts"),
            "cjs" => Some("cts"),
            _ => None,
        };
        if let Some(twin_ext) = twin {
            candidates.push(format!("{stem}.{twin_ext}"));
        }
    }
    for candidate in candidates {
        let path = PathBuf::from(&candidate);
        if known.contains(&path) {
            return Some(path);
        }
    }
    None
}

/// The reverse graph of JavaScript imports under one folder: a file and every file that imports it.
fn jsts_dependents_of(folder: &Path) -> HashMap<PathBuf, BTreeSet<PathBuf>> {
    let files = jsts_files(folder);
    let known: std::collections::HashSet<PathBuf> = files.iter().cloned().collect();
    let mut dependents: HashMap<PathBuf, BTreeSet<PathBuf>> = HashMap::new();
    for file in &files {
        let Ok(text) = std::fs::read_to_string(folder.join(file)) else {
            continue;
        };
        for spec in jsts_imports_of(&text) {
            if let Some(target) = resolve_jsts_specifier(file, &spec, &known) {
                if target != *file {
                    dependents.entry(target).or_default().insert(file.clone());
                }
            }
        }
    }
    dependents
}

/// A change in the JavaScript world to the tests that cover it: the same reading pytest gets, over a
/// graph parsed from `import`, `export` and `require`.
fn resolve_jsts(folder: &Path, changed: &BTreeSet<PathBuf>, full_on_config_change: bool) -> Resolution {
    let mut targets: BTreeSet<PathBuf> = BTreeSet::new();
    let mut seeds: BTreeSet<PathBuf> = BTreeSet::new();
    for path in changed {
        let name = path.file_name().and_then(|one| one.to_str()).unwrap_or("");
        if matches!(name, "tsconfig.json" | "package.json") {
            if full_on_config_change {
                return Resolution::Full;
            }
        } else if has_jsts_extension(name) {
            seeds.insert(path.clone());
        }
    }
    if !seeds.is_empty() {
        let dependents = jsts_dependents_of(folder);
        let mut visited: HashSet<PathBuf> = HashSet::new();
        let mut frontier: Vec<PathBuf> = seeds.into_iter().collect();
        let mut hops = 0;
        while !frontier.is_empty() && hops < MAX_HOPS {
            hops += 1;
            let mut next: Vec<PathBuf> = Vec::new();
            for file in frontier {
                if !visited.insert(file.clone()) {
                    continue;
                }
                if is_jsts_test_file(&file) {
                    targets.insert(file.clone());
                }
                if let Some(importers) = dependents.get(&file) {
                    for importer in importers {
                        if !visited.contains(importer) {
                            next.push(importer.clone());
                        }
                    }
                }
            }
            frontier = next;
        }
    }
    if targets.is_empty() {
        Resolution::NoTests
    } else {
        Resolution::Targets(targets.into_iter().collect())
    }
}

/// The type checker that runs with a JavaScript run, when one can be found: a configured program,
/// the project's own `node_modules/.bin/tsc`, or `tsc` on the PATH. No `tsconfig.json`, or no
/// `tsc` anywhere, is no typecheck — never an error.
fn choose_tsc(folder: &Path, config: &Config) -> Option<String> {
    if !folder.join("tsconfig.json").is_file() {
        return None;
    }
    if let Some(program) = &config.tsc {
        return Some(program.clone());
    }
    let local = folder.join("node_modules/.bin/tsc");
    #[cfg(unix)]
    let executable = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(&local)
            .map(|meta| meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    };
    #[cfg(not(unix))]
    let executable = local.is_file();
    if executable {
        return Some(local.to_string_lossy().into_owned());
    }
    std::env::var_os("PATH")
        .and_then(|paths| {
            std::env::split_paths(&paths)
                .any(|dir| dir.join("tsc").is_file())
                .then_some("tsc".to_string())
        })
}

/// The runner at the other end of every JavaScript run: a configured program, or `node` on the PATH.
fn choose_node(config: &Config) -> String {
    config.node.clone().unwrap_or_else(|| "node".to_string())
}

/// What `node --test` said, read back: the failing tests by name, and the counts when it gave them.
/// The TAP shape is `not ok <n> - <name>`, nested for a failing test inside a failing file.
fn parse_tap(output: &str, exit_code: i32) -> (String, String, Vec<Failure>) {
    let mut failures: Vec<Failure> = Vec::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut tests: Option<String> = None;
    let mut pass: Option<String> = None;
    let mut fail: Option<String> = None;
    for line in output.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("not ok ") {
            let name = rest
                .split(" # ")
                .next()
                .map(|text| text.split_once(" - ").map(|(_, name)| name).unwrap_or(text).trim().to_string())
                .unwrap_or_default();
            if !name.is_empty() {
                let key = (name.clone(), String::new());
                if seen.insert(key) {
                    failures.push(Failure {
                        nodeid: name,
                        summary: String::new(),
                    });
                }
            }
        } else if let Some(rest) = line.strip_prefix("# tests ") {
            tests = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("# pass ") {
            pass = Some(rest.trim().to_string());
        } else if let Some(rest) = line.strip_prefix("# fail ") {
            fail = Some(rest.trim().to_string());
        }
    }
    let status = if exit_code == 0 { "passed" } else { "failed" };
    let summary = match (tests, pass, fail) {
        (Some(tests), Some(pass), Some(fail)) => format!("{fail} failing of {tests} tests ({pass} passed)"),
        _ => format!("node --test exited with {exit_code}"),
    };
    (status.to_string(), summary, failures)
}

/// What `tsc` said, read back: `path(line,col): error TS<nnnn>: message`, one failure per file.
fn parse_tsc_errors(output: &str) -> Vec<Failure> {
    let mut failures: Vec<Failure> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for line in output.lines() {
        let Some((where_, rest)) = line.split_once(": error TS") else {
            continue;
        };
        let file = match where_.rsplit_once('(') {
            Some((file, _)) => file.trim().to_string(),
            None => where_.trim().to_string(),
        };
        if file.is_empty() || !seen.insert(file.clone()) {
            continue;
        }
        failures.push(Failure {
            nodeid: file,
            summary: format!("error TS{rest}"),
        });
    }
    failures
}

// ---------- the runner: one pytest at a time ----------

fn runner(hub: Hub) {
    loop {
        let job = {
            let (lock, cvar) = &*hub;
            let mut shared = lock.lock().unwrap();
            loop {
                if shared.stopping {
                    return;
                }
                if let Some(job) = shared.jobs.pop_front() {
                    break Some(job);
                }
                shared = cvar.wait(shared).unwrap();
            }
        };
        let Some(job) = job else {
            return;
        };
        {
            let (lock, _) = &*hub;
            let mut shared = lock.lock().unwrap();
            if shared.stopping {
                return;
            }
            let Some(tracker) = shared.workspaces.get_mut(&job.workspace) else {
                continue;
            };
            tracker.phase = "running".to_string();
            let running_started_at = now_ms();
            set_timeline_kind(tracker, "run", running_started_at);
            tracker.running = Some(RunningView {
                targets: target_names(&job),
                started_at: running_started_at,
            });
        }
        // A side with no work merges away: a TypeScript-only workspace never spawns pytest, and a
        // Python-only one never asks for tsc or node.
        let outcome = merge_outcomes(
            if !job.full && job.targets.is_empty() {
                Outcome {
                    status: "no_tests".to_string(),
                    summary: String::new(),
                    failures: vec![],
                    output: String::new(),
                    duration_ms: 0,
                }
            } else {
                run_pytest(&hub, job.workspace.clone(), &job)
            },
            run_jsts(&hub, job.workspace.clone(), &job),
        );
        {
            let (lock, _) = &*hub;
            let mut shared = lock.lock().unwrap();
            // One card holds every problem this workspace has, updated by its key on every bad run,
            // and taken off when a run comes back green, so the board shows the problems and
            // nothing when there are none. Decided first, queued after the tracker is unborrowed.
            let (card_note, card_remove) = {
                let Some(tracker) = shared.workspaces.get_mut(&job.workspace) else {
                    continue;
                };
                tracker.running = None;
                tracker.phase = "idle".to_string();
                tracker.last_output = outcome.output.clone();
                tracker.last_run = Some(LastRun {
                    at: now_ms(),
                    status: outcome.status.clone(),
                    summary: outcome.summary.clone(),
                    duration_ms: outcome.duration_ms,
                    targets: target_names(&job),
                    failures: outcome.failures.clone(),
                });
                let finished = now_ms();
                let mut note = None;
                let mut removal = None;
                match outcome.status.as_str() {
                    "failed" | "timeout" | "error" => {
                        tracker.card_posted = true;
                        set_timeline_kind(tracker, "idle", finished);
                        let outcome_log = Arc::new(outcome.output.clone());
                        for failure in &outcome.failures {
                            tracker.failure_events.push(TimelineFailure {
                                at_ms: finished,
                                nodeid: failure.nodeid.clone(),
                                summary: failure.summary.clone(),
                                log: Arc::clone(&outcome_log),
                            });
                        }
                        if outcome.failures.is_empty() {
                            tracker.failure_events.push(TimelineFailure {
                                at_ms: finished,
                                nodeid: "error".to_string(),
                                summary: outcome.summary.clone(),
                                log: outcome_log,
                            });
                        }
                        prune_timeline(tracker, finished);
                        note = Some(json!({
                            "workspace": tracker.workspace_id.clone(),
                            "title": format!("bughound: {}", outcome.status),
                            "markdown": format!(
                                "**{} · {}** — {}\n\n{}\n\n{}",
                                tracker.name,
                                tracker.thread_id,
                                outcome.status,
                                outcome.summary,
                                failure_lines(&outcome.failures)
                            ),
                            "key": tracker.card_key.clone(),
                        }));
                    }
                    "passed" => {
                        set_timeline_kind(tracker, "pass", finished);
                        record_timeline_interval(tracker, finished, finished + 3_000, "pass");
                        tracker.timeline_kind = Some("idle".to_string());
                        tracker.timeline_since = finished + 3_000;
                        if tracker.card_posted {
                            tracker.card_posted = false;
                            removal = Some(json!({
                                "workspace": tracker.workspace_id.clone(),
                                "key": tracker.card_key.clone(),
                            }));
                        }
                    }
                    _ => set_timeline_kind(tracker, "idle", finished),
                }
                (note, removal)
            };
            if let Some(note) = card_note {
                shared.notes.push_back(note);
            }
            if let Some(removal) = card_remove {
                shared.removals.push_back(removal);
            }
        }
        // Changes that arrived during the run are now eligible for their own.
        wake(&hub);
    }
}

/// One drain thread for one pipe of the child. A run that fills its output must not stop at a full
/// pipe while nobody reads it, so both pipes are drained while the child runs.
fn spawn_reader<R: io::Read + Send + 'static>(stream: R, output: Arc<Mutex<String>>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut reader = io::BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let mut text = output.lock().unwrap();
                    text.push_str(&line);
                    if text.len() > OUTPUT_CAP {
                        *text = truncate_output(&text);
                    }
                }
                Err(_) => break,
            }
        }
    })
}

/// A full log keeps its head and its tail: the counts live in the last line, and cutting only the
/// front would lose the summary a failing run is read by.
fn truncate_output(text: &str) -> String {
    const HEAD_CAP: usize = 16 * 1024;
    const TAIL_CAP: usize = 48 * 1024;
    if text.len() <= OUTPUT_CAP {
        return text.to_string();
    }
    let mut head_end = HEAD_CAP;
    while head_end > 0 && !text.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let head = match text[..head_end].rfind('\n') {
        Some(at) => &text[..at],
        None => &text[..head_end],
    };
    let tail_start = text.len() - TAIL_CAP;
    let mut tail_start = tail_start;
    while tail_start < text.len() && !text.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let tail = match text[tail_start..].find('\n') {
        Some(at) => &text[tail_start + at + 1..],
        None => &text[tail_start..],
    };
    format!("{head}\n… [output truncated] …\n{tail}")
}

fn target_names(job: &Job) -> Vec<String> {
    if job.full || job.js_full {
        vec!["<full suite>".to_string()]
    } else {
        job.targets
            .iter()
            .chain(job.js_targets.iter())
            .map(|target| target.to_string_lossy().into_owned())
            .collect()
    }
}

fn failure_lines(failures: &[Failure]) -> String {
    failures
        .iter()
        .take(10)
        .map(|failure| format!("- `{}` — {}", failure.nodeid, failure.summary))
        .collect::<Vec<_>>()
        .join("\n")
}

/// The program that runs the tests and whatever has to be said first: a configured binary with its
/// flags, a project venv's pytest or `python -m pytest`, or `pytest` on the PATH. Never a shell.
fn choose_pytest(folder: &Path, config: &Config) -> (String, Vec<String>) {
    if let Some(bin) = &config.pytest {
        return (bin.clone(), config.pytest_args.clone());
    }
    let candidates: [(&str, Vec<String>); 4] = [
        (".venv/bin/pytest", vec![]),
        ("venv/bin/pytest", vec![]),
        (".venv/bin/python", vec!["-m".to_string(), "pytest".to_string()]),
        ("venv/bin/python", vec!["-m".to_string(), "pytest".to_string()]),
    ];
    for (relative, prefix) in candidates {
        let path = folder.join(relative);
        #[cfg(unix)]
        let executable = {
            use std::os::unix::fs::PermissionsExt;
            std::fs::metadata(&path)
                .map(|meta| meta.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        };
        #[cfg(not(unix))]
        let executable = path.is_file();
        if executable {
            return (path.to_string_lossy().into_owned(), prefix);
        }
    }
    ("pytest".to_string(), vec![])
}

struct Outcome {
    status: String,
    summary: String,
    failures: Vec<Failure>,
    output: String,
    duration_ms: u64,
}

/// One child run to the end of its pipes: streamed output, the timeout, and the exit. Every
/// checker runs through here — pytest, the type checker, `node --test` — so the deadline, the
/// readers and the output cap are one behavior instead of three.
struct ProgramRun {
    code: Option<i32>,
    output: String,
    timed_out: bool,
}

fn run_program(program: &str, args: &[String], folder: &Path, env: &[(&str, &str)], timeout_s: u64, hub: &Hub) -> ProgramRun {
    let started = Instant::now();
    let mut command = Command::new(program);
    command
        .args(args)
        .current_dir(folder)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in env {
        command.env(key, value);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return ProgramRun {
                code: None,
                output: format!("could not start {program}: {error}\n"),
                timed_out: false,
            };
        }
    };
    let output = Arc::new(Mutex::new(String::new()));
    let mut readers: Vec<std::thread::JoinHandle<()>> = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        readers.push(spawn_reader(stdout, Arc::clone(&output)));
    }
    if let Some(stderr) = child.stderr.take() {
        readers.push(spawn_reader(stderr, Arc::clone(&output)));
    }
    let deadline = started + Duration::from_secs(timeout_s);
    let mut timed_out = false;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                let (lock, _) = &**hub;
                if lock.lock().unwrap().stopping || Instant::now() >= deadline {
                    if Instant::now() >= deadline {
                        timed_out = true;
                    }
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => break,
        }
    }
    for reader in readers {
        let _ = reader.join();
    }
    let code = child.try_wait().ok().flatten().and_then(|status| status.code());
    let text = output.lock().unwrap().clone();
    ProgramRun {
        code,
        output: text,
        timed_out,
    }
}

fn run_pytest(hub: &Hub, workspace: String, job: &Job) -> Outcome {
    let (config, folder) = workspace_config(hub, workspace);
    let started = Instant::now();
    let (program, mut prefix) = choose_pytest(&folder, &config);
    let mut args = prefix.drain(..).collect::<Vec<String>>();
    args.extend(["-q", "--tb=short", "--no-header", "--color=no"].iter().map(|one| one.to_string()));
    if !job.full {
        for target in &job.targets {
            args.push(target.to_string_lossy().into_owned());
        }
    }
    let run = run_program(
        &program,
        &args,
        &folder,
        &[("PYTHONDONTWRITEBYTECODE", "1"), ("PYTHONUNBUFFERED", "1")],
        config.timeout_s,
        hub,
    );
    if run.timed_out {
        return Outcome {
            status: "timeout".to_string(),
            summary: format!("did not finish in {}s", config.timeout_s),
            failures: vec![],
            output: run.output,
            duration_ms: started.elapsed().as_millis() as u64,
        };
    }
    if run.code.is_none() && run.output.starts_with("could not start") {
        return Outcome {
            status: "error".to_string(),
            summary: run.output.trim().to_string(),
            failures: vec![],
            output: run.output,
            duration_ms: started.elapsed().as_millis() as u64,
        };
    }
    let (status, summary, failures) = parse_result(&run.output, run.code.unwrap_or(-1));
    Outcome {
        status,
        summary,
        failures,
        output: run.output,
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

/// The folder and configuration a run works in, read without the lock.
fn workspace_config(hub: &Hub, workspace: String) -> (Config, PathBuf) {
    let (lock, _) = &**hub;
    let shared = lock.lock().unwrap();
    let folder = shared
        .workspaces
        .get(&workspace)
        .map(|tracker| tracker.folder.clone())
        .unwrap_or_default();
    (shared.config.clone(), folder)
}

/// The JavaScript side of a job: the type check first, then the tests it names, with the same
/// deadline and the same output cap as a pytest run. A job with no JavaScript work is `no_tests`
/// and merges away into the Python outcome.
fn run_jsts(hub: &Hub, workspace: String, job: &Job) -> Outcome {
    let (config, folder) = workspace_config(hub, workspace);
    let started = Instant::now();
    let mut failures: Vec<Failure> = Vec::new();
    let mut summaries: Vec<String> = Vec::new();
    let mut output = String::new();
    let mut ran = false;
    let mut status = "passed";
    if job.js_typecheck {
        if let Some(tsc) = choose_tsc(&folder, &config) {
            ran = true;
            let run = run_program(&tsc, &["--noEmit".to_string()], &folder, &[], config.timeout_s, hub);
            if run.timed_out {
                status = "timeout";
                summaries.push(format!("did not finish in {}s", config.timeout_s));
                output.push_str(&run.output);
            } else if run.code.is_none() && run.output.starts_with("could not start") {
                status = "error";
                summaries.push(run.output.trim().to_string());
                output.push_str(&run.output);
            } else if run.code.unwrap_or(-1) != 0 {
                status = "failed";
                let errors = parse_tsc_errors(&run.output);
                summaries.push(format!("{} type error(s)", errors.len()));
                failures.extend(errors);
                output.push_str(&run.output);
            }
        }
    }
    let tests: Vec<PathBuf> = if job.js_full {
        jsts_files(&folder).into_iter().filter(|file| is_jsts_test_file(file)).collect()
    } else {
        job.js_targets.clone()
    };
    if !tests.is_empty() {
        ran = true;
        let node = choose_node(&config);
        let args: Vec<String> = {
            let mut args = vec!["--test".to_string()];
            for test in &tests {
                args.push(test.to_string_lossy().into_owned());
            }
            args
        };
        let run = run_program(&node, &args, &folder, &[], config.timeout_s, hub);
        if run.timed_out {
            status = "timeout";
            summaries.push(format!("did not finish in {}s", config.timeout_s));
        } else if run.code.is_none() && run.output.starts_with("could not start") {
            status = "error";
            summaries.push(run.output.trim().to_string());
        } else {
            let (test_status, summary, test_failures) = parse_tap(&run.output, run.code.unwrap_or(-1));
            summaries.push(summary);
            if test_status == "failed" && status == "passed" {
                status = "failed";
            }
            failures.extend(test_failures);
        }
        output.push_str(&run.output);
    }
    if !ran {
        return Outcome {
            status: rep_status(status, false),
            summary: summaries.join(" · "),
            failures,
            output: truncate_output(&output),
            duration_ms: started.elapsed().as_millis() as u64,
        };
    }
    Outcome {
        status: status.to_string(),
        summary: summaries.join(" · "),
        failures,
        output: truncate_output(&output),
        duration_ms: started.elapsed().as_millis() as u64,
    }
}

/// Whether a status actually means nothing ran, so merging cannot call it a pass.
fn rep_status(status: &str, ran: bool) -> String {
    if ran {
        status.to_string()
    } else {
        "no_tests".to_string()
    }
}

/// Two outcomes, one run: a side that ran nothing merges away, and a timeout or an error on
/// either side is what the run is.
fn merge_outcomes(left: Outcome, right: Outcome) -> Outcome {
    if left.status == "no_tests" {
        return right;
    }
    if right.status == "no_tests" {
        return left;
    }
    let rank = |status: &str| match status {
        "timeout" => 3,
        "error" => 2,
        "failed" => 1,
        _ => 0,
    };
    let status = if rank(&left.status) >= rank(&right.status) {
        left.status.clone()
    } else {
        right.status.clone()
    };
    let summary = [left.summary, right.summary]
        .into_iter()
        .filter(|one| !one.is_empty())
        .collect::<Vec<_>>()
        .join(" · ");
    let output = truncate_output(&format!("{}\n{}\n", left.output, right.output));
    Outcome {
        status,
        summary,
        failures: [left.failures, right.failures].concat(),
        output,
        duration_ms: left.duration_ms + right.duration_ms,
    }
}

/// What a `-q --tb=short` run said, read back: the summary counts, and the failures by node id.
fn parse_result(output: &str, exit_code: i32) -> (String, String, Vec<Failure>) {
    let mut failures: Vec<Failure> = Vec::new();
    let mut summary = String::new();
    for line in output.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("FAILED ") {
            let (nodeid, note) = rest.split_once(" - ").unwrap_or((rest, ""));
            failures.push(Failure {
                nodeid: nodeid.trim().to_string(),
                summary: note.trim().to_string(),
            });
        } else if let Some(rest) = line.strip_prefix("ERROR ") {
            let (nodeid, note) = rest.split_once(" - ").unwrap_or((rest, ""));
            failures.push(Failure {
                nodeid: nodeid.trim().to_string(),
                summary: note.trim().to_string(),
            });
        } else if line.contains(" in ") && (line.contains("passed") || line.contains("failed") || line.contains("error")) {
            // The summary banner ends "in 0.42s"; a traceback line that merely mentions "error" ends
            // a different way and must not stand in for it.
            summary = line.to_string();
        }
    }
    if summary.is_empty() {
        summary = output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .last()
            .unwrap_or("")
            .trim()
            .to_string();
    }
    let status = match exit_code {
        0 => "passed",
        1 => "failed",
        5 => "no_tests",
        _ => "error",
    };
    (status.to_string(), summary, failures)
}

// ---------- what the panel reads ----------

fn settings_view(hub: &Hub) -> Value {
    let (lock, _) = &**hub;
    let shared = lock.lock().unwrap();
    settings_view_of(&shared)
}

fn settings_view_of(shared: &Shared) -> Value {
    json!({
        "intervalMs": shared.config.interval_ms,
        "debounceMs": shared.config.debounce_ms,
        "timeoutS": shared.config.timeout_s,
        "fullOnConfigChange": shared.config.full_on_config_change,
    })
}

fn state_snapshot(hub: &Hub, only: &str, selected_lane: Option<&str>) -> Value {
    let (lock, _) = &**hub;
    let shared = lock.lock().unwrap();
    let now = now_ms();
    let rows: Vec<Value> = shared
        .workspaces
        .values()
        .filter(|one| only.is_empty() || one.id == only || one.workspace_id == only)
        .map(|one| {
            let phase = if shared.paused {
                "paused".to_string()
            } else {
                one.phase.clone()
            };
            json!({
                "id": one.id,
                "trackId": one.id,
                "workspaceId": one.workspace_id,
                "cardKey": one.card_key,
                "name": one.name,
                "branch": one.branch,
                "threadId": one.thread_id,
                "folder": one.folder.to_string_lossy(),
                "watched": one.watched,
                // What this folder is watched for, so the panel's switches show the truth the watch is run by.
                "watch": watch_of(&shared, &one.id).view(),
                "phase": phase,
                "message": one.message,
                "timeline": timeline_events_json(one, now),
                "failureEvents": timeline_failures_json(one, now, selected_lane == Some(one.id.as_str())),
                "pending": one.pending.iter().map(|path| path.to_string_lossy().into_owned()).collect::<Vec<_>>(),
                "running": one.running.as_ref().map(|running| json!({
                    "targets": running.targets,
                    "startedAt": running.started_at,
                })),
                "lastRun": one.last_run.as_ref().map(|last| json!({
                    "at": last.at,
                    "status": last.status,
                    "summary": last.summary,
                    "durationMs": last.duration_ms,
                    "targets": last.targets,
                    "failures": last.failures.iter().map(|failure| json!({
                        "nodeid": failure.nodeid,
                        "summary": failure.summary,
                    })).collect::<Vec<_>>(),
                })),
                "lastScanAt": one.last_scan_at,
                "lastChangeAt": one.last_change_at,
            })
        })
        .collect();
    json!({
        "paused": shared.paused,
        "nowMs": now,
        "phase": computed_phase(&shared),
        "intervalMs": shared.config.interval_ms,
        "debounceMs": shared.config.debounce_ms,
        "timeoutS": shared.config.timeout_s,
        "settings": settings_view_of(&shared),
        "workspaces": rows,
    })
}

fn computed_phase(shared: &Shared) -> &'static str {
    if shared.paused {
        return "paused";
    }
    if shared.workspaces.values().any(|one| one.running.is_some()) {
        return "running";
    }
    if shared.workspaces.values().any(|one| one.phase == "debouncing") {
        return "debouncing";
    }
    "idle"
}

fn last_output(hub: &Hub, workspace: &str) -> String {
    let (lock, _) = &**hub;
    let shared = lock.lock().unwrap();
    if workspace.is_empty() {
        shared
            .workspaces
            .values()
            .max_by_key(|one| one.last_run.as_ref().map(|last| last.at).unwrap_or(0))
            .map(|one| one.last_output.clone())
            .unwrap_or_default()
    } else if let Some(one) = shared.workspaces.get(workspace) {
        one.last_output.clone()
    } else {
        shared
            .workspaces
            .values()
            .filter(|one| one.workspace_id == workspace)
            .max_by_key(|one| one.last_run.as_ref().map(|last| last.at).unwrap_or(0))
            .map(|one| one.last_output.clone())
            .unwrap_or_default()
    }
}

fn queue_manual_run(hub: &Hub, scope: &str, workspace: &str) -> Value {
    let ids: Vec<String> = {
        let (lock, _) = &**hub;
        let shared = lock.lock().unwrap();
        if workspace.is_empty() {
            shared.workspaces.keys().cloned().collect()
        } else if shared.workspaces.contains_key(workspace) {
            vec![workspace.to_string()]
        } else {
            shared
                .workspaces
                .values()
                .filter(|one| one.workspace_id == workspace)
                .map(|one| one.id.clone())
                .collect()
        }
    };
    let mut queued = false;
    // Why nothing was queued, when nothing was: a folder with nothing switched on has nothing to run.
    let mut off: Vec<String> = Vec::new();
    for id in ids {
        let watch = {
            let (lock, _) = &**hub;
            let shared = lock.lock().unwrap();
            watch_of(&shared, &id)
        };
        if !watch.any() {
            off.push(id);
            continue;
        }
        if scope == "failed" {
            let targets: Vec<PathBuf> = {
                let (lock, _) = &**hub;
                let shared = lock.lock().unwrap();
                shared
                    .workspaces
                    .get(&id)
                    .and_then(|tracker| tracker.last_run.as_ref())
                    .map(|last| {
                        last.failures
                            .iter()
                            .map(|failure| PathBuf::from(failure.nodeid.split("::").next().unwrap_or("")))
                            .collect()
                    })
                    .unwrap_or_default()
            };
            if targets.is_empty() {
                continue;
            }
            // A failure names a Python node or a JavaScript file; each side reruns only its own, and
            // only while that side is switched on.
            let mut py_targets = Vec::new();
            let mut js_targets = Vec::new();
            for target in targets {
                if target.extension().is_some_and(|ext| JSTS_EXTS.contains(&ext.to_str().unwrap_or(""))) {
                    if watch.javascript {
                        js_targets.push(target);
                    }
                } else if watch.python {
                    py_targets.push(target);
                }
            }
            if py_targets.is_empty() && js_targets.is_empty() {
                continue;
            }
            push_job(
                hub,
                Job {
                    workspace: id,
                    targets: py_targets,
                    full: false,
                    js_targets,
                    js_full: false,
                    js_typecheck: false,
                    reason: "rerun the failures".to_string(),
                },
            );
            queued = true;
        } else {
            // A whole-suite run covers only the sides this folder is watched for: a JavaScript folder
            // never spawns pytest, and a Python one never asks for tsc or node.
            push_job(
                hub,
                Job {
                    workspace: id,
                    targets: vec![],
                    full: watch.python,
                    js_targets: vec![],
                    js_full: watch.javascript,
                    js_typecheck: watch.javascript,
                    reason: "manual run all".to_string(),
                },
            );
            queued = true;
        }
    }
    json!({
        "queued": queued,
        "scope": scope,
        "off": off,
        "reason": if queued { String::new() } else { NOTHING_ON.to_string() },
    })
}

// ---------- the tests ----------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    /// A daemon a test can drive: the lines it says, and the frames this plugin wrote back.
    struct Script {
        incoming: VecDeque<String>,
        sent: Vec<Value>,
    }

    impl Script {
        fn new(lines: Vec<Value>) -> Self {
            Self {
                incoming: lines.into_iter().map(|line| line.to_string()).collect(),
                sent: Vec::new(),
            }
        }
    }

    impl Transport for Script {
        fn write(&mut self, message: Value) -> io::Result<()> {
            self.sent.push(message);
            Ok(())
        }

        fn read_line(&mut self) -> io::Result<Option<String>> {
            Ok(self.incoming.pop_front())
        }
    }

    fn call_frame(id: i64, method: &str, params: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
    }

    fn response_frame(id: i64, result: Value) -> Value {
        json!({ "jsonrpc": "2.0", "id": id, "result": result })
    }

    /// A folder a test can edit under, given a name no other test writes at once.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path = std::env::temp_dir().join(format!("bughound-test-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).unwrap();
            Scratch(path)
        }

        fn write(&self, relative: &str, text: &str) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn config_defaults_and_flags() {
        let config = parse_args(&[]).unwrap();
        assert_eq!(config.interval_ms, 2000);
        assert_eq!(config.debounce_ms, 500);
        assert_eq!(config.timeout_s, 60);
        assert!(config.node.is_none());
        assert!(config.tsc.is_none());
        let config = parse_args(&[
            "--interval-ms".to_string(),
            "100".to_string(),
            "--workspace=foo".to_string(),
            "--pytest".to_string(),
            "uv".to_string(),
            "--pytest-arg".to_string(),
            "run".to_string(),
            "--node=deno".to_string(),
            "--tsc".to_string(),
            "tsc-next".to_string(),
        ])
        .unwrap();
        assert_eq!(config.interval_ms, 250, "an interval under a quarter second is clamped up");
        assert_eq!(config.workspaces, vec!["foo".to_string()]);
        assert_eq!(config.pytest.as_deref(), Some("uv"));
        assert_eq!(config.pytest_args, vec!["run".to_string()]);
        assert_eq!(config.node.as_deref(), Some("deno"));
        assert_eq!(config.tsc.as_deref(), Some("tsc-next"));
        assert!(parse_args(&["--nope".to_string()]).is_err());
        assert!(parse_args(&["--interval-ms".to_string(), "soon".to_string()]).is_err());
        assert!(parse_args(&["--interval-ms".to_string()]).is_err());
        // Nothing is watched until a `--watch` arg or the panel says what for.
        assert_eq!(parse_args(&[]).unwrap().watch_defaults, Watch::default());
        let config = parse_args(&["--watch".to_string(), "python".to_string(), "--watch".to_string(), "javascript".to_string()]).unwrap();
        assert_eq!(config.watch_defaults, Watch { python: true, javascript: true });
        assert!(parse_args(&["--watch".to_string(), "ruby".to_string()]).is_err(), "an unknown side is refused rather than guessed");
    }

    #[test]
    fn a_deep_change_pulls_in_only_the_tests_that_cover_it() {
        let project = Scratch::new("deep-closure");
        project.write("lib.py", "X = 1\n");
        project.write("fun.py", "from lib import X\n");
        project.write("tests/test_fun.py", "from fun import X\n\ndef test_fun():\n    assert X == 1\n");
        project.write("tests/test_other.py", "def test_other():\n    assert True\n");
        let files: BTreeSet<PathBuf> = [PathBuf::from("lib.py")].into_iter().collect();
        match resolve_targets(&project.0, &files, false) {
            Resolution::Targets(targets) => {
                assert_eq!(targets, vec![PathBuf::from("tests/test_fun.py")]);
            }
            _ => panic!("a change to lib.py should resolve through fun.py to only test_fun.py"),
        }
    }

    #[test]
    fn a_changed_module_pulls_in_the_test_that_imports_it() {
        let project = Scratch::new("closure");
        project.write("pkg/calc.py", "def add(a, b):\n    return a + b\n");
        project.write(
            "tests/test_calc.py",
            "from pkg.calc import add\n\ndef test_add():\n    assert add(1, 2) == 3\n",
        );
        project.write("pkg/unused.py", "X = 1\n");
        let files: BTreeSet<PathBuf> = [PathBuf::from("pkg/calc.py")].into_iter().collect();
        match resolve_targets(&project.0, &files, false) {
            Resolution::Targets(targets) => {
                assert_eq!(targets, vec![PathBuf::from("tests/test_calc.py")]);
            }
            _ => panic!("a covered source file should resolve to its test"),
        }
        let files: BTreeSet<PathBuf> = [PathBuf::from("pkg/unused.py")].into_iter().collect();
        assert!(matches!(resolve_targets(&project.0, &files, false), Resolution::NoTests));
    }

    #[test]
    fn a_changed_test_runs_itself_and_config_runs_everything() {
        let project = Scratch::new("config");
        project.write("tests/test_a.py", "def test_a():\n    pass\n");
        project.write("conftest.py", "");
        let files: BTreeSet<PathBuf> = [PathBuf::from("tests/test_a.py")].into_iter().collect();
        match resolve_targets(&project.0, &files, false) {
            Resolution::Targets(targets) => assert_eq!(targets, vec![PathBuf::from("tests/test_a.py")]),
            _ => panic!("a changed test file covers itself"),
        }
        let files: BTreeSet<PathBuf> = [PathBuf::from("pyproject.toml")].into_iter().collect();
        assert!(matches!(resolve_targets(&project.0, &files, false), Resolution::NoTests));
        assert!(matches!(resolve_targets(&project.0, &files, true), Resolution::Full));
        let files: BTreeSet<PathBuf> = [PathBuf::from("conftest.py")].into_iter().collect();
        assert!(matches!(resolve_targets(&project.0, &files, true), Resolution::Full));
    }

    #[test]
    fn imports_are_read_with_relative_forms_and_continuations() {
        let project = Scratch::new("imports");
        project.write("pkg/one.py", "X = 1\n");
        project.write("pkg/two.py", "Y = 2\n");
        project.write(
            "tests/test_three.py",
            "from pkg import one  # a comment\nfrom pkg.two import (\n    Y,\n)\nimport pkg.one\n\ndef test_three():\n    assert one.X + Y == 3\n",
        );
        let files: BTreeSet<PathBuf> = [PathBuf::from("pkg/one.py")].into_iter().collect();
        match resolve_targets(&project.0, &files, false) {
            Resolution::Targets(targets) => assert_eq!(targets, vec![PathBuf::from("tests/test_three.py")]),
            _ => panic!("the import forms should reach the test"),
        }
        let files: BTreeSet<PathBuf> = [PathBuf::from("pkg/two.py")].into_iter().collect();
        match resolve_targets(&project.0, &files, false) {
            Resolution::Targets(targets) => assert_eq!(targets, vec![PathBuf::from("tests/test_three.py")]),
            _ => panic!("a parenthesised continuation should reach the test"),
        }
    }

    #[test]
    fn result_parsing_reads_counts_and_failures() {
        let output = "test_calc.py::test_add FAILED\nFAILED tests/test_calc.py::test_add - assert 1 == 2\n1 failed, 3 passed in 0.42s\n";
        let (status, summary, failures) = parse_result(output, 1);
        assert_eq!(status, "failed");
        assert_eq!(summary, "1 failed, 3 passed in 0.42s");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].nodeid, "tests/test_calc.py::test_add");
        assert_eq!(failures[0].summary, "assert 1 == 2");
        let (status, summary, _) = parse_result("4 passed in 0.10s\n", 0);
        assert_eq!(status, "passed");
        assert_eq!(summary, "4 passed in 0.10s");
        let (status, _, _) = parse_result("no tests ran\n", 5);
        assert_eq!(status, "no_tests");
    }

    #[test]
    fn the_manifest_carries_the_version_this_binary_reports() {
        // One version lives in `Cargo.toml`, and the handshake reports it. `plugin.json` is read by the
        // daemon before the process has ever run, so it carries a copy: this fails the moment they drift.
        let manifest: Value = serde_json::from_str(include_str!("../plugin.json")).expect("plugin.json is JSON");
        let declared = manifest["version"].as_str().unwrap_or("");
        assert_eq!(
            declared,
            PLUGIN_VERSION,
            "plugin.json carries a copy of Cargo.toml's version: it says {declared}, the binary is {PLUGIN_VERSION}"
        );
    }

    #[test]
    fn the_switches_round_trip_through_the_settings_file() {
        let project = Scratch::new("switches");
        let path = project.0.join("bughound.json");
        let mut saved = Settings {
            interval_ms: Some(900),
            debounce_ms: Some(300),
            timeout_s: Some(30),
            full_on_config_change: Some(true),
            watch: BTreeMap::new(),
        };
        saved.watch.insert("one".to_string(), Watch { python: true, javascript: false });
        saved
            .watch
            .insert("two".to_string(), Watch { python: false, javascript: true });
        saved.write(&path).unwrap();
        let read = Settings::read(&path);
        assert_eq!(read.interval_ms, Some(900));
        assert_eq!(read.full_on_config_change, Some(true));
        assert_eq!(read.watch.get("one").copied(), Some(Watch { python: true, javascript: false }));
        assert_eq!(read.watch.get("two").copied(), Some(Watch { python: false, javascript: true }));
        let config = resolve(Given::default(), Some(read)).unwrap();
        assert!(config.full_on_config_change);
        assert_eq!(config.watch.get("two").copied(), Some(Watch { python: false, javascript: true }));
        // A folder nobody spoke for is watched for nothing at all.
        assert_eq!(config.watch.get("three").copied(), None);
    }

    #[test]
    fn saved_settings_yield_to_the_registration() {
        let saved = Settings {
            interval_ms: Some(900),
            debounce_ms: Some(900),
            timeout_s: Some(90),
            ..Settings::default()
        };
        let config = resolve(Given::default(), Some(saved)).unwrap();
        assert_eq!(config.interval_ms, 900);
        assert_eq!(config.debounce_ms, 900);
        assert_eq!(config.timeout_s, 90);
        let given = Given {
            interval_ms: Some(3000),
            ..Given::default()
        };
        let saved = Settings {
            interval_ms: Some(900),
            debounce_ms: Some(900),
            timeout_s: Some(90),
            ..Settings::default()
        };
        let config = resolve(given, Some(saved)).unwrap();
        assert_eq!(config.interval_ms, 3000, "an arg wins over the file");
        assert_eq!(config.debounce_ms, 900, "what no arg said still comes from the file");
    }

    #[test]
    fn typescript_imports_are_read_and_a_change_resolves_to_its_test() {
        let project = Scratch::new("jstsclosure");
        project.write(
            "src/calc.ts",
            "export function add(a: number, b: number) {\n    return a + b;\n}\n",
        );
        project.write(
            "src/calc.test.ts",
            "import { add } from \"./calc\";\nimport \"node:test\";\n// import { fake } from \"./fake\";\n",
        );
        project.write("src/other.ts", "export const x = 1;\n");
        project.write("tsconfig.json", "{\"compilerOptions\": {}}\n");
        // The five import shapes, a comment in the middle, and a multiline one.
        let specs = jsts_imports_of("import { a } from './a';\nimport './side';\nexport * from \"./re\";\nconst { b } = require('./req');\nconst c = import('./dyn');\n/* import { x } from './hidden'; */\nimport {\n  d\n} from './multi';\n");
        assert_eq!(specs, vec!["./a", "./side", "./re", "./req", "./dyn", "./multi"]);
        let files: BTreeSet<PathBuf> = [PathBuf::from("src/calc.ts")].into_iter().collect();
        match resolve_jsts(&project.0, &files, false) {
            Resolution::Targets(targets) => assert_eq!(targets, vec![PathBuf::from("src/calc.test.ts")]),
            _ => panic!("a covered source file should resolve to its test"),
        }
        let files: BTreeSet<PathBuf> = [PathBuf::from("src/other.ts")].into_iter().collect();
        assert!(matches!(resolve_jsts(&project.0, &files, false), Resolution::NoTests));
        let files: BTreeSet<PathBuf> = [PathBuf::from("tsconfig.json")].into_iter().collect();
        assert!(matches!(resolve_jsts(&project.0, &files, false), Resolution::NoTests));
        assert!(matches!(resolve_jsts(&project.0, &files, true), Resolution::Full));
        assert!(is_jsts_test_file(&PathBuf::from("src/calc.test.ts")));
        assert!(is_jsts_test_file(&PathBuf::from("test-utils.test.js")));
        assert!(!is_jsts_test_file(&PathBuf::from("src/calc.ts")));
        assert!(!is_jsts_test_file(&PathBuf::from("src/latest.ts")));
    }

    #[test]
    fn a_file_with_letters_outside_ascii_is_still_read() {
        // A project with a letter outside ASCII in a string, a template literal or an identifier used
        // to stop the scan: it walked bytes and then took a slice at one, which lands inside a letter.
        let text = "import { add } from './calc';\nconst title = \"Пříliš žluťoučký kůň — üblich\";\nconst glyph = `\u{1f642} ${title}`;\n// import { gone } from './gone';\nimport './side';\n";
        assert_eq!(jsts_imports_of(text), vec!["./calc", "./side"]);
        assert_eq!(
            jsts_imports_of("import x from './kůň';\nexport { x } from './\u{1f642}';"),
            vec!["./kůň", "./\u{1f642}"],
            "a specifier keeps the letters it was written with"
        );
        assert_eq!(
            strip_comments("// český\nconst a = \"…\";\n"),
            "\nconst a = \"…\";\n",
            "the text the scan reads is the file's own, not one character per byte"
        );
        assert_eq!(
            jsts_imports_of("const a = \"a\\\"b\";\nimport './after';\n"),
            vec!["./after"],
            "an escaped quote still ends nothing, and the import after it is found"
        );
        // The file that stops it need not be the one edited: the whole folder is read.
        let project = Scratch::new("unicode");
        project.write("src/calc.ts", "export const add = (a: number) => a;\n");
        project.write("src/other.ts", "const note = \"příliš žluťoučký kůň\";\nexport const x = 1;\n");
        project.write("src/calc.test.ts", "import { add } from \"./calc\";\nimport \"node:test\";\n");
        let files: BTreeSet<PathBuf> = [PathBuf::from("src/calc.ts")].into_iter().collect();
        match resolve_jsts(&project.0, &files, false) {
            Resolution::Targets(targets) => assert_eq!(targets, vec![PathBuf::from("src/calc.test.ts")]),
            _ => panic!("a letter outside ASCII elsewhere in the folder should not stop the resolution"),
        }
    }

    #[test]
    fn tap_results_and_tsc_errors_are_read_back() {
        let tap = "TAP version 13\n# Subtest: adds\nnot ok 1 - adds\n1..1\n# tests 1\n# pass 0\n# fail 1\n";
        let (status, summary, failures) = parse_tap(tap, 1);
        assert_eq!(status, "failed");
        assert_eq!(summary, "1 failing of 1 tests (0 passed)");
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].nodeid, "adds");
        let (status, _, failures) = parse_tap("# tests 2\n# pass 2\n# fail 0\n", 0);
        assert_eq!(status, "passed");
        assert!(failures.is_empty());
        let errors = parse_tsc_errors("src/calc.ts(1,5): error TS2304: Cannot find name 'x'.\nsrc/calc.ts(2,1): error TS7006: Implicit any.\nsrc/other.ts(3,2): error TS2304: Cannot find name 'y'.\n");
        assert_eq!(errors.len(), 2, "one failure per file, not per error");
        assert_eq!(errors[0].nodeid, "src/calc.ts");
        assert!(errors[0].summary.starts_with("error TS2304"));
    }

    #[test]
    fn a_long_output_keeps_its_head_and_its_tail() {
        let mut text = String::from("first line\n");
        for i in 0..10000 {
            text.push_str(&format!("line {i}\n"));
        }
        text.push_str("2 failed, 3 passed in 0.42s\n");
        let kept = truncate_output(&text);
        assert!(kept.starts_with("first line"), "the head survives");
        assert!(kept.contains("… [output truncated] …"), "the cut is marked");
        assert!(kept.ends_with("2 failed, 3 passed in 0.42s\n"), "and so does the summary in the tail");
        assert!(kept.len() <= OUTPUT_CAP + 64, "the log stays bounded");
    }

    #[test]
    fn the_handshake_and_a_panel_state_are_answered_in_memory() {
        let project = Scratch::new("state");
        project.write("calc.py", "X = 1\n");
        let folder = project.0.to_string_lossy().into_owned();
        let workspaces = json!({
            "workspaces": [{ "id": "ws", "name": "ws", "folder": folder, "missing": false, "scratch": false }]
        });
        let script = Script::new(vec![
            call_frame(1, "initialize", json!({ "protocolVersion": 1 })),
            response_frame(1, workspaces),
            response_frame(2, json!({ "types": ["thread.settled"] })),
            call_frame(3, "visual.request", json!({ "workspace": "ws", "method": "state", "params": {} })),
            // Nothing is switched on yet, so the panel's run all has nothing to run and says so.
            call_frame(4, "visual.request", json!({ "workspace": "ws", "method": "run", "params": { "scope": "all" } })),
            call_frame(5, "visual.request", json!({ "workspace": "ws", "method": "set_watch", "params": { "python": true } })),
            call_frame(6, "visual.request", json!({ "workspace": "ws", "method": "run", "params": { "scope": "all" } })),
            call_frame(7, "visual.request", json!({ "workspace": "ws", "method": "zap", "params": {} })),
            json!({ "jsonrpc": "2.0", "method": "shutdown" }),
        ]);
        let settings_path = project.0.join("settings.json");
        let mut plugin = Bughound::new(script, Config::with_defaults(), settings_path.clone());
        plugin.run().unwrap();
        let sent = &plugin.transport.sent;
        let answer = |id: i64| sent.iter().find(|frame| frame.get("id") == Some(&json!(id))).cloned();
        assert_eq!(answer(1).unwrap()["result"]["name"], json!("bughound"));
        let state = answer(3).unwrap()["result"].clone();
        assert_eq!(state["workspaces"][0]["id"], json!("ws"));
        assert_eq!(state["workspaces"][0]["watched"], json!(true));
        assert_eq!(state["workspaces"][0]["watch"], json!({ "python": false, "javascript": false }));
        let refused = answer(4).unwrap()["result"].clone();
        assert_eq!(refused["queued"], json!(false), "nothing is watched until the panel says what for");
        assert_eq!(refused["off"], json!(["ws"]));
        assert_eq!(answer(5).unwrap()["result"]["watch"], json!({ "python": true, "javascript": false }));
        assert_eq!(answer(6).unwrap()["result"]["queued"], json!(true));
        assert_eq!(answer(7).unwrap()["error"]["code"], json!(METHOD_NOT_FOUND));
        let written = std::fs::read_to_string(&settings_path).unwrap();
        assert!(written.contains("\"ws\""), "the switch survives the process, in the settings file: {written}");
        assert!(written.contains("\"python\":true"), "and it is the one that was ticked: {written}");
    }

    #[test]
    fn git_worktree_list_names_are_parsed_for_each_lane() {
        let text = "worktree /repo\nHEAD 1111111111111111111111111111111111111111\nbranch refs/heads/dev\n\nworktree /repo/.agents/1e724a28\nHEAD 2222222222222222222222222222222222222222\nbranch refs/heads/chore/release-0.7.0-1e724a28\n\nworktree /repo-detached\nHEAD 3333333333333333333333333333333333333333\ndetached\n";
        let parsed = parse_worktree_list(text);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed[0].branch.as_deref(), Some("dev"));
        assert_eq!(parsed[1].branch.as_deref(), Some("chore/release-0.7.0-1e724a28"));
        assert_eq!(parsed[2].branch, None);
    }

    #[test]
    fn a_folder_is_scanned_for_the_sides_that_are_switched_on() {
        let project = Scratch::new("sides");
        project.write("pkg/calc.py", "X = 1\n");
        project.write("pyproject.toml", "[project]\nname = \"x\"\n");
        project.write("src/calc.ts", "export const x = 1;\n");
        project.write("tsconfig.json", "{}\n");
        let python = scan(&project.0, Watch { python: true, javascript: false });
        let mut seen: Vec<String> = python.keys().map(|path| path.to_string_lossy().into_owned()).collect();
        seen.sort();
        assert_eq!(seen, vec!["pkg/calc.py", "pyproject.toml"]);
        let js = scan(&project.0, Watch { python: false, javascript: true });
        let mut seen: Vec<String> = js.keys().map(|path| path.to_string_lossy().into_owned()).collect();
        seen.sort();
        assert_eq!(seen, vec!["src/calc.ts", "tsconfig.json"]);
        assert!(scan(&project.0, Watch::default()).is_empty(), "nothing switched on walks nothing");
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (config, settings_path) = match parse_given(&args) {
        Ok(given) => {
            let path = given
                .settings_path
                .clone()
                .map(PathBuf::from)
                .unwrap_or_else(default_settings_path);
            let saved = Settings::read(&path);
            (resolve(given, Some(saved)).unwrap_or_else(|_| Config::with_defaults()), path)
        }
        Err(error) => {
            eprintln!("bughound: {error}");
            std::process::exit(2);
        }
    };
    let mut plugin = Bughound::new(StdioTransport::new(), config, settings_path);
    if let Err(error) = plugin.run() {
        // A daemon that has gone away is the ordinary end of a plugin, and worth no fuss.
        if error.kind() != io::ErrorKind::BrokenPipe {
            eprintln!("bughound: {error}");
            std::process::exit(1);
        }
    }
}
