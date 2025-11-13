import { useCallback, useEffect, useMemo, useState } from "react";
import { toast, Toaster } from "sonner";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:4000";
const CAPABILITIES = [
  "image-processing",
  "data-analysis",
  "text-processing",
  "web-scraping",
  "machine-learning",
];

type TaskStatus = "queued" | "processing" | "completed" | "failed";

interface Task {
  id: string;
  name: string;
  status: TaskStatus;
  capabilityRequired: string;
  creditCost: number;
  inputType: string;
  metadataJson: string | null;
  totalChunks: number | null;
  processedChunks: number | null;
  progress: number | null;
  createdAt: string;
  creatorId?: string | null;
  workerId?: string | null;
  assignedWorkers?: string[];
}

interface ChunkResult {
  id: string;
  taskId: string;
  chunkIndex: number;
  status: "completed" | "failed" | "skipped";
  resultText: string | null;
  createdAt?: string;
  updatedAt?: string;
}

async function fetchJSON<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed (${res.status})`);
  }
  return res.json();
}

async function listTasks(status?: string) {
  const query = status ? `?status=${encodeURIComponent(status)}` : "";
  const data = await fetchJSON<{ tasks: Task[] }>(`${API_BASE}/api/tasks${query}`);
  return data.tasks;
}

async function fetchTask(taskId: string) {
  return fetchJSON<{ task: Task; results: ChunkResult[] }>(`${API_BASE}/api/tasks/${taskId}`);
}

async function createTask(formData: FormData) {
  const res = await fetch(`${API_BASE}/api/tasks`, {
    method: "POST",
    body: formData,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || "Failed to create task");
  }
  return res.json();
}

async function claimTask(taskId: string, workerId?: string) {
  const init = {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: workerId ? JSON.stringify({ workerId }) : JSON.stringify({}),
  } as RequestInit;
  const res = await fetch(`${API_BASE}/api/tasks/${taskId}/claim`, init);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Failed to claim task ${taskId}`);
  }
  return res.json();
}

async function fetchChunkResults(taskId: string) {
  const data = await fetchJSON<{ results: ChunkResult[] }>(`${API_BASE}/api/tasks/${taskId}/results`);
  return data.results;
}

async function fetchWorkerOnline(workerId: string) {
  return fetchJSON<{ online: boolean; lastHeartbeat?: string }>(`${API_BASE}/api/worker/online/${encodeURIComponent(workerId)}`);
}

function formatStatus(status: TaskStatus) {
  switch (status) {
    case "queued":
      return "Queued";
    case "processing":
      return "Processing";
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    default:
      return status;
  }
}

function formatCapability(capability: string) {
  return capability
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function ProgressBar({ value }: { value: number | null }) {
  if (value === null || Number.isNaN(value)) return null;
  return (
    <div className="progress">
      <div className="progress-value" style={{ width: `${Math.min(100, value)}%` }} />
    </div>
  );
}

function TaskCard({ task, onSelect, onClaim, isActive }: { task: Task; onSelect?: (task: Task) => void; onClaim?: (task: Task) => void; isActive?: boolean }) {
  return (
    <div className={`task-card${isActive ? " active" : ""}`}>
      <h3 className="task-name">{task.name}</h3>
      <div className="task-header">
        <span className={`status status-${task.status}`}>{formatStatus(task.status)}</span>
        <span className="capability">{formatCapability(task.capabilityRequired)}</span>
        <span className="credits">{task.creditCost} credits</span>
      </div>
      <ProgressBar value={task.progress ?? null} />
      <div className="task-meta">
        <span>
          Created {new Date(task.createdAt).toLocaleString()} · ID: {task.id}
        </span>
        {typeof task.processedChunks === "number" && typeof task.totalChunks === "number" && (
          <span>
            {task.processedChunks}/{task.totalChunks} chunks
          </span>
        )}
      </div>
      <div className="task-actions">
        {onSelect && (
          <button className="btn" onClick={() => onSelect(task)}>
            View
          </button>
        )}
        {onClaim && (
          <button className="btn primary" onClick={() => onClaim(task)}>
            Claim
          </button>
        )}
      </div>
    </div>
  );
}

function chunkToLines(result?: string | null) {
  if (!result) return { input: "", output: "" };
  const inputMatch = result.match(/\[input\]\s*([\s\S]*?)(?:\n\[|$)/i);
  const outputMatch = result.match(/\[output\]\s*([\s\S]*)/i);
  return {
    input: inputMatch?.[1]?.trim() ?? "",
    output: outputMatch?.[1]?.trim() ?? "",
  };
}

function CustomerView({ sessionId }: { sessionId: string }) {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [results, setResults] = useState<ChunkResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [inputMode, setInputMode] = useState<"file" | "database">("file");
  const [dbUri, setDbUri] = useState("");
  const [dbName, setDbName] = useState("");
  const [dbCollection, setDbCollection] = useState("");
  const [dbAttached, setDbAttached] = useState(false);
  const [metadataJsonValue, setMetadataJsonValue] = useState("");

  const refreshTasks = async () => {
    try {
      const data = await listTasks();
      // show only tasks created by this session in the Customer "My Tasks" list
      const mine = data.filter((t) => (t.creatorId || null) === sessionId);
      setTasks(mine);
      if (selectedTask) {
        const found = data.find((t) => t.id === selectedTask.id);
        if (found) {
          setSelectedTask(found);
          const details = await fetchChunkResults(found.id);
          setResults(details);
        }
      }
    } catch (error: any) {
      toast.error(error.message || "Failed to fetch tasks");
    }
  };

  useEffect(() => {
    refreshTasks();
    const interval = setInterval(refreshTasks, 4000);
    return () => clearInterval(interval);
  }, []);

  const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = event.currentTarget;
    const formData = new FormData(form);
    const nameValue = (formData.get("name") as string | null)?.trim() ?? "";
    if (!nameValue) {
      toast.error("Please enter a task name");
      return;
    }
    formData.set("name", nameValue);
    formData.set("inputType", inputMode === "database" ? "database" : "file");
    // include creator session id so backend can associate tasks with a creator
    formData.set("creatorId", sessionId);
    const codeFile = formData.get("code") as File | null;
    if (!codeFile || !codeFile.name.endsWith(".zip")) {
      toast.error("Please upload a code.zip file containing main.js");
      return;
    }
    if (inputMode === "database") {
      if (!dbAttached) {
        toast.error("Attach the database connection before submitting");
        return;
      }
      const trimmedUri = dbUri.trim();
      const trimmedDb = dbName.trim();
      const trimmedCollection = dbCollection.trim();
      if (!trimmedUri || !trimmedDb || !trimmedCollection) {
        toast.error("Database connection fields are required");
        return;
      }
      const metadata = metadataJsonValue || JSON.stringify({
        type: "mongodb",
        uri: trimmedUri,
        database: trimmedDb,
        collection: trimmedCollection,
      });
      formData.set("metadataJson", metadata);
      formData.delete("data");
    } else {
      formData.set("metadataJson", "");
    }
    setLoading(true);
    try {
      await createTask(formData);
      toast.success("Task submitted");
      form.reset();
      setInputMode("file");
      setDbUri("");
      setDbName("");
      setDbCollection("");
      setDbAttached(false);
      setMetadataJsonValue("");
      await refreshTasks();
    } catch (error: any) {
      toast.error(error.message || "Failed to submit task");
    } finally {
      setLoading(false);
    }
  };

  const handleAttachDatabase = () => {
    const trimmedUri = dbUri.trim();
    const trimmedDb = dbName.trim();
    const trimmedCollection = dbCollection.trim();
    if (!trimmedUri || !trimmedDb || !trimmedCollection) {
      toast.error("Fill in URI, database, and collection to attach");
      return;
    }
    const metadata = {
      type: "mongodb",
      uri: trimmedUri,
      database: trimmedDb,
      collection: trimmedCollection,
    };
    setMetadataJsonValue(JSON.stringify(metadata));
    setDbAttached(true);
    toast.success("Database attached");
  };

  const switchInputMode = (mode: "file" | "database") => {
    setInputMode(mode);
    if (mode === "file") {
      setDbAttached(false);
      setMetadataJsonValue("");
    }
  };

  return (
    <div className="panel">
      <h2>Submit New Task</h2>
      <form className="card" onSubmit={handleSubmit}>
        <label>
          Task Name
          <input type="text" name="name" placeholder="Summarize invoices" maxLength={120} required />
        </label>
        <div className="tab-switch">
          <button
            type="button"
            className={`tab-button${inputMode === "file" ? " active" : ""}`}
            onClick={() => switchInputMode("file")}
          >
            Data File
          </button>
          <button
            type="button"
            className={`tab-button${inputMode === "database" ? " active" : ""}`}
            onClick={() => switchInputMode("database")}
          >
            Database
          </button>
        </div>
        <div className="grid">
          <label>
            Capability Required
            <select name="capabilityRequired" defaultValue="image-processing" required>
              {CAPABILITIES.map((cap) => (
                <option key={cap} value={cap}>
                  {formatCapability(cap)}
                </option>
              ))}
            </select>
          </label>
          <label>
            Credit Cost
            <input type="number" name="creditCost" min={1} max={100} defaultValue={10} required />
          </label>
        </div>
        <label>
          Code ZIP (must include main.js)
          <input type="file" name="code" accept=".zip" required />
        </label>
        {inputMode === "file" ? (
          <div className="tab-panel">
            <label>
              data.json (optional)
              <input type="file" name="data" accept="application/json" />
            </label>
            <small className="muted">Upload a JSON array to distribute work across chunks.</small>
          </div>
        ) : (
          <div className="tab-panel">
            <div className="grid">
              <label>
                MongoDB URI
                <input
                  type="url"
                  value={dbUri}
                  onChange={(event) => {
                    setDbUri(event.target.value);
                    setDbAttached(false);
                  }}
                  placeholder="mongodb+srv://user:pass@cluster.example"
                  required={inputMode === "database"}
                />
              </label>
              <label>
                Database Name
                <input
                  type="text"
                  value={dbName}
                  onChange={(event) => {
                    setDbName(event.target.value);
                    setDbAttached(false);
                  }}
                  placeholder="analytics"
                  required={inputMode === "database"}
                />
              </label>
            </div>
            <label>
              Collection Name
              <input
                type="text"
                value={dbCollection}
                onChange={(event) => {
                  setDbCollection(event.target.value);
                  setDbAttached(false);
                }}
                placeholder="invoices"
                required={inputMode === "database"}
              />
            </label>
            <div className="attach-row">
              <button type="button" className="btn" onClick={handleAttachDatabase}>
                Attach Database
              </button>
              {dbAttached ? <span className="success-pill">Attached</span> : <span className="muted">Fill details and click attach.</span>}
            </div>
          </div>
        )}
        <label>
          Total chunks (optional)
          <input type="number" name="totalChunks" min={1} />
        </label>
  <input type="hidden" name="inputType" value={inputMode === "database" ? "database" : "file"} readOnly />
  <input type="hidden" name="metadataJson" value={metadataJsonValue} readOnly />
        <button type="submit" disabled={loading}>
          {loading ? "Submitting..." : "Submit Task"}
        </button>
      </form>

      <div className="panel">
        <h2>My Tasks</h2>
        {tasks.length === 0 ? (
          <p className="muted">No tasks submitted yet.</p>
        ) : (
          <div className="task-grid">
            {tasks.map((task) => (
              <TaskCard
                key={task.id}
                task={task}
                onSelect={setSelectedTask}
                isActive={selectedTask?.id === task.id}
              />
            ))}
          </div>
        )}
      </div>

      {selectedTask && (
        <div className="panel">
          <h2>Task Details</h2>
          <div className="card">
            <h3 className="task-detail-name">{selectedTask.name}</h3>
            <div className="task-header">
              <span className={`status status-${selectedTask.status}`}>{formatStatus(selectedTask.status)}</span>
              <span className="capability">{formatCapability(selectedTask.capabilityRequired)}</span>
              {selectedTask.progress !== null && (
                <span>{selectedTask.progress}%</span>
              )}
            </div>
            <ProgressBar value={selectedTask.progress ?? null} />
            <div className="task-meta">
              <span>
                {selectedTask.processedChunks ?? 0}/{selectedTask.totalChunks ?? "?"} chunks processed
              </span>
              <span>Created {new Date(selectedTask.createdAt).toLocaleString()}</span>
            </div>
            <h3>Chunk Results</h3>
            {results.length === 0 ? (
              <p className="muted">No chunk results yet.</p>
            ) : (
              <table className="results">
                <thead>
                  <tr>
                    <th>Chunk</th>
                    <th>Status</th>
                    <th>Input</th>
                    <th>Output</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r) => {
                    const lines = chunkToLines(r.resultText);
                    return (
                      <tr key={r.id}>
                        <td>#{r.chunkIndex}</td>
                        <td>{r.status}</td>
                        <td><pre>{lines.input || "—"}</pre></td>
                        <td><pre>{lines.output || "—"}</pre></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function WorkerView({ sessionId }: { sessionId: string }) {
  const [availableTasks, setAvailableTasks] = useState<Task[]>([]);
  const [assignedTasks, setAssignedTasks] = useState<Task[]>([]);
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);
  const [results, setResults] = useState<ChunkResult[]>([]);
  const [searchTerm, setSearchTerm] = useState("");
  const [capabilityFilter, setCapabilityFilter] = useState("all");
  const [workerOnline, setWorkerOnline] = useState<boolean | null>(null);

  const refresh = useCallback(async () => {
    try {
      const allTasks = await listTasks();
      const available = allTasks.filter(
        (t) => t.status === "queued" && (t.assignedWorkers || []).length === 0
      );
      const mine = allTasks.filter((t) => (t.assignedWorkers || []).includes(sessionId));
      setAvailableTasks(available);
      setAssignedTasks(mine);
      setSelectedTask((prev) => {
        if (!prev) return prev;
        const updated =
          mine.find((t) => t.id === prev.id) ||
          allTasks.find((t) => t.id === prev.id) ||
          null;
        return updated || null;
      });
    } catch (error: any) {
      toast.error(error.message || "Failed to refresh tasks");
    }
  }, [sessionId]);

  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 4000);
    return () => clearInterval(interval);
  }, [refresh]);

  const checkWorkerStatus = useCallback(async () => {
    try {
      const status = await fetchWorkerOnline(sessionId);
      setWorkerOnline(status.online);
    } catch (error) {
      setWorkerOnline(false);
    }
  }, [sessionId]);

  useEffect(() => {
    checkWorkerStatus();
    const timer = setInterval(checkWorkerStatus, 5000);
    return () => clearInterval(timer);
  }, [checkWorkerStatus]);

  const normalizedSearch = searchTerm.trim().toLowerCase();
  const filteredAvailable = useMemo(() => {
    return availableTasks.filter((task) => {
      const matchesName =
        !normalizedSearch || task.name.toLowerCase().includes(normalizedSearch);
      const matchesCapability =
        capabilityFilter === "all" || task.capabilityRequired === capabilityFilter;
      return matchesName && matchesCapability;
    });
  }, [availableTasks, normalizedSearch, capabilityFilter]);

  const filteredAssigned = useMemo(() => {
    return assignedTasks.filter((task) => {
      const matchesName =
        !normalizedSearch || task.name.toLowerCase().includes(normalizedSearch);
      const matchesCapability =
        capabilityFilter === "all" || task.capabilityRequired === capabilityFilter;
      return matchesName && matchesCapability;
    });
  }, [assignedTasks, normalizedSearch, capabilityFilter]);

  useEffect(() => {
    setSelectedTask((prev) => {
      if (filteredAssigned.length === 0) {
        return null;
      }
      if (prev && filteredAssigned.some((task) => task.id === prev.id)) {
        return prev;
      }
      return filteredAssigned[0];
    });
  }, [filteredAssigned]);

  useEffect(() => {
    if (!selectedTask) {
      setResults([]);
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const data = await fetchChunkResults(selectedTask.id);
        if (!cancelled) {
          setResults(data);
        }
      } catch (error) {
        if (!cancelled) {
          console.error("Failed to load chunk results", error);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedTask?.id, selectedTask?.processedChunks, selectedTask?.status]);

  const handleClaim = async (task: Task) => {
    try {
      const status = await fetchWorkerOnline(sessionId);
      setWorkerOnline(status.online);
      if (!status.online) {
        toast.error("Start the worker process before claiming a task.");
        return;
      }
    } catch (error) {
      setWorkerOnline(false);
      toast.error("Start the worker process before claiming a task.");
      return;
    }
    try {
      const resp = await claimTask(task.id, sessionId);
      const updated = resp.task || task;
      setSelectedTask(updated);
      toast.success("Task claimed");
      await refresh();
    } catch (error: any) {
      toast.error(error.message || "Failed to claim task");
    }
  };

  return (
    <div className="panel">
      <div
        className={`worker-status ${
          workerOnline === null ? "unknown" : workerOnline ? "online" : "offline"
        }`}
      >
        {workerOnline === null ? (
          <span>Checking worker status…</span>
        ) : workerOnline ? (
          <span>Worker online</span>
        ) : (
          <span>Worker offline. Start the runner process shown below before claiming tasks.</span>
        )}
      </div>
      <div className="card task-filters">
        <div className="task-filters-group">
          <label className="sr-only" htmlFor="worker-search">
            Search by task name
          </label>
          <input
            id="worker-search"
            className="search-input"
            type="search"
            placeholder="Search tasks by name"
            value={searchTerm}
            onChange={(event) => setSearchTerm(event.target.value)}
          />
        </div>
        <div className="task-filters-group">
          <label className="sr-only" htmlFor="worker-capability-filter">
            Filter by capability
          </label>
          <select
            id="worker-capability-filter"
            className="capability-filter"
            value={capabilityFilter}
            onChange={(event) => setCapabilityFilter(event.target.value)}
          >
            <option value="all">All capabilities</option>
            {CAPABILITIES.map((cap) => (
              <option key={cap} value={cap}>
                {formatCapability(cap)}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="panel">
        <h2>Available Tasks</h2>
        {availableTasks.length === 0 ? (
          <p className="muted">No queued tasks right now.</p>
        ) : filteredAvailable.length === 0 ? (
          <p className="muted">No queued tasks match your filters.</p>
        ) : (
          <div className="task-grid">
            {filteredAvailable.map((task) => (
              <TaskCard key={task.id} task={task} onClaim={handleClaim} />
            ))}
          </div>
        )}
      </div>

      <div className="panel">
        <h2>My Assigned Tasks</h2>
        {assignedTasks.length === 0 ? (
          <p className="muted">
            No tasks assigned yet. Claim a task above to start working.
          </p>
        ) : filteredAssigned.length === 0 ? (
          <p className="muted">No assigned tasks match your filters.</p>
        ) : (
          <div className="task-grid">
            {filteredAssigned.map((task) => (
              <TaskCard
                key={task.id}
                task={task}
                onSelect={(t) => setSelectedTask(t)}
                isActive={selectedTask?.id === task.id}
              />
            ))}
          </div>
        )}
      </div>

      {selectedTask ? (
        <div className="panel">
          <h2>Task Details</h2>
          <div className="card">
            <h3 className="task-detail-name">{selectedTask.name}</h3>
            <div className="task-header">
              <span className={`status status-${selectedTask.status}`}>{formatStatus(selectedTask.status)}</span>
              <span className="capability">{formatCapability(selectedTask.capabilityRequired)}</span>
              <span className="credits">{selectedTask.creditCost} credits</span>
            </div>
            <ProgressBar value={selectedTask.progress ?? null} />
            <div className="task-meta">
              <span>
                {selectedTask.processedChunks ?? 0}/{selectedTask.totalChunks ?? "?"} chunks processed
              </span>
              <span>Task ID: {selectedTask.id}</span>
            </div>
            <div className="runner-box">
              <p>Worker service / runner</p>
              <p>
                Start a worker process (long-running). The worker will <b>only</b> process tasks that have been claimed for this worker via the UI.
              </p>
              <pre>{`WORKER_ID=${sessionId} node scripts/worker-runner.mjs`}</pre>
              <small>
                Ensure the backend is running on <code>http://localhost:4000</code>. Claim a task above to assign it to this worker.
              </small>
            </div>
            <h3>Chunk Results</h3>
            {results.length === 0 ? (
              <p className="muted">No chunk results yet.</p>
            ) : (
              <table className="results">
                <thead>
                  <tr>
                    <th>Chunk</th>
                    <th>Status</th>
                    <th>Input</th>
                    <th>Output</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r) => {
                    const lines = chunkToLines(r.resultText);
                    return (
                      <tr key={r.id}>
                        <td>#{r.chunkIndex}</td>
                        <td>{r.status}</td>
                        <td><pre>{lines.input || "—"}</pre></td>
                        <td><pre>{lines.output || "—"}</pre></td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      ) : assignedTasks.length > 0 ? (
        <p className="muted">Select a task above to inspect its chunk results.</p>
      ) : null}
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState<"customer" | "worker">("customer");
  // generate or reuse a session id for this browser instance (used as creatorId or workerId)
  const [sessionId] = useState<string>(() => {
    const key = 'shifted_session_id';
    const existing = localStorage.getItem(key);
    if (existing) return existing;
    const gen = `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2,8)}`;
    try { localStorage.setItem(key, gen); } catch (e) {}
    return gen;
  });

  return (
    <div className="app">
      <header>
        <h1>Shifted Task Network</h1>
        <p>Offline-friendly asset-backed task processing</p>
        <nav>
          <button
            className={tab === "customer" ? "active" : ""}
            onClick={() => setTab("customer")}
          >
            Customer
          </button>
          <button
            className={tab === "worker" ? "active" : ""}
            onClick={() => setTab("worker")}
          >
            Worker
          </button>
        </nav>
        <div className="session">Session: <code>{sessionId}</code></div>
      </header>
      <main>{tab === "customer" ? <CustomerView sessionId={sessionId} /> : <WorkerView sessionId={sessionId} />}</main>
      <Toaster position="top-right" richColors closeButton />
    </div>
  );
}
