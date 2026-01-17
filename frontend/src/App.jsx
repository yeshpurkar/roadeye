import { useMemo, useState } from "react";

const ASSET_OPTIONS = [
  { key: "roadside_signs", label: "Roadside Signs (STOP, SPEED, etc)" },
  { key: "overhead_signs", label: "Overhead Signs" },
  { key: "mileposts", label: "Mileposts" },
  { key: "guardrails", label: "Guardrails" },
  { key: "light_poles", label: "Light Poles" },
];

const API_BASE = "http://localhost:8000"; // backend

function fileKey(f) {
  return `${f.name}_${f.size}_${f.lastModified}`;
}

export default function App() {
  const [step, setStep] = useState(1);

  const [videos, setVideos] = useState([]);
  const [assets, setAssets] = useState({
    roadside_signs: true,
    overhead_signs: true,
    mileposts: true,
    guardrails: true,
    light_poles: true,
  });

  // status per fileKey: { status, jobId, message }
  const [jobs, setJobs] = useState({}); 
  const [isStarting, setIsStarting] = useState(false);

  const selectedAssets = useMemo(
    () => Object.entries(assets).filter(([, v]) => v).map(([k]) => k),
    [assets]
  );

  // 1) Append + dedupe
  function onPickVideos(fileList) {
    const arr = Array.from(fileList || []).filter((f) => f.type.startsWith("video/"));
    if (arr.length === 0) return;

    setVideos((prev) => {
      const map = new Map(prev.map((f) => [fileKey(f), f]));
      for (const f of arr) map.set(fileKey(f), f);
      return Array.from(map.values());
    });

    // initialize job rows (Queued) if not present
    setJobs((prev) => {
      const next = { ...prev };
      for (const f of arr) {
        const k = fileKey(f);
        if (!next[k]) next[k] = { status: "Queued", jobId: null, message: "" };
      }
      return next;
    });

    setStep(2);
  }

  function removeVideo(idx) {
    setVideos((prev) => {
      const f = prev[idx];
      const k = f ? fileKey(f) : null;
      if (k) {
        setJobs((prevJobs) => {
          const next = { ...prevJobs };
          delete next[k];
          return next;
        });
      }
      return prev.filter((_, i) => i !== idx);
    });
  }

  function toggleAsset(key) {
    setAssets((prev) => ({ ...prev, [key]: !prev[key] }));
  }

  function back() {
    setStep(1);
  }

  function setJob(k, patch) {
    setJobs((prev) => ({
      ...prev,
      [k]: { ...(prev[k] || { status: "Queued", jobId: null, message: "" }), ...patch },
    }));
  }

  // 3) Connect Start to backend: create job + upload video
  async function startUpload() {
    if (videos.length === 0) return;
    if (selectedAssets.length === 0) {
      alert("Please select at least one asset type.");
      return;
    }

    setIsStarting(true);

    try {
      // Upload each video as its own job (simplest MVP)
      for (const f of videos) {
        const k = fileKey(f);

        setJob(k, { status: "Creating job…", message: "" });

        // Create job
        const createRes = await fetch(`${API_BASE}/jobs`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            assets: selectedAssets,
            filename: f.name,
          }),
        });

        if (!createRes.ok) {
          const txt = await createRes.text();
          setJob(k, { status: "Error", message: `Job create failed: ${txt}` });
          continue;
        }

        const { job_id } = await createRes.json();
        setJob(k, { status: "Uploading…", jobId: job_id });

        // Upload video
        const form = new FormData();
        form.append("file", f);

        const upRes = await fetch(`${API_BASE}/jobs/${job_id}/upload`, {
          method: "POST",
          body: form,
        });

        if (!upRes.ok) {
          const txt = await upRes.text();
          setJob(k, { status: "Error", message: `Upload failed: ${txt}`, jobId: job_id });
          continue;
        }

        setJob(k, { status: "Uploaded", message: "Ready for processing", jobId: job_id });
      }
    } catch (e) {
      alert(`Unexpected error: ${e?.message || e}`);
    } finally {
      setIsStarting(false);
    }
  }

  return (
    <div style={styles.page}>
      {step === 1 ? (
        <div style={styles.centerWrap}>
          <div style={styles.brand}>RoadEye</div>
          <div style={styles.tagline}>
            Upload roadway videos, then choose what assets to extract.
          </div>

          <label style={styles.searchBar}>
            <span style={styles.icon}>🔍</span>
            <span style={styles.placeholder}>
              Upload videos (click here or drag & drop)
            </span>
            <span style={styles.pill}>Multiple</span>

            <input
              style={styles.hiddenInput}
              type="file"
              accept="video/*"
              multiple
              onChange={(e) => onPickVideos(e.target.files)}
            />
          </label>

          <div
            style={styles.dropZone}
            onDragOver={(e) => e.preventDefault()}
            onDrop={(e) => {
              e.preventDefault();
              onPickVideos(e.dataTransfer.files);
            }}
          >
            Drag and drop videos here
          </div>

          {videos.length > 0 && (
            <div style={styles.selectedBox}>
              <div style={styles.selectedTitle}>Selected videos</div>
              <ul style={styles.list}>
                {videos.map((v, i) => (
                  <li key={fileKey(v)} style={styles.listItem}>
                    <span style={styles.fileName}>{v.name}</span>
                    <button style={styles.linkBtn} onClick={() => removeVideo(i)}>
                      remove
                    </button>
                  </li>
                ))}
              </ul>
              <button style={styles.primaryBtn} onClick={() => setStep(2)}>
                Next
              </button>
            </div>
          )}
        </div>
      ) : (
        <div style={styles.card}>
          <div style={styles.headerRow}>
            <div>
              <div style={styles.h1}>Step 2</div>
              <div style={styles.h2}>Extract the following assets:</div>
            </div>
            <button style={styles.secondaryBtn} onClick={back}>
              Back
            </button>
          </div>

          <div style={styles.section}>
            <div style={styles.miniTitle}>Videos</div>
            <ul style={styles.list}>
              {videos.map((v, i) => (
                <li key={fileKey(v)} style={styles.listItem}>
                  <span style={styles.fileName}>{v.name}</span>
                  <button style={styles.linkBtn} onClick={() => removeVideo(i)}>
                    remove
                  </button>
                </li>
              ))}
            </ul>

            <label style={styles.addMore}>
              + Add more videos
              <input
                style={styles.hiddenInput}
                type="file"
                accept="video/*"
                multiple
                onChange={(e) => onPickVideos(e.target.files)}
              />
            </label>
          </div>

          <div style={styles.section}>
            <div style={styles.miniTitle}>Assets</div>
            <div style={styles.assetGrid}>
              {ASSET_OPTIONS.map((opt) => (
                <label key={opt.key} style={styles.assetRow}>
                  <input
                    type="checkbox"
                    checked={assets[opt.key]}
                    onChange={() => toggleAsset(opt.key)}
                  />
                  <span style={{ marginLeft: 10 }}>{opt.label}</span>
                </label>
              ))}
            </div>
          </div>

          {/* 2) Status table */}
          <div style={styles.section}>
            <div style={styles.miniTitle}>Upload status</div>
            <div style={styles.statusBox}>
              {videos.length === 0 ? (
                <div style={styles.small}>No videos selected.</div>
              ) : (
                <table style={styles.table}>
                  <thead>
                    <tr>
                      <th style={styles.th}>Video</th>
                      <th style={styles.th}>Status</th>
                      <th style={styles.th}>Job</th>
                      <th style={styles.th}>Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {videos.map((v) => {
                      const k = fileKey(v);
                      const row = jobs[k] || { status: "Queued", jobId: null, message: "" };
                      return (
                        <tr key={k}>
                          <td style={styles.td}>{v.name}</td>
                          <td style={styles.td}>{row.status}</td>
                          <td style={styles.td}>{row.jobId || "-"}</td>
                          <td style={styles.td}>{row.message || "-"}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              )}
            </div>
          </div>

          <div style={styles.section}>
            <button
              style={{
                ...styles.primaryBtn,
                opacity: isStarting ? 0.7 : 1,
                cursor: isStarting ? "not-allowed" : "pointer",
              }}
              onClick={startUpload}
              disabled={videos.length === 0 || isStarting}
            >
              {isStarting ? "Starting…" : "Start (upload to backend)"}
            </button>
            <div style={styles.small}>
              This will create one job per video and upload files to your backend at {API_BASE}.
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "#fff",
    fontFamily: "system-ui, -apple-system, Segoe UI, Roboto, sans-serif",
    padding: 24,
  },
  centerWrap: {
    maxWidth: 720,
    margin: "8vh auto 0",
    textAlign: "center",
  },
  brand: { fontSize: 48, fontWeight: 700, letterSpacing: -1 },
  tagline: { marginTop: 10, color: "#555", fontSize: 16, lineHeight: 1.5 },

  searchBar: {
    marginTop: 28,
    display: "flex",
    alignItems: "center",
    gap: 12,
    padding: "14px 16px",
    borderRadius: 999,
    border: "1px solid #e6e6e6",
    boxShadow: "0 2px 10px rgba(0,0,0,0.06)",
    cursor: "pointer",
    userSelect: "none",
  },
  icon: { fontSize: 18, opacity: 0.8 },
  placeholder: { flex: 1, textAlign: "left", color: "#666" },
  pill: {
    fontSize: 12,
    padding: "6px 10px",
    borderRadius: 999,
    background: "#f3f3f3",
    color: "#444",
  },
  hiddenInput: { display: "none" },

  dropZone: {
    marginTop: 14,
    padding: 18,
    borderRadius: 16,
    border: "1px dashed #e0e0e0",
    color: "#666",
    background: "#fafafa",
  },

  selectedBox: {
    marginTop: 18,
    textAlign: "left",
    border: "1px solid #eee",
    borderRadius: 16,
    padding: 16,
    background: "#fff",
  },
  selectedTitle: { fontWeight: 600, marginBottom: 8 },

  card: {
    maxWidth: 820,
    margin: "4vh auto 0",
    border: "1px solid #eee",
    borderRadius: 18,
    padding: 18,
    boxShadow: "0 8px 24px rgba(0,0,0,0.06)",
  },
  headerRow: { display: "flex", justifyContent: "space-between", alignItems: "center" },
  h1: { fontSize: 14, color: "#666", fontWeight: 600 },
  h2: { fontSize: 22, fontWeight: 700, marginTop: 4 },

  section: { marginTop: 16 },
  miniTitle: { fontWeight: 700, marginBottom: 10 },

  assetGrid: { display: "grid", gap: 10 },
  assetRow: {
    display: "flex",
    alignItems: "center",
    padding: 12,
    border: "1px solid #eee",
    borderRadius: 14,
    background: "#fafafa",
  },

  list: { listStyle: "none", padding: 0, margin: 0 },
  listItem: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "10px 0",
    borderBottom: "1px solid #f0f0f0",
  },
  fileName: { color: "#222" },
  linkBtn: {
    border: "none",
    background: "transparent",
    color: "#1a73e8",
    cursor: "pointer",
    padding: 6,
  },

  addMore: {
    display: "inline-flex",
    marginTop: 10,
    padding: "10px 12px",
    borderRadius: 12,
    border: "1px solid #e6e6e6",
    background: "#fff",
    cursor: "pointer",
    userSelect: "none",
    fontWeight: 600,
  },

  primaryBtn: {
    marginTop: 12,
    padding: "12px 16px",
    borderRadius: 12,
    border: "none",
    cursor: "pointer",
    background: "#000",
    color: "#fff",
    fontSize: 15,
    fontWeight: 600,
  },
  secondaryBtn: {
    padding: "10px 14px",
    borderRadius: 12,
    border: "1px solid #e6e6e6",
    background: "#fff",
    cursor: "pointer",
    fontWeight: 600,
  },
  small: { marginTop: 10, color: "#666", fontSize: 13, lineHeight: 1.4 },

  statusBox: {
    border: "1px solid #eee",
    borderRadius: 12,
    padding: 12,
    overflowX: "auto",
    background: "#fff",
  },
  table: { width: "100%", borderCollapse: "collapse" },
  th: { textAlign: "left", fontSize: 12, color: "#666", padding: "8px 6px", borderBottom: "1px solid #eee" },
  td: { padding: "8px 6px", borderBottom: "1px solid #f3f3f3", fontSize: 13 },
};
