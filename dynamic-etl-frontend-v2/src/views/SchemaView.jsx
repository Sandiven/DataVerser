// src/views/SchemaView.jsx
import React, { useMemo, useState } from "react";
import { Database, Clock, ChevronDown, ChevronUp, PlusCircle, MinusCircle, Edit3, Eye } from "lucide-react";

/**
 * Expects `currentSchema` shape:
 * {
 *   version: number|string,
 *   created_at: "...",
 *   schema: { fields: [ { path, type, nullable, example_value, confidence, etl_meta }, ... ] },
 *   schema_diff: { added: [...], removed: [...], changed: [...] }   // flexible shape supported
 * }
 *
 * If `schema_diff` is missing, we'll attempt to infer basic info.
 */

const Badge = ({ children, className = "" }) => (
  <span className={`inline-block px-2 py-0.5 rounded-md text-xs font-medium ${className}`}>{children}</span>
);

const StatCard = ({ label, value, colorClass }) => (
  <div className="bg-white rounded-lg p-4 border shadow-sm">
    <p className="text-xs text-slate-400">{label}</p>
    <div className="flex items-center gap-3">
      <h3 className="text-xl font-semibold text-slate-800">{value}</h3>
    </div>
  </div>
);

const DiffRow = ({ name, before, after }) => (
  <div className="flex items-start justify-between gap-4 p-3 rounded border bg-white">
    <div className="min-w-0">
      <div className="flex items-center gap-3">
        <p className="font-mono text-sm font-semibold truncate">{name}</p>
      </div>
      <p className="text-xs text-slate-500 mt-1">
        {before !== undefined && after !== undefined ? (
          <>
            <span className="font-mono">{String(before)}</span> → <span className="font-mono">{String(after)}</span>
          </>
        ) : before !== undefined ? (
          <span className="text-slate-500">was <span className="font-mono">{String(before)}</span></span>
        ) : after !== undefined ? (
          <span className="text-slate-500">now <span className="font-mono">{String(after)}</span></span>
        ) : (
          <span className="text-slate-500">no details</span>
        )}
      </p>
    </div>
    <div className="text-right">
      {after && <Badge className="bg-slate-100 text-slate-700">{String(after).substring(0, 24)}</Badge>}
    </div>
  </div>
);

const SchemaView = ({ currentSchema, history = [] }) => {
  const [openDetail, setOpenDetail] = useState(false);

  const schema = currentSchema?.schema || {};
  const schemaDiffRaw = currentSchema?.schema_diff ?? null;

  // Defensive parser: normalize schema_diff to object with arrays
  const schemaDiff = useMemo(() => {
    const empty = { added: [], removed: [], changed: [] };
    if (!schemaDiffRaw) return empty;

    // If shape is { added: [...], removed: [...], changed: [...] } use it.
    if (typeof schemaDiffRaw === "object" && (schemaDiffRaw.added || schemaDiffRaw.removed || schemaDiffRaw.changed)) {
      return {
        added: schemaDiffRaw.added || [],
        removed: schemaDiffRaw.removed || [],
        changed: schemaDiffRaw.changed || []
      };
    }

    // If it's a list of operations: try to split by op
    if (Array.isArray(schemaDiffRaw)) {
      const added = [], removed = [], changed = [];
      schemaDiffRaw.forEach(op => {
        const type = (op.op || op.action || "").toString().toLowerCase();
        if (type.includes("add")) added.push(op);
        else if (type.includes("remove") || type.includes("del")) removed.push(op);
        else changed.push(op);
      });
      return { added, removed, changed };
    }

    // If it's a simple object mapping, try to diff keys
    if (typeof schemaDiffRaw === "object") {
      // treat keys present only in new as added, only in old as removed, differing types as changed
      const oldKeys = schemaDiffRaw.old ? Object.keys(schemaDiffRaw.old) : [];
      const newKeys = schemaDiffRaw.new ? Object.keys(schemaDiffRaw.new) : [];
      const added = newKeys.filter(k => !oldKeys.includes(k)).map(k => ({ name: k, new: schemaDiffRaw.new?.[k] }));
      const removed = oldKeys.filter(k => !newKeys.includes(k)).map(k => ({ name: k, old: schemaDiffRaw.old?.[k] }));
      const changed = newKeys.filter(k => oldKeys.includes(k) && JSON.stringify(schemaDiffRaw.old?.[k]) !== JSON.stringify(schemaDiffRaw.new?.[k]))
        .map(k => ({ name: k, old: schemaDiffRaw.old?.[k], new: schemaDiffRaw.new?.[k] }));
      return { added, removed, changed };
    }

    return empty;
  }, [schemaDiffRaw]);

  // friendly counts
  const addedCount = schemaDiff.added.length;
  const removedCount = schemaDiff.removed.length;
  const changedCount = schemaDiff.changed.length;
  const totalFields = (schema.fields && schema.fields.length) || (schema.raw_schema && Object.keys(schema.raw_schema).length) || 0;

  // helper to render items which may be simple names or objects
  const normalizeItem = (item) => {
    if (!item) return { name: "unknown", before: undefined, after: undefined };
    if (typeof item === "string") return { name: item, before: undefined, after: undefined };
    // common shapes: { name, old, new } or { field, before, after } or { path, old, new }
    const name = item.name || item.field || item.path || item.key || item.column || item.key_name || (item.new && item.new.path) || "unknown";
    const before = item.old ?? item.before ?? (item.new && item.new.old) ?? (item.old_type ?? undefined);
    const after = item.new ?? item.after ?? (item.new && item.new.type) ?? (item.new_type ?? undefined);
    // if before/new are objects, try to read .type
    const beforeType = typeof before === "object" && before !== null ? (before.type || JSON.stringify(before)) : before;
    const afterType = typeof after === "object" && after !== null ? (after.type || JSON.stringify(after)) : after;
    return { name, before: beforeType, after: afterType };
  };

  // Render small preview of detected fields (name + type)
  const fieldPreview = (schema.fields || []).slice(0, 6).map((f, idx) => {
    if (typeof f === "string") return { name: f, type: "unknown", nullable: true };
    return {
      name: f.path || f.name || (f.etl_meta && f.etl_meta.path) || `field_${idx}`,
      type: f.type || (f.etl_meta && f.etl_meta.type) || "string",
      nullable: f.nullable !== undefined ? f.nullable : true,
      confidence: f.confidence || (f.etl_meta && f.etl_meta.confidence) || 0.6
    };
  });

  return (
    <div className="space-y-8">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Schema Evolution</h2>
          <p className="text-slate-500">Git-style diffs between schema versions — shows Added / Removed / Changed fields.</p>
        </div>
        <div className="flex items-center gap-2">
          <Badge className="bg-blue-100 text-blue-700">Current Version: {currentSchema?.version ?? "—"}</Badge>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-6 gap-6">
        <div className="lg:col-span-4 grid grid-cols-3 gap-4">
          <StatCard label="Total Fields" value={totalFields} />
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <p className="text-xs text-slate-400">Added</p>
            <div className="mt-2 flex items-center gap-2">
              <PlusCircle className="text-green-500" />
              <h3 className="text-lg font-semibold text-green-700">{addedCount}</h3>
            </div>
          </div>
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <p className="text-xs text-slate-400">Removed</p>
            <div className="mt-2 flex items-center gap-2">
              <MinusCircle className="text-red-500" />
              <h3 className="text-lg font-semibold text-red-700">{removedCount}</h3>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border shadow-sm col-span-2">
            <p className="text-xs text-slate-400">Changed</p>
            <div className="mt-2 flex items-center gap-2">
              <Edit3 className="text-orange-500" />
              <h3 className="text-lg font-semibold text-orange-700">{changedCount}</h3>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border shadow-sm col-span-3">
            <p className="text-xs text-slate-400">Detected Schema (latest)</p>
            <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3">
              {fieldPreview.length === 0 && <p className="text-sm text-slate-400">No fields detected.</p>}
              {fieldPreview.map((f, i) => (
                <div key={i} className="p-3 rounded border bg-slate-50">
                  <p className="font-mono text-sm text-slate-700 truncate">{f.name}</p>
                  <div className="flex items-center justify-between mt-2">
                    <Badge className="bg-slate-100 text-slate-700">{f.type}</Badge>
                    <span className="text-xs text-slate-400">{f.nullable ? "Nullable" : "Required"}</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        <aside className="lg:col-span-2">
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <h4 className="font-semibold text-slate-700 mb-2">Evolution History</h4>
            <div className="space-y-3">
              {history.slice(0, 8).reverse().map((h, idx) => (
                <div key={idx} className="p-3 rounded border bg-slate-50 flex items-center justify-between">
                  <div>
                    <div className="text-sm font-medium">v{h.version ?? (history.length - idx)}</div>
                    <div className="text-xs text-slate-400">{h.created_at ? new Date(h.created_at).toLocaleString() : "unknown"}</div>
                  </div>
                  <div className="text-xs text-slate-400">{(h.schema?.summary?.field_count ?? (h.schema?.fields?.length ?? 0))} fields</div>
                </div>
              ))}
              {history.length === 0 && <div className="text-sm text-slate-400">No history available.</div>}
            </div>
          </div>
        </aside>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 space-y-4">
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <div className="flex items-center justify-between mb-3">
              <h4 className="font-semibold text-slate-700">Schema Diff (latest vs previous)</h4>
              <div className="flex items-center gap-2">
                <button onClick={() => setOpenDetail(!openDetail)} className="flex items-center gap-2 text-sm text-slate-600">
                  <Eye className="h-4 w-4" /> {openDetail ? "Hide JSON" : "Detailed JSON"}
                </button>
              </div>
            </div>

            <div className="space-y-4">
              {/* ADDED */}
              <div>
                <h5 className="text-sm font-medium text-green-700 mb-2 flex items-center gap-2"><PlusCircle /> Added ({addedCount})</h5>
                {addedCount === 0 ? <p className="text-sm text-slate-400">No new fields</p> : (
                  <div className="grid gap-2">
                    {schemaDiff.added.map((item, i) => {
                      const it = normalizeItem(item);
                      return <DiffRow key={`a-${i}`} name={it.name} before={it.before} after={it.after ?? "added"} />;
                    })}
                  </div>
                )}
              </div>

              {/* REMOVED */}
              <div>
                <h5 className="text-sm font-medium text-red-700 mb-2 flex items-center gap-2"><MinusCircle /> Removed ({removedCount})</h5>
                {removedCount === 0 ? <p className="text-sm text-slate-400">No removed fields</p> : (
                  <div className="grid gap-2">
                    {schemaDiff.removed.map((item, i) => {
                      const it = normalizeItem(item);
                      return <DiffRow key={`r-${i}`} name={it.name} before={it.before ?? "removed"} after={it.after} />;
                    })}
                  </div>
                )}
              </div>

              {/* CHANGED */}
              <div>
                <h5 className="text-sm font-medium text-orange-700 mb-2 flex items-center gap-2"><Edit3 /> Changed ({changedCount})</h5>
                {changedCount === 0 ? <p className="text-sm text-slate-400">No changed fields</p> : (
                  <div className="grid gap-2">
                    {schemaDiff.changed.map((item, i) => {
                      const it = normalizeItem(item);
                      return <DiffRow key={`c-${i}`} name={it.name} before={it.before} after={it.after} />;
                    })}
                  </div>
                )}
              </div>
            </div>

            {openDetail && (
              <pre className="mt-4 max-h-72 overflow-auto rounded border bg-slate-50 p-3 text-xs text-slate-700">
                {JSON.stringify(schemaDiffRaw, null, 2)}
              </pre>
            )}
          </div>
        </div>

        <div className="space-y-4">
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <h4 className="text-sm font-semibold text-slate-700 mb-3">Change Summary</h4>
            <p className="text-sm text-slate-500">Added: <span className="font-medium text-green-700">{addedCount}</span> • Removed: <span className="font-medium text-red-700">{removedCount}</span> • Changed: <span className="font-medium text-orange-700">{changedCount}</span></p>
            <div className="mt-4">
              <p className="text-xs text-slate-400">Stability</p>
              <div className="w-full bg-slate-100 rounded h-2 mt-2">
                <div className="h-2 bg-green-500 rounded" style={{ width: `${Math.max(0, (((totalFields - (addedCount + removedCount + changedCount)) / Math.max(1, totalFields)) * 100))}%` }} />
              </div>
              <p className="text-xs text-slate-400 mt-2">Stability score: {Math.round(Math.max(0, (((totalFields - (addedCount + removedCount + changedCount)) / Math.max(1, totalFields)) * 100)))}%</p>
            </div>
          </div>

          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <h4 className="text-sm font-semibold text-slate-700 mb-2">Quick Actions</h4>
            <button className="w-full py-2 rounded bg-blue-600 text-white text-sm">Compare Full Versions</button>
            <button className="w-full mt-2 py-2 rounded border text-sm">Export Diff (JSON)</button>
          </div>
        </div>
      </div>
    </div>
  );
};

export default SchemaView;
