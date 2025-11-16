// src/components/SchemaDiffView.jsx
import React from 'react';

const Row = ({ children, className = '' }) => (
  <div className={`flex justify-between items-center p-2 rounded ${className}`}>{children}</div>
);

export default function SchemaDiffView({ oldSchema, newSchema, diff }) {
  // diff computed by schemaUtils.diffSchemas
  const { added = [], removed = [], changed = [], unchanged = [] } = diff || {};

  return (
    <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
      <div className="bg-slate-50 px-6 py-4 border-b border-slate-200 flex justify-between items-center">
        <h3 className="font-semibold text-slate-700">Schema Diff (latest vs previous)</h3>
        <span className="text-xs text-slate-400 font-mono">Detailed changes</span>
      </div>

      <div className="p-6 space-y-4">
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">Added ({added.length})</h4>
          {added.length === 0 && <div className="text-sm text-slate-400">No new fields</div>}
          {added.map((f, idx) => (
            <Row key={idx} className="bg-green-50 border border-green-100">
              <div>
                <div className="font-mono text-sm text-slate-800">{f.name}</div>
                <div className="text-xs text-slate-500">{f.type} • {f.nullable ? 'Nullable' : 'Required'}</div>
              </div>
            </Row>
          ))}
        </div>

        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">Removed ({removed.length})</h4>
          {removed.length === 0 && <div className="text-sm text-slate-400">No removed fields</div>}
          {removed.map((f, idx) => (
            <Row key={idx} className="bg-red-50 border border-red-100">
              <div>
                <div className="font-mono text-sm text-slate-800">{f.name}</div>
                <div className="text-xs text-slate-500">{f.type}</div>
              </div>
            </Row>
          ))}
        </div>

        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">Changed ({changed.length})</h4>
          {changed.length === 0 && <div className="text-sm text-slate-400">No changed fields</div>}
          {changed.map((c, idx) => (
            <div key={idx} className="p-2 border rounded border-slate-100 mb-2">
              <div className="flex justify-between items-start">
                <div>
                  <div className="font-mono text-sm text-slate-800">{c.name}</div>
                  <div className="text-xs text-slate-500">
                    {c.before.type} → {c.after.type}
                    {" • "}
                    {c.before.nullable === c.after.nullable ? (c.before.nullable ? 'Nullable' : 'Required') : `${c.before.nullable ? 'Nullable' : 'Required'} → ${c.after.nullable ? 'Nullable' : 'Required'}`}
                  </div>
                </div>
                <div className="text-xs">
                  <span className="px-2 py-1 rounded bg-orange-50 text-orange-700 border border-orange-100">{c.diffs.join(', ')}</span>
                </div>
              </div>
            </div>
          ))}
        </div>

      </div>
    </div>
  );
}
