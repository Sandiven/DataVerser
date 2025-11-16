// src/components/FieldTimeline.jsx
import React from 'react';

export default function FieldTimeline({ fieldName, historyTimeline = [] }) {
  if (!fieldName) return null;
  return (
    <div className="bg-white rounded-xl shadow-sm border border-slate-200 p-4">
      <h4 className="font-semibold text-slate-700 mb-3">Field Timeline: <span className="font-mono text-sm">{fieldName}</span></h4>

      <div className="space-y-3">
        {historyTimeline.length === 0 && <div className="text-sm text-slate-400">No history available</div>}
        {historyTimeline.map((entry, idx) => (
          <div key={idx} className={`flex items-center justify-between p-2 rounded ${entry.present ? 'bg-white' : 'bg-slate-50'}`}>
            <div>
              <div className={`text-sm ${entry.present ? 'text-slate-800' : 'text-slate-400'}`}>
                {entry.version} {entry.created_at ? `• ${new Date(entry.created_at).toLocaleDateString()}` : ''}
              </div>
              <div className="text-xs text-slate-500">
                {entry.present ? `${entry.type} • ${entry.nullable ? 'nullable' : 'required'}` : 'absent'}
              </div>
            </div>
            <div>
              <div className="text-xs text-slate-400">{entry.present ? 'present' : 'missing'}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
