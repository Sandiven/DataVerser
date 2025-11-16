// src/components/SchemaMetrics.jsx
import React from 'react';

const StatSmall = ({ label, value, color = 'bg-slate-50' }) => (
  <div className={`p-3 rounded-md border ${color} flex items-center justify-between`}>
    <div>
      <p className="text-xs text-slate-500">{label}</p>
      <div className="text-lg font-bold text-slate-800">{value}</div>
    </div>
  </div>
);

export default function SchemaMetrics({ diffSummary = {}, stability = 100, totalFields = 0 }) {
  const { added = [], removed = [], changed = [] } = diffSummary;
  return (
    <div className="grid grid-cols-4 gap-4">
      <StatSmall label="Total Fields" value={totalFields} />
      <StatSmall label="Added" value={added.length} color="bg-green-50 border-green-200" />
      <StatSmall label="Removed" value={removed.length} color="bg-red-50 border-red-200" />
      <StatSmall label="Changed" value={changed.length} color="bg-orange-50 border-orange-200" />
      <div className="col-span-4 mt-2 p-3 rounded-md border bg-white">
        <p className="text-xs text-slate-500">Schema Stability</p>
        <div className="w-full bg-slate-100 h-2 rounded-full mt-2 overflow-hidden">
          <div style={{ width: `${stability}%` }} className="h-2 rounded-full bg-green-500" />
        </div>
        <div className="mt-2 text-xs text-slate-600">Stability score: <span className="font-medium">{stability}%</span></div>
      </div>
    </div>
  );
}
