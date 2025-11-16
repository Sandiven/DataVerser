// src/views/DashboardView.jsx
import React from 'react';
import { ResponsiveContainer, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from 'recharts';
import { RefreshCw, CheckCircle, Database, GitBranch } from 'lucide-react';
import StatCard from '../components/StatCard';

const safeNumber = (v, fallback = 0) => {
  // Accept numbers or numeric strings; otherwise return fallback
  if (v === null || v === undefined) return fallback;
  const n = typeof v === 'number' ? v : Number(v);
  return Number.isFinite(n) ? n : fallback;
};

const DashboardView = ({ metrics = {}, logs = [], chartData = [] }) => {
  // Use safe defaults when metrics fields are missing
  const totalRuns = safeNumber(metrics.totalRuns, 0);
  const successRate = safeNumber(metrics.successRate, 0);
  const totalRecords = safeNumber(metrics.totalRecords, 0);
  const activeVersion = metrics?.activeVersion ?? '—';

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
        <StatCard
          title="Total ETL Runs"
          value={totalRuns}
          icon={<RefreshCw className="text-blue-600" />}
          color="bg-blue-50 border-blue-200"
        />
        <StatCard
          title="Success Rate"
          value={`${successRate.toFixed(1)}%`}
          icon={<CheckCircle className="text-green-600" />}
          color="bg-green-50 border-green-200"
        />
        <StatCard
          title="Total Records"
          value={totalRecords.toLocaleString()}
          icon={<Database className="text-purple-600" />}
          color="bg-purple-50 border-purple-200"
        />
        <StatCard
          title="Active Schema"
          value={activeVersion}
          icon={<GitBranch className="text-orange-600" />}
          color="bg-orange-50 border-orange-200"
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2 bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
          <h3 className="text-lg font-semibold text-slate-800 mb-6">Ingestion Volume (Last 7 Days)</h3>
          <div className="h-72">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="name" stroke="#64748b" />
                <YAxis stroke="#64748b" />
                <Tooltip contentStyle={{ borderRadius: '8px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }} />
                <Legend />
                <Line type="monotone" dataKey="records" stroke="#3b82f6" strokeWidth={3} dot={{ r: 4 }} activeDot={{ r: 8 }} name="Records Processed" />
                <Line type="monotone" dataKey="runs" stroke="#8b5cf6" strokeWidth={2} name="Pipeline Runs" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>

        <div className="bg-white p-6 rounded-xl border border-slate-200 shadow-sm">
          <h3 className="text-lg font-semibold text-slate-800 mb-4">Recent Activity</h3>
          <div className="space-y-4">
            {logs.length === 0 && <p className="text-sm text-slate-400">No logs found.</p>}
            {logs.slice(0, 5).map(log => (
              <div key={log.id} className="flex items-start gap-3 p-3 rounded-lg hover:bg-slate-50 transition-colors border border-transparent hover:border-slate-100">
                <div className={`mt-1 h-2 w-2 rounded-full ${log.status === 'Success' ? 'bg-green-500' : 'bg-red-500'}`} />
                <div className="flex-1">
                  <div className="flex justify-between items-center">
                    <p className="text-sm font-medium text-slate-900 truncate">{log.filename || log.id}</p>
                    <span className="text-xs text-slate-400 ml-2 flex-shrink-0">
                      {log.timestamp ? (log.timestamp instanceof Date ? log.timestamp.toLocaleDateString() : new Date(log.timestamp).toLocaleDateString()) : ''}
                    </span>
                  </div>
                  <p className="text-xs text-slate-500 mt-1">Processed {safeNumber(log.record_count || log.records || 0).toLocaleString()} records • {log.schemaVersion ?? '—'}</p>
                </div>
              </div>
            ))}
          </div>
          <button className="w-full mt-4 py-2 text-sm text-blue-600 font-medium hover:bg-blue-50 rounded-lg transition-colors">
            View All Logs
          </button>
        </div>
      </div>
    </div>
  );
};

export default DashboardView;
