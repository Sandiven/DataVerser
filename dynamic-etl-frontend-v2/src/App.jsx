// src/App.jsx
import React, { useState, useEffect, useCallback } from 'react';
import { Activity, Upload as UploadIcon, GitBranch, List, LayoutDashboard, Server, AlertCircle, Search } from 'lucide-react';
import SidebarItem from './components/SidebarItem';
import DashboardView from './views/DashboardView';
import UploadView from './views/UploadView';
import SchemaView from './views/SchemaView';
import LogsView from './views/LogsView';
import QueryView from './views/QueryView';
import { uploadAPI, logsAPI, schemaAPI, metricsAPI, healthCheck } from './utils/api';

export default function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);

  const [logs, setLogs] = useState([]);
  const [currentSchema, setCurrentSchema] = useState(null);
  const [schemaHistory, setSchemaHistory] = useState([]);
  const [metrics, setMetrics] = useState({ totalRecords: 0, totalRuns: 0, successRate: 0, activeVersion: 'v1.0' });
  const [systemStatus, setSystemStatus] = useState('disconnected');
  const [loading, setLoading] = useState(true);

  const [isProcessing, setIsProcessing] = useState(false);
  const [processSteps, setProcessSteps] = useState([]);
  const [jobDetails, setJobDetails] = useState(null);
  const [cleaningStats, setCleaningStats] = useState(null);
  const [chartData, setChartData] = useState([]);

  // UPDATED FUNCTION -------------------------------
  const transformLogRow = (log) => {
      const extra = log.extra || {};

      let status = "Completed_With_Errors";
      if (log.message?.toLowerCase().includes("uploaded")) status = "Success";
      if (log.message?.toLowerCase().includes("failed")) status = "Failed";

      // Parse timestamp properly
      let timestamp;
      if (log.timestamp) {
        try {
          timestamp = typeof log.timestamp === 'string' ? new Date(log.timestamp) : log.timestamp;
        } catch {
          timestamp = new Date();
        }
      } else {
        timestamp = new Date();
      }

      return {
          id: log.id || log._id || `${log.filename || "file"}-${log.timestamp}`,
          filename: log.filename || extra.filename || "unknown",
          message: log.message || "",
          timestamp: timestamp,
          timestampString: timestamp.toLocaleString(),

          // Check both top-level and nested in extra for backward compatibility
          structured_preview: log.structured_preview || extra.structured_preview || [],
          cleaning_stats: log.cleaning_stats || extra.cleaning_stats || {},
          record_count: log.record_count || extra.record_count || 0,
          schemaVersion: log.schema_version || extra.schema_version || "v1.0",

          status,
      };
  };
  // ------------------------------------------------

  // Generate chart data from logs for last 7 days
  const generateChartData = useCallback((logs) => {
    const last7Days = [];
    const today = new Date();
    
    // Create array of last 7 days
    for (let i = 6; i >= 0; i--) {
      const date = new Date(today);
      date.setDate(date.getDate() - i);
      date.setHours(0, 0, 0, 0);
      
      const dayName = date.toLocaleDateString('en-US', { weekday: 'short' });
      const dayMonth = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      
      last7Days.push({
        date: date,
        name: `${dayName} ${dayMonth}`,
        records: 0,
        runs: 0
      });
    }

    // Group logs by date
    logs.forEach(log => {
      if (!log.timestamp) return;
      
      const logDate = new Date(log.timestamp);
      logDate.setHours(0, 0, 0, 0);
      
      // Find matching day
      const dayData = last7Days.find(day => {
        const dayDate = new Date(day.date);
        dayDate.setHours(0, 0, 0, 0);
        return dayDate.getTime() === logDate.getTime();
      });
      
      if (dayData) {
        dayData.runs += 1;
        dayData.records += log.record_count || 0;
      }
    });

    return last7Days.map(day => ({
      name: day.name,
      records: day.records,
      runs: day.runs
    }));
  }, []);

  const fetchLogs = useCallback(async () => {
    try {
      const data = await logsAPI.getAllLogs();
      const transformed = (Array.isArray(data) ? data : []).map(transformLogRow);
      setLogs(transformed);
      
      // Generate chart data from logs
      const chartDataGenerated = generateChartData(transformed);
      setChartData(chartDataGenerated);
    } catch (e) {
      console.warn('Failed to fetch logs', e);
    }
  }, [generateChartData]);

  const fetchMetricsAndSchema = useCallback(async () => {
    try {
      const metricsData = await metricsAPI.getMetrics().catch(() => ({ totalRecords: 0, totalRuns: 0, successRate: 0, activeVersion: 'v1.0' }));
      setMetrics({
        totalRecords: metricsData.totalRecords || 0,
        totalRuns: metricsData.totalRuns || 0,
        successRate: metricsData.successRate || 0,
        activeVersion: metricsData.activeVersion || 'v1.0'
      });

      const available = await schemaAPI.getAvailableSources().catch(() => ({ source_ids: [] }));
      const sourceIds = available.source_ids || [];

      if (sourceIds.length > 0) {
        const src = sourceIds[0];
        const [schemaData, historyData] = await Promise.all([
          schemaAPI.getSchema(src).catch(() => null),
          schemaAPI.getSchemaHistory(src).catch(() => ({ history: [] }))
        ]);

        if (schemaData) {
          setCurrentSchema({
            version: schemaData.version || 'v1.0',
            created_at: schemaData.created_at,
            fields: Object.entries(schemaData.schema?.fields || schemaData.schema || {}).map(([name, field]) => {
              if (typeof field === 'object' && field !== null) {
                return { name, type: field.type || 'string', nullable: field.nullable !== false, ...field };
              }
              return { name, type: typeof field === 'string' ? field : JSON.stringify(field), nullable: true };
            })
          });
        }

        if (historyData && historyData.history) {
          setSchemaHistory(historyData.history.map(h => ({
            version: h.version,
            created_at: h.created_at,
            fields: Object.entries(h.schema?.fields || h.schema || {}).map(([name, field]) => {
              if (typeof field === 'object' && field !== null) {
                return { name, type: field.type || 'string', nullable: field.nullable !== false, ...field };
              }
              return { name, type: typeof field === 'string' ? field : JSON.stringify(field), nullable: true };
            })
          })));
        }
      }
    } catch (e) {
      console.warn('Metrics/Schema fetch failed', e);
    }
  }, []);

  // Combined fetch - only refresh when not on query tab to avoid interference
  const fetchAllData = useCallback(async () => {
    // Don't refresh if we're on the query tab to avoid interfering with query execution
    if (activeTab === 'query') {
      return;
    }
    
    setLoading(true);
    try {
      const healthy = await healthCheck();
      setSystemStatus(healthy ? 'operational' : 'disconnected');

      if (healthy) {
        await Promise.all([ fetchMetricsAndSchema(), fetchLogs() ]);
      }
    } catch (e) {
      setSystemStatus('disconnected');
    } finally {
      setLoading(false);
    }
  }, [fetchLogs, fetchMetricsAndSchema, activeTab]);

  useEffect(() => {
    fetchAllData();
    // Only set up auto-refresh if not on query tab
    if (activeTab !== 'query') {
      const interval = setInterval(fetchAllData, 30000);
      return () => clearInterval(interval);
    }
  }, [fetchAllData, activeTab]);

  const addStep = (message, type = 'info') => {
    setProcessSteps(prev => [...prev, { message, type, timestamp: new Date().toLocaleTimeString() }]);
  };

  const handleFileUpload = async (file) => {
    setIsProcessing(true);
    setProcessSteps([]);
    setJobDetails({ fileName: file.name, size: (file.size / 1024).toFixed(2) });
    setCleaningStats(null);
    setActiveTab('upload');

    try {
      addStep(`Starting upload for ${file.name}...`, 'info');
      addStep(`[Upload] Sending file to backend...`, 'process');
      const response = await uploadAPI.uploadFile(file);

      addStep(`[Upload] File uploaded successfully`, 'success');
      addStep(`[Process] Processing ${response.record_count || 0} records...`, 'process');

      setCleaningStats(response.cleaning_stats || {
        nullsRemoved: 0, rowsDroppedEmpty: 0, duplicatesDropped: 0, typesCast: 0, formatsFixed: 0
      });

      addStep(`Upload completed successfully!`, 'success');
      addStep(`Records processed: ${response.record_count || 0}`, 'info');

      setIsProcessing(false);

      fetchMetricsAndSchema();
      fetchLogs();
    } catch (error) {
      console.error('Upload failed:', error);
      addStep(`Upload failed: ${error.message}`, 'error');
      setIsProcessing(false);
      setCleaningStats({
        nullsRemoved: 0, rowsDroppedEmpty: 0, duplicatesDropped: 0, typesCast: 0, formatsFixed: 0
      });
    }
  };

  const getSystemStatus = () => {
    if (systemStatus === 'operational') {
      return (
        <span className="flex items-center gap-2 text-sm text-green-600 bg-green-50 px-3 py-1 rounded-full border border-green-200">
          <Server className="h-3 w-3" /> Backend Connected
        </span>
      );
    }
    return (
      <span className="flex items-center gap-2 text-sm text-red-600 bg-red-50 px-3 py-1 rounded-full border border-red-200">
        <AlertCircle className="h-3 w-3" /> Backend Disconnected
      </span>
    );
  };

  const renderContent = () => {
    if (loading)
      return <div className="flex items-center justify-center h-64"><div className="text-slate-500">Loading data...</div></div>;

    switch (activeTab) {
      case 'dashboard':
        return <DashboardView metrics={metrics} logs={logs} chartData={chartData} />;
      case 'upload':
        return <UploadView onUpload={handleFileUpload} isProcessing={isProcessing} steps={processSteps} jobDetails={jobDetails} cleaningStats={cleaningStats} onReset={() => { setJobDetails(null); setProcessSteps([]); setCleaningStats(null); }} />;
      case 'schema':
        return <SchemaView currentSchema={currentSchema} history={schemaHistory} />;
      case 'logs':
        return <LogsView logs={logs} refreshLogs={fetchLogs} />;
      case 'query':
        return <QueryView />;
      default:
        return <DashboardView metrics={metrics} logs={logs} chartData={chartData} />;
    }
  };

  return (
    <div className="flex h-screen bg-slate-50 text-slate-900 font-sans overflow-hidden">
      <aside className={`bg-slate-900 text-white transition-all duration-300 ${isSidebarOpen ? 'w-64' : 'w-20'} flex flex-col shadow-xl`}>
        <div className="p-6 flex items-center justify-between border-b border-slate-700">
          {isSidebarOpen && (
            <div className="flex items-center gap-2 font-bold text-xl tracking-tight text-blue-400">
              <Activity className="h-6 w-6" />
              <span>DynETL</span>
            </div>
          )}
          <button onClick={() => setIsSidebarOpen(!isSidebarOpen)} className="p-1 hover:bg-slate-800 rounded">
            {isSidebarOpen ? '<' : '>'}
          </button>
        </div>

        <nav className="flex-1 py-6 space-y-2 px-3">
          <SidebarItem icon={<LayoutDashboard />} label="Dashboard" id="dashboard" active={activeTab} setTab={setActiveTab} expanded={isSidebarOpen} />
          <SidebarItem icon={<UploadIcon />} label="Ingest Data" id="upload" active={activeTab} setTab={setActiveTab} expanded={isSidebarOpen} />
          <SidebarItem icon={<GitBranch />} label="Schema Evolution" id="schema" active={activeTab} setTab={setActiveTab} expanded={isSidebarOpen} />
          <SidebarItem icon={<Search />} label="Query Data" id="query" active={activeTab} setTab={setActiveTab} expanded={isSidebarOpen} />
          <SidebarItem icon={<List />} label="ETL Logs" id="logs" active={activeTab} setTab={setActiveTab} expanded={isSidebarOpen} />
        </nav>

        <div className="p-4 border-t border-slate-700">
          <div className={`flex items-center gap-3 ${!isSidebarOpen && 'justify-center'}`}>
            <div className="h-8 w-8 rounded-full bg-blue-500 flex items-center justify-center text-xs font-bold">AU</div>
            {isSidebarOpen && (
              <div>
                <p className="text-sm font-medium">Admin User</p>
                <p className="text-xs text-slate-400">DevOps Lead</p>
              </div>
            )}
          </div>
        </div>
      </aside>

      <main className="flex-1 overflow-y-auto relative">
        <header className="bg-white h-16 border-b border-slate-200 flex items-center justify-between px-8 shadow-sm sticky top-0 z-10">
          <h1 className="text-xl font-semibold text-slate-800 capitalize">{activeTab.replace('-', ' ')}</h1>
          <div className="flex items-center gap-4">
            {getSystemStatus()}
          </div>
        </header>

        <div className="p-8 max-w-7xl mx-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}
