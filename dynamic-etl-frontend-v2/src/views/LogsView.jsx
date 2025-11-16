import React, { useState } from "react"
import { CheckCircle, AlertCircle, ChevronDown, ChevronRight } from "lucide-react"

const normalizeLog = (log) => {
    return {
        id: log._id || log.id || Math.random().toString(36).slice(2),
        timestamp: log.timestamp || "",
        filename: log.filename || log.source_id || "unknown",
        schemaVersion: log.schema_version ?? log.schemaVersion ?? "N/A",
        structured_preview: log.structured_preview || log.structured_data || log.processed_df || [],
        cleaning_stats: log.cleaning_stats || null,
        record_count: log.record_count ?? 0,

        // derive status
        status: log.message?.toLowerCase().includes("failed")
            ? "Failed"
            : "Success",
    }
}

const LogsView = ({ logs }) => {
    const [expandedRow, setExpandedRow] = useState(null)

    const toggleExpand = (id) => {
        setExpandedRow(expandedRow === id ? null : id)
    }

    const normalized = logs.map(normalizeLog)

    return (
        <div className="space-y-6">
            <div>
                <h2 className="text-2xl font-bold text-slate-800">ETL Execution Logs</h2>
                <p className="text-slate-500">Complete audit trail with structured previews.</p>
            </div>

            <div className="bg-white rounded-xl shadow-sm border border-slate-200 overflow-hidden">
                <div className="overflow-x-auto">
                    <table className="w-full text-left text-sm">
                        <thead className="bg-slate-50 text-slate-600 border-b border-slate-200">
                            <tr>
                                <th className="px-6 py-4 font-semibold">Expand</th>
                                <th className="px-6 py-4 font-semibold">Timestamp</th>
                                <th className="px-6 py-4 font-semibold">Filename</th>
                                <th className="px-6 py-4 font-semibold">Schema Ver</th>
                                <th className="px-6 py-4 font-semibold">Records</th>
                                <th className="px-6 py-4 font-semibold">Status</th>
                            </tr>
                        </thead>

                        <tbody className="divide-y divide-slate-100">
                            {normalized.length === 0 && (
                                <tr>
                                    <td colSpan="6" className="text-center p-8 text-slate-400">
                                        No logs found.
                                    </td>
                                </tr>
                            )}

                            {normalized.map((log) => (
                                <React.Fragment key={log.id}>
                                    {/* MAIN ROW */}
                                    <tr
                                        className="hover:bg-slate-50 transition-colors cursor-pointer"
                                        onClick={() => toggleExpand(log.id)}
                                    >
                                        <td className="px-6 py-4">
                                            {expandedRow === log.id ? (
                                                <ChevronDown className="h-4 w-4 text-slate-500" />
                                            ) : (
                                                <ChevronRight className="h-4 w-4 text-slate-500" />
                                            )}
                                        </td>

                                        <td className="px-6 py-4 text-slate-500">
                                            {new Date(log.timestamp).toLocaleString()}
                                        </td>

                                        <td className="px-6 py-4 text-slate-700">
                                            {log.filename}
                                        </td>

                                        <td className="px-6 py-4">
                                            <span className="px-2 py-1 bg-slate-100 rounded text-xs font-mono text-slate-600">
                                                {log.schemaVersion}
                                            </span>
                                        </td>

                                        <td className="px-6 py-4 text-slate-700">
                                            {log.record_count}
                                        </td>

                                        <td className="px-6 py-4">
                                            <span
                                                className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium
                                                ${log.status === "Success"
                                                        ? "bg-green-50 text-green-700 border border-green-100"
                                                        : "bg-red-50 text-red-700 border border-red-100"
                                                    }`}
                                            >
                                                {log.status === "Success" ? (
                                                    <CheckCircle className="h-3 w-3" />
                                                ) : (
                                                    <AlertCircle className="h-3 w-3" />
                                                )}
                                                {log.status}
                                            </span>
                                        </td>
                                    </tr>

                                    {/* EXPANDED PREVIEW */}
                                    {expandedRow === log.id && (
                                        <tr>
                                            <td colSpan="6" className="bg-slate-50 p-6">
                                                <div className="space-y-6">

                                                    {/* STRUCTURED PREVIEW */}
                                                    <div>
                                                        <h3 className="text-lg font-semibold text-slate-800 mb-2">
                                                            Structured Data Preview
                                                        </h3>

                                                        {log.structured_preview.length > 0 ? (
                                                            <pre className="bg-slate-900 text-slate-100 p-4 rounded-lg text-xs overflow-x-auto">
                                                                {JSON.stringify(
                                                                    log.structured_preview,
                                                                    null,
                                                                    2
                                                                )}
                                                            </pre>
                                                        ) : (
                                                            <p className="text-slate-500">
                                                                No preview available.
                                                            </p>
                                                        )}
                                                    </div>

                                                    {/* CLEANING STATS */}
                                                    <div>
                                                        <h3 className="text-lg font-semibold text-slate-800 mb-2">
                                                            Cleaning Statistics
                                                        </h3>

                                                        {log.cleaning_stats ? (
                                                            <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                                                                {Object.entries(log.cleaning_stats).map(([k, v]) => (
                                                                    <div
                                                                        key={k}
                                                                        className="bg-white shadow-sm border border-slate-200 rounded p-3"
                                                                    >
                                                                        <p className="text-xs uppercase text-slate-400">
                                                                            {k}
                                                                        </p>
                                                                        <p className="text-lg font-semibold text-slate-800">
                                                                            {v}
                                                                        </p>
                                                                    </div>
                                                                ))}
                                                            </div>
                                                        ) : (
                                                            <p className="text-slate-500">
                                                                No cleaning stats available.
                                                            </p>
                                                        )}
                                                    </div>
                                                </div>
                                            </td>
                                        </tr>
                                    )}
                                </React.Fragment>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    )
}

export default LogsView
