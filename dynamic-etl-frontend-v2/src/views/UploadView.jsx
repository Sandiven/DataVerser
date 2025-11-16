import React, { useState, useRef } from 'react'
import { Upload, Sparkles, RefreshCw, Terminal, Eraser, Type, Filter, Database,Activity } from 'lucide-react'


const UploadView = ({ onUpload, isProcessing, steps, jobDetails, cleaningStats, onReset }) => {
    const [dragActive, setDragActive] = useState(false)
    const fileInputRef = useRef(null)


    const handleDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === "dragenter" || e.type === "dragover") {
            setDragActive(true);
        } else if (e.type === "dragleave") {
            setDragActive(false);
        }
    }


    const handleDrop = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragActive(false);
        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            onUpload(e.dataTransfer.files[0]);
        }
    }


    const handleChange = (e) => {
        if (e.target.files && e.target.files[0]) {
            onUpload(e.target.files[0]);
        }
    }
    if (jobDetails) {
        return (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 h-[calc(100vh-140px)]">
                <div className="flex flex-col gap-6">
                    <div>
                        <h2 className="text-2xl font-bold text-slate-800">Processing Job</h2>
                        <p className="text-slate-500">Analyzing {jobDetails.fileName} ({jobDetails.size} KB)</p>
                    </div>


                    <div className="bg-white rounded-xl border border-slate-200 shadow-sm p-6 flex-1">
                        <h3 className="font-semibold text-slate-700 flex items-center gap-2 mb-6">
                            <Sparkles className="h-5 w-5 text-purple-500" />
                            Transformation & Cleaning Metrics
                        </h3>
                        {!cleaningStats ? (
                            <div className="h-40 flex flex-col items-center justify-center text-slate-400 bg-slate-50 rounded-lg border border-dashed border-slate-200">
                                <RefreshCw className="h-8 w-8 mb-2 animate-spin opacity-50" />
                                <p className="text-sm">Processing in backend...</p>
                            </div>
                        ) : (
                            <div className="grid grid-cols-2 gap-4 animate-fade-in-up">
                                <div className="bg-red-50 p-4 rounded-lg border border-red-100">
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="text-xs font-bold text-red-600 uppercase tracking-wide">Nulls Removed</span>
                                        <Eraser className="h-4 w-4 text-red-400" />
                                    </div>
                                    <p className="text-3xl font-bold text-slate-800">{cleaningStats.nullsRemoved}</p>
                                    <p className="text-xs text-slate-500 mt-1">Fields cleaned</p>
                                </div>


                                <div className="bg-blue-50 p-4 rounded-lg border border-blue-100">
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="text-xs font-bold text-blue-600 uppercase tracking-wide">Type Inference</span>
                                        <Type className="h-4 w-4 text-blue-400" />
                                    </div>
                                    <p className="text-3xl font-bold text-slate-800">{cleaningStats.typesCast}</p>
                                    <p className="text-xs text-slate-500 mt-1">Strings → Numbers/Dates</p>
                                </div>


                                <div className="bg-orange-50 p-4 rounded-lg border border-orange-100">
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="text-xs font-bold text-orange-600 uppercase tracking-wide">Format Fixes</span>
                                        <Filter className="h-4 w-4 text-orange-400" />
                                    </div>
                                    <p className="text-3xl font-bold text-slate-800">{cleaningStats.formatsFixed}</p>
                                    <p className="text-xs text-slate-500 mt-1">Standardized patterns</p>
                                </div>

                                <div className="bg-purple-50 p-4 rounded-lg border border-purple-100">
                                    <div className="flex items-center justify-between mb-2">
                                        <span className="text-xs font-bold text-purple-600 uppercase tracking-wide">Duplicates</span>
                                        <Database className="h-4 w-4 text-purple-400" />
                                    </div>
                                    <p className="text-3xl font-bold text-slate-800">{cleaningStats.duplicatesDropped}</p>
                                    <p className="text-xs text-slate-500 mt-1">Rows dropped</p>
                                </div>
                            </div>
                        )}


                        {!isProcessing && (
                            <button
                                onClick={onReset}
                                className="mt-8 w-full py-3 bg-slate-900 text-white rounded-lg hover:bg-slate-800 transition-colors flex items-center justify-center gap-2"
                            >
                                <Upload className="h-4 w-4" /> Upload Another File
                            </button>
                        )}
                    </div>
                </div>

                <div className="bg-slate-900 rounded-2xl overflow-hidden flex flex-col shadow-2xl border border-slate-700">
                    <div className="bg-slate-800 px-4 py-3 flex items-center justify-between border-b border-slate-700">
                        <div className="flex items-center gap-2 text-slate-200">
                            <Terminal className="h-4 w-4" />
                            <span className="text-sm font-mono font-medium">ETL Pipeline Console</span>
                        </div>
                        {isProcessing ? (
                            <div className="flex items-center gap-2">
                                <span className="relative flex h-3 w-3">
                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                                    <span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
                                </span>
                                <span className="text-xs text-green-400 font-mono">PROCESSING</span>
                            </div>
                        ) : (
                            <span className="text-xs text-slate-400 font-mono">IDLE</span>
                        )}
                    </div>
                    <div className="flex-1 p-6 font-mono text-sm overflow-y-auto space-y-3 custom-scrollbar">
                        {steps.map((step, idx) => (
                            <div key={idx} className={`flex gap-3 animate-fade-in`}>
                                <span className="text-slate-500 shrink-0">[{step.timestamp}]</span>
                                <span className={`
${step.type === 'info' ? 'text-blue-400' : ''}
${step.type === 'process' ? 'text-slate-200' : ''}
${step.type === 'success' ? 'text-green-400 font-bold' : ''}
${step.type === 'error' ? 'text-red-400 font-bold' : ''}
${step.type === 'warning' ? 'text-orange-400' : ''}
`}>
                                    {step.type === 'process' && <span className="mr-2">➜</span>}
                                    {step.type === 'success' && <span className="mr-2">✔</span>}
                                    {step.message}
                                </span>
                            </div>
                        ))}
                        {isProcessing && (
                            <div className="flex gap-3 animate-fade-in">
                                <span className="text-slate-500 shrink-0">[{new Date().toLocaleTimeString()}]</span>
                                <span className="text-slate-200 flex items-center gap-2">
                                    <RefreshCw className="h-3 w-3 animate-spin" />
                                    Waiting for backend response...
                                </span>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        )
    }

    return (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 h-[calc(100vh-140px)]">
            <div className="flex flex-col">
                <div className="mb-6">
                    <h2 className="text-2xl font-bold text-slate-800">Ingest Data</h2>
                    <p className="text-slate-500">Upload raw unstructured files (JSON, CSV, HTML). The system will auto-detect the schema.</p>
                </div>


                <div
                    className={`flex-1 border-2 border-dashed rounded-2xl flex flex-col items-center justify-center p-8 transition-all duration-300 relative overflow-hidden
${dragActive ? 'border-blue-500 bg-blue-50' : 'border-slate-300 bg-slate-50'}
cursor-pointer hover:border-blue-400 hover:bg-slate-100
`}
                    onDragEnter={handleDrag}
                    onDragLeave={handleDrag}
                    onDragOver={handleDrag}
                    onDrop={handleDrop}
                    onClick={() => fileInputRef.current.click()}
                >
                    <input
                        ref={fileInputRef}
                        type="file"
                        className="hidden"
                        onChange={handleChange}
                        accept=".csv,.json,.txt,.html"
                        disabled={isProcessing}
                    />
                    <div className="bg-white p-6 rounded-full shadow-lg mb-6">
                        <Upload className={`h-12 w-12 ${dragActive ? 'text-blue-500' : 'text-slate-400'}`} />
                    </div>
                    <h3 className="text-xl font-semibold text-slate-700 mb-2">Click to upload or drag and drop</h3>
                    <p className="text-slate-500 max-w-xs text-center">
                        Supports JSON, CSV, Text, and HTML.
                    </p>
                </div>
            </div>


            <div className="bg-slate-900 rounded-2xl overflow-hidden flex flex-col shadow-2xl border border-slate-700">
                <div className="bg-slate-800 px-4 py-3 flex items-center justify-between border-b border-slate-700">
                    <div className="flex items-center gap-2 text-slate-200">
                        <Terminal className="h-4 w-4" />
                        <span className="text-sm font-mono font-medium">ETL Pipeline Console</span>
                    </div>
                    <span className="text-xs text-slate-400 font-mono">IDLE</span>
                </div>
                <div className="flex-1 flex flex-col items-center justify-center text-slate-600">
                    <Activity className="h-12 w-12 mb-4 opacity-20" />
                    <p>Waiting for input stream...</p>
                </div>
            </div>
        </div>
    )
}


export default UploadView