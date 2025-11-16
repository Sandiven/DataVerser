// src/views/QueryView.jsx
import React, { useState, useEffect } from 'react';
import { Search, Database, Code, MessageSquare, Loader2, CheckCircle, XCircle, RefreshCw } from 'lucide-react';
import { queryAPI, schemaAPI } from '../utils/api';

const QueryView = () => {
  const [sourceIds, setSourceIds] = useState([]);
  const [selectedSourceId, setSelectedSourceId] = useState('');
  const [queryMode, setQueryMode] = useState('mongo'); // 'mongo' | 'mongo_nl' | 'semantic'
  const [queryText, setQueryText] = useState('');
  const [mongoFilter, setMongoFilter] = useState('{}');
  const [isExecuting, setIsExecuting] = useState(false);
  const [queryResult, setQueryResult] = useState(null);
  const [error, setError] = useState(null);
  const [queryHistory, setQueryHistory] = useState([]);
  const [executionAbortController, setExecutionAbortController] = useState(null);

  // Fetch available source IDs on mount
  useEffect(() => {
    const fetchSources = async () => {
      try {
        const data = await schemaAPI.getAvailableSources();
        const ids = data.source_ids || [];
        setSourceIds(ids);
        if (ids.length > 0 && !selectedSourceId) {
          setSelectedSourceId(ids[0]);
        }
      } catch (err) {
        console.error('Failed to fetch sources:', err);
      }
    };
    fetchSources();
  }, []);

  // Fetch query history when source changes
  useEffect(() => {
    if (selectedSourceId) {
      const fetchHistory = async () => {
        try {
          const data = await queryAPI.getQueriesBySource(selectedSourceId);
          setQueryHistory(data.queries || []);
        } catch (err) {
          console.error('Failed to fetch query history:', err);
        }
      };
      fetchHistory();
    }
  }, [selectedSourceId]);

  const validateMongoFilter = (filterStr) => {
    try {
      const parsed = JSON.parse(filterStr);
      if (typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error('Filter must be a JSON object');
      }
      return parsed;
    } catch (err) {
      throw new Error(`Invalid JSON filter: ${err.message}`);
    }
  };

  const handleExecuteQuery = async () => {
    if (!selectedSourceId) {
      setError('Please select a source ID');
      return;
    }

    // Prevent multiple simultaneous executions
    if (isExecuting) {
      setError('A query is already executing. Please wait.');
      return;
    }

    // Abort any previous execution
    if (executionAbortController) {
      executionAbortController.abort();
    }

    const abortController = new AbortController();
    setExecutionAbortController(abortController);

    setIsExecuting(true);
    setError(null);
    setQueryResult(null);

    try {
      let filter = null;
      let query = null;

      if (queryMode === 'mongo') {
        if (!mongoFilter.trim()) {
          filter = {};
        } else {
          filter = validateMongoFilter(mongoFilter);
        }
      } else {
        if (!queryText.trim()) {
          throw new Error('Please enter a query');
        }
        query = queryText;
      }

      const queryRequest = {
        source_id: selectedSourceId,
        mode: queryMode,
        filter: filter,
        query: query,
        async_mode: false
      };

      // Execute query with timeout and abort signal
      const executePromise = queryAPI.executeQuery(queryRequest);
      const timeoutPromise = new Promise((_, reject) => {
        const timeoutId = setTimeout(() => reject(new Error('Query execution timeout. Please try again.')), 30000);
        abortController.signal.addEventListener('abort', () => {
          clearTimeout(timeoutId);
          reject(new Error('Query execution cancelled'));
        });
      });
      
      const response = await Promise.race([executePromise, timeoutPromise]);
      
      // Check if aborted
      if (abortController.signal.aborted) {
        return;
      }
      
      // Poll for results with retry logic
      if (response.query_id) {
        const maxRetries = 10;
        const retryDelay = 500; // Start with 500ms
        let result = null;
        let lastError = null;

        for (let attempt = 0; attempt < maxRetries; attempt++) {
          // Check if aborted
          if (abortController.signal.aborted) {
            return;
          }

          try {
            result = await queryAPI.getQueryResult(response.query_id);
            
            // Check if aborted during fetch
            if (abortController.signal.aborted) {
              return;
            }
            
            // Check if result exists and has data
            if (result && (result.results !== undefined || result.result_count !== undefined)) {
              setQueryResult(result);
              
              // Refresh history
              try {
                const historyData = await queryAPI.getQueriesBySource(selectedSourceId);
                if (!abortController.signal.aborted) {
                  setQueryHistory(historyData.queries || []);
                }
              } catch (historyErr) {
                console.warn('Failed to refresh query history:', historyErr);
                // Don't fail the whole query if history refresh fails
              }
              
              return; // Success, exit retry loop
            }
          } catch (err) {
            // Check if aborted
            if (abortController.signal.aborted) {
              return;
            }

            lastError = err;
            // If it's a 404, the query might still be processing
            if (err.message && err.message.includes('404')) {
              // Wait before retrying with exponential backoff
              await new Promise(resolve => {
                const timeoutId = setTimeout(resolve, retryDelay * (attempt + 1));
                abortController.signal.addEventListener('abort', () => {
                  clearTimeout(timeoutId);
                });
              });
              continue;
            }
            // For other errors, throw immediately
            throw err;
          }
        }

        // If we exhausted retries, show the last error or a timeout message
        if (!result) {
          throw lastError || new Error('Query is taking longer than expected. Please check query history.');
        }
      }
    } catch (err) {
      // Provide more specific error messages
      let errorMessage = 'Failed to execute query';
      
      if (err.message) {
        if (err.message.includes('timeout')) {
          errorMessage = 'Query execution timeout. The query may still be processing. Please check query history.';
        } else if (err.message.includes('500') || err.message.includes('Internal Server Error')) {
          errorMessage = 'Server error occurred. Please try again in a moment.';
        } else if (err.message.includes('404')) {
          errorMessage = 'Query result not found. The query may still be processing.';
        } else if (err.message.includes('Network') || err.message.includes('fetch')) {
          errorMessage = 'Network error. Please check your connection and try again.';
        } else {
          errorMessage = err.message;
        }
      }
      
      setError(errorMessage);
      console.error('Query execution error:', err);
    } finally {
      setIsExecuting(false);
      setExecutionAbortController(null);
    }
  };

  const getModeIcon = (mode) => {
    switch (mode) {
      case 'mongo':
        return <Code className="h-4 w-4" />;
      case 'mongo_nl':
        return <MessageSquare className="h-4 w-4" />;
      case 'semantic':
        return <Search className="h-4 w-4" />;
      default:
        return <Database className="h-4 w-4" />;
    }
  };

  const getModeDescription = (mode) => {
    switch (mode) {
      case 'mongo':
        return 'Direct MongoDB filter (JSON object)';
      case 'mongo_nl':
        return 'Natural language query converted to MongoDB filter';
      case 'semantic':
        return 'Semantic search using text similarity';
      default:
        return '';
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-slate-800">Query Data</h2>
          <p className="text-slate-500">Execute queries against your ingested data</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Query Form */}
        <div className="lg:col-span-2 space-y-4">
          <div className="bg-white rounded-lg p-6 border shadow-sm">
            <h3 className="text-lg font-semibold text-slate-800 mb-4">Query Configuration</h3>

            {/* Source Selection */}
            <div className="mb-4">
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Source ID
              </label>
              <select
                value={selectedSourceId}
                onChange={(e) => setSelectedSourceId(e.target.value)}
                className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                disabled={sourceIds.length === 0}
              >
                {sourceIds.length === 0 ? (
                  <option value="">No sources available</option>
                ) : (
                  sourceIds.map((id) => (
                    <option key={id} value={id}>
                      {id}
                    </option>
                  ))
                )}
              </select>
            </div>

            {/* Query Mode Selection */}
            <div className="mb-4">
              <label className="block text-sm font-medium text-slate-700 mb-2">
                Query Mode
              </label>
              <div className="grid grid-cols-3 gap-2">
                {['mongo', 'mongo_nl', 'semantic'].map((mode) => (
                  <button
                    key={mode}
                    onClick={() => setQueryMode(mode)}
                    className={`flex items-center justify-center gap-2 px-4 py-2 rounded-lg border transition-colors ${
                      queryMode === mode
                        ? 'bg-blue-500 text-white border-blue-500'
                        : 'bg-white text-slate-700 border-slate-300 hover:bg-slate-50'
                    }`}
                  >
                    {getModeIcon(mode)}
                    <span className="text-sm font-medium capitalize">{mode.replace('_', ' ')}</span>
                  </button>
                ))}
              </div>
              <p className="text-xs text-slate-500 mt-2">{getModeDescription(queryMode)}</p>
            </div>

            {/* Query Input */}
            {queryMode === 'mongo' ? (
              <div className="mb-4">
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  MongoDB Filter (JSON)
                </label>
                <textarea
                  value={mongoFilter}
                  onChange={(e) => setMongoFilter(e.target.value)}
                  placeholder='{"field": "value"}'
                  className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                  rows={4}
                />
                <p className="text-xs text-slate-400 mt-1">Leave empty for all records</p>
              </div>
            ) : (
              <div className="mb-4">
                <label className="block text-sm font-medium text-slate-700 mb-2">
                  Query Text
                </label>
                <textarea
                  value={queryText}
                  onChange={(e) => setQueryText(e.target.value)}
                  placeholder={queryMode === 'mongo_nl' ? 'Find all records where...' : 'Search for...'}
                  className="w-full px-3 py-2 border border-slate-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  rows={4}
                />
              </div>
            )}

            {/* Execute Button */}
            <button
              onClick={handleExecuteQuery}
              disabled={isExecuting || !selectedSourceId || (queryMode !== 'mongo' && !queryText.trim())}
              className="w-full py-3 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 disabled:bg-slate-400 disabled:cursor-not-allowed flex items-center justify-center gap-2 transition-colors"
            >
              {isExecuting ? (
                <>
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Executing...
                </>
              ) : (
                <>
                  <Search className="h-4 w-4" />
                  Execute Query
                </>
              )}
            </button>

            {/* Error Display */}
            {error && (
              <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
                <XCircle className="h-5 w-5 text-red-600 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="text-sm font-medium text-red-800">Error</p>
                  <p className="text-sm text-red-600">{error}</p>
                </div>
              </div>
            )}
          </div>

          {/* Results Display */}
          {queryResult && (
            <div className="bg-white rounded-lg p-6 border shadow-sm">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold text-slate-800">Query Results</h3>
                <div className="flex items-center gap-2">
                  <CheckCircle className="h-5 w-5 text-green-600" />
                  <span className="text-sm text-slate-600">
                    {queryResult.result_count || 0} result{queryResult.result_count !== 1 ? 's' : ''}
                  </span>
                </div>
              </div>

              {queryResult.results && queryResult.results.length > 0 ? (
                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {queryResult.results.map((result, idx) => (
                    <div
                      key={idx}
                      className="p-4 border border-slate-200 rounded-lg bg-slate-50 hover:bg-slate-100 transition-colors"
                    >
                      <pre className="text-xs text-slate-700 whitespace-pre-wrap">
                        {JSON.stringify(result, null, 2)}
                      </pre>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-slate-400 text-center py-8">No results found</p>
              )}
            </div>
          )}
        </div>

        {/* Query History Sidebar */}
        <div className="space-y-4">
          <div className="bg-white rounded-lg p-4 border shadow-sm">
            <div className="flex items-center justify-between mb-4">
              <h4 className="font-semibold text-slate-700">Query History</h4>
              <button
                onClick={async () => {
                  if (selectedSourceId) {
                    try {
                      const data = await queryAPI.getQueriesBySource(selectedSourceId);
                      setQueryHistory(data.queries || []);
                    } catch (err) {
                      console.error('Failed to refresh history:', err);
                    }
                  }
                }}
                className="p-1 hover:bg-slate-100 rounded"
                title="Refresh"
              >
                <RefreshCw className="h-4 w-4 text-slate-600" />
              </button>
            </div>
            <div className="space-y-2 max-h-96 overflow-y-auto">
              {queryHistory.length === 0 ? (
                <p className="text-sm text-slate-400 text-center py-4">No query history</p>
              ) : (
                queryHistory.slice(0, 10).map((query, idx) => (
                  <div
                    key={idx}
                    className="p-3 rounded border bg-slate-50 hover:bg-slate-100 transition-colors cursor-pointer"
                    onClick={async () => {
                      try {
                        const result = await queryAPI.getQueryResult(query.query_id);
                        setQueryResult(result);
                      } catch (err) {
                        setError(`Failed to load query: ${err.message}`);
                      }
                    }}
                  >
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs font-mono text-slate-600 truncate">
                        {query.query_id.substring(0, 8)}...
                      </span>
                      <span
                        className={`text-xs px-2 py-0.5 rounded ${
                          query.status === 'done'
                            ? 'bg-green-100 text-green-700'
                            : query.status === 'queued'
                            ? 'bg-yellow-100 text-yellow-700'
                            : 'bg-slate-100 text-slate-700'
                        }`}
                      >
                        {query.status}
                      </span>
                    </div>
                    <div className="text-xs text-slate-500">
                      {getModeIcon(query.mode)}
                      <span className="ml-1 capitalize">{query.mode.replace('_', ' ')}</span>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default QueryView;

