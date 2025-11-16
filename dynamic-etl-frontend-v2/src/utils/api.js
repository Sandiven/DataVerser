// src/utils/api.js
const API_BASE_URL = '/api';

async function fetchAPI(endpoint, options = {}) {
  const url = `${API_BASE_URL}${endpoint}`;
  const defaultOptions = {
    headers: {
      'Content-Type': 'application/json',
      ...options.headers,
    },
    ...options,
  };

  try {
    const response = await fetch(url, defaultOptions);
    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ detail: response.statusText }));
      throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
    }
    return await response.json();
  } catch (err) {
    console.error(`API Error (${endpoint}):`, err);
    throw err;
  }
}

export const uploadAPI = {
  uploadFile: async (file, sourceId = null, version = null) => {
    const formData = new FormData();
    formData.append('file', file);
    if (sourceId) formData.append('source_id', sourceId);
    if (version) formData.append('version', version);

    const response = await fetch(`${API_BASE_URL}/upload/`, {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ detail: response.statusText }));
      throw new Error(errorData.detail || `Upload failed: ${response.status}`);
    }
    return await response.json();
  },
};

export const logsAPI = {
  // Returns all logs (backend currently exposes /logs/)
  // Client does filtering/pagination
  getAllLogs: async () => {
    return await fetchAPI('/logs/');
  },

  // Shortcut to get logs for a single filename (backend provides /logs/{filename})
  getLogsByFilename: async (filename) => {
    return await fetchAPI(`/logs/${encodeURIComponent(filename)}`);
  }
};

export const schemaAPI = {
  // GET /schema/sources
  getAvailableSources: async () => {
    try {
      return await fetchAPI('/schema/sources');
    } catch {
      return { source_ids: [] };
    }
  },

  // GET /schema/latest?source_id=X
  getSchema: async (sourceId) => {
    try {
      return await fetchAPI(`/schema/latest?source_id=${encodeURIComponent(sourceId)}`);
    } catch (error) {
      if (error.message.includes('404')) return null;
      throw error;
    }
  },

  // GET /schema/history?source_id=X&limit=100
  getSchemaHistory: async (sourceId, limit = 100) => {
    try {
      const res = await fetchAPI(
        `/schema/history?source_id=${encodeURIComponent(sourceId)}&limit=${limit}`
      );
      return res.history || [];
    } catch {
      return [];
    }
  }
};

export const queryAPI = {
  executeQuery: async (queryRequest) => {
    return await fetchAPI('/queries/', { 
      method: 'POST', 
      body: JSON.stringify(queryRequest) 
    });
  },
  getQueryResult: async (queryId) => fetchAPI(`/queries/${encodeURIComponent(queryId)}`),
  getQueriesBySource: async (sourceId) => fetchAPI(`/queries/source/${encodeURIComponent(sourceId)}`),
};

export const metricsAPI = {
  getMetrics: async () => fetchAPI('/metrics/')
};

export const healthCheck = async () => {
  try {
    const response = await fetch(`${API_BASE_URL}/health`);
    return response.ok;
  } catch {
    return false;
  }
};

export default { uploadAPI, logsAPI, schemaAPI, queryAPI, metricsAPI, healthCheck };
