# Backend-Frontend Connection Guide

This guide explains how the Python FastAPI backend is connected to the React+Vite frontend.

## Setup Overview

### 1. Backend Configuration (FastAPI)

The backend has been configured with CORS middleware to allow requests from the frontend:

- **File**: `backend/app/main.py`
- **CORS Origins**: `http://localhost:5173` (Vite default), `http://localhost:3000`, `http://127.0.0.1:5173`
- **Port**: Backend runs on `http://localhost:8000` (default FastAPI/Uvicorn port)

### 2. Frontend Configuration (Vite)

The frontend has been configured with a proxy to forward API requests:

- **File**: `dynamic-etl-frontend-v2/vite.config.js`
- **Proxy**: All requests to `/api/*` are forwarded to `http://localhost:8000/*`
- **Port**: Frontend runs on `http://localhost:5173` (Vite default)

### 3. API Utility

A centralized API utility has been created for making backend requests:

- **File**: `dynamic-etl-frontend-v2/src/utils/api.js`
- Provides typed functions for all backend endpoints

## How to Run

### Start the Backend

```bash
cd backend
# Install dependencies if not already done
pip install -r requirements.txt

# Run the FastAPI server (using uvicorn)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The backend will be available at: `http://localhost:8000`
API documentation: `http://localhost:8000/docs`

### Start the Frontend

```bash
cd dynamic-etl-frontend-v2
# Install dependencies if not already done
npm install

# Run the Vite dev server
npm run dev
```

The frontend will be available at: `http://localhost:5173`

## Usage Examples

### Example 1: Upload a File

```javascript
import { uploadAPI } from './utils/api';

const handleFileUpload = async (file) => {
  try {
    const response = await uploadAPI.uploadFile(file, 'my-source-id');
    console.log('Upload successful:', response);
    // Response includes: filename, source_id, upload_id, schema_version, etc.
  } catch (error) {
    console.error('Upload failed:', error);
  }
};
```

### Example 2: Fetch Logs

```javascript
import { logsAPI } from './utils/api';

const fetchLogs = async () => {
  try {
    const logs = await logsAPI.getAllLogs();
    console.log('All logs:', logs);
  } catch (error) {
    console.error('Failed to fetch logs:', error);
  }
};
```

### Example 3: Get Schema History

```javascript
import { schemaAPI } from './utils/api';

const fetchSchemaHistory = async (sourceId) => {
  try {
    const history = await schemaAPI.getSchemaHistory(sourceId, 50);
    console.log('Schema history:', history);
  } catch (error) {
    console.error('Failed to fetch schema history:', error);
  }
};
```

### Example 4: Execute a Query

```javascript
import { queryAPI } from './utils/api';

const executeQuery = async () => {
  try {
    const queryRequest = {
      source_id: 'my-source-id',
      query: 'SELECT * FROM data WHERE condition = "value"',
      // ... other query parameters
    };
    
    const response = await queryAPI.executeQuery(queryRequest);
    console.log('Query ID:', response.query_id);
    
    // Later, fetch results
    const results = await queryAPI.getQueryResult(response.query_id);
    console.log('Query results:', results);
  } catch (error) {
    console.error('Query failed:', error);
  }
};
```

### Example 5: Using in a React Component

```javascript
import React, { useState, useEffect } from 'react';
import { logsAPI } from '../utils/api';

function LogsComponent() {
  const [logs, setLogs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchLogs = async () => {
      try {
        setLoading(true);
        const data = await logsAPI.getAllLogs();
        setLogs(data);
        setError(null);
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchLogs();
  }, []);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div>
      <h2>Logs</h2>
      <ul>
        {logs.map(log => (
          <li key={log.id}>
            {log.timestamp}: {log.message}
          </li>
        ))}
      </ul>
    </div>
  );
}

export default LogsComponent;
```

## Available API Endpoints

### Upload
- `POST /api/upload/` - Upload a file

### Logs
- `GET /api/logs/` - Get all logs
- `GET /api/logs/{filename}` - Get logs by filename

### Schema
- `GET /api/schema/?source_id={sourceId}` - Get latest schema
- `GET /api/schema/history?source_id={sourceId}&limit={limit}` - Get schema history
- `GET /api/schema-history/?filename={filename}&limit={limit}` - Get schema history by filename

### Queries
- `POST /api/queries/` - Execute a query
- `GET /api/queries/{query_id}` - Get query results
- `GET /api/queries/source/{source_id}` - Get queries by source

## Troubleshooting

### CORS Errors
If you see CORS errors, make sure:
1. The backend CORS middleware is configured correctly
2. The frontend URL matches one of the allowed origins in `backend/app/main.py`

### Connection Refused
If you see connection refused errors:
1. Make sure the backend is running on port 8000
2. Check that the Vite proxy configuration points to the correct backend URL
3. Verify both servers are running

### Proxy Not Working
If the proxy isn't working:
1. Restart the Vite dev server after changing `vite.config.js`
2. Make sure you're using `/api/` prefix in your API calls
3. Check the browser console for detailed error messages

## Testing the Connection

You can test the connection by:

1. **Backend Health Check**: Visit `http://localhost:8000/docs` to see the FastAPI interactive docs
2. **Frontend API Test**: Open browser console and run:
   ```javascript
   import { healthCheck } from './utils/api';
   healthCheck().then(console.log);
   ```

## Production Considerations

For production deployment:
1. Update CORS origins to your production frontend URL
2. Configure environment variables for API base URL
3. Set up proper error handling and retry logic
4. Consider adding authentication/authorization
5. Use environment-specific configuration files

