export const initialMetrics = {
    totalRecords: 142129,
    activeVersion: 'v1.4'
}


export const chartData = [
    { name: 'Mon', records: 4100, runs: 24 },
    { name: 'Tue', records: 3200, runs: 13 },
    { name: 'Wed', records: 2150, runs: 98 },
    { name: 'Thu', records: 2780, runs: 39 },
    { name: 'Fri', records: 1890, runs: 48 },
    { name: 'Sat', records: 2390, runs: 38 },
    { name: 'Sun', records: 3490, runs: 43 },
]


export const initialLogs = [
    { id: 'RUN-4A9B1C', timestamp: '2025-11-15 14:30:12', status: 'Success', records: 1200, schemaVersion: 'v1.4', duration: '2.1s' },
    { id: 'RUN-X7Z2D8', timestamp: '2025-11-15 14:25:04', status: 'Success', records: 850, schemaVersion: 'v1.3', duration: '1.8s' },
    { id: 'RUN-F3G5H1', timestamp: '2025-11-15 14:20:55', status: 'Completed_With_Errors', records: 50, schemaVersion: 'v1.3', duration: '0.7s' },
    { id: 'RUN-L9K0P2', timestamp: '2025-11-15 14:15:33', status: 'Success', records: 3200, schemaVersion: 'v1.3', duration: '3.5s' },
    { id: 'RUN-M4N8B6', timestamp: '2025-11-15 14:10:01', status: 'Failed', records: 0, schemaVersion: 'v1.2', duration: '0.2s' },
]


export const initialSchemaHistory = [
    {
        version: 'v1.4',
        timestamp: '2025-11-15',
        fields: [
            { name: 'user_id', type: 'Integer', nullable: false },
            { name: 'username', type: 'String', nullable: false },
            { name: 'email', type: 'String (Validated)', nullable: false },
            { name: 'created_at', type: 'DateTime', nullable: true },
            { name: 'last_login', type: 'DateTime', nullable: true, new: true },
            { name: 'address', type: 'String', nullable: true },
            { name: 'metadata_tags', type: 'Array[String]', nullable: true, new: true },
        ]
    },
    {
        version: 'v1.3',
        timestamp: '2025-11-14',
        fields: [
            { name: 'user_id', type: 'Integer', nullable: false },
            { name: 'username', type: 'String', nullable: false },
            { name: 'email', type: 'String (Validated)', nullable: false },
            { name: 'created_at', type: 'DateTime', nullable: true },
            { name: 'address', type: 'String', nullable: true },
        ]
    },
    {
        version: 'v1.2',
        timestamp: '2025-11-12',
        fields: [
            { name: 'id', type: 'String', nullable: false },
            { name: 'user', type: 'String', nullable: false },
            { name: 'email', type: 'String', nullable: false },
            { name: 'joinDate', type: 'String', nullable: true },
        ]
    },
]