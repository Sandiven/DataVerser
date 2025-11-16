import React from 'react'


const StatCard = ({ title, value, icon, color }) => (
    <div className={`p-6 rounded-xl border ${color} flex items-center justify-between`}>
        <div>
            <p className="text-sm font-medium text-slate-500 mb-1">{title}</p>
            <h4 className="text-2xl font-bold text-slate-800">{value}</h4>
        </div>
        <div className="p-3 bg-white rounded-lg shadow-sm">
            {icon}
        </div>
    </div>
)


export default StatCard