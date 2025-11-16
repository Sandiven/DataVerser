import React from 'react'


const SidebarItem = ({ icon, label, id, active, setTab, expanded }) => (
    <button
        onClick={() => setTab(id)}
        className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-all duration-200 group
${active === id ? 'bg-blue-600 text-white shadow-lg shadow-blue-900/50' : 'text-slate-400 hover:bg-slate-800 hover:text-white'}
`}
    >
        <div className={`${active === id ? 'text-white' : 'text-slate-400 group-hover:text-white'}`}>
            {icon}
        </div>
        {expanded && <span className="font-medium text-sm">{label}</span>}
    </button>
)


export default SidebarItem