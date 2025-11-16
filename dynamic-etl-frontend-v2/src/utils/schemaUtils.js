// src/utils/schemaUtils.js
export function normalizeFieldsArray(fields = []) {
  // ensure consistent internal shape: { name, type, nullable, new? }
  return (fields || []).map(f => ({
    name: f.name ?? String(f.field ?? '').trim(),
    type: (f.type ?? 'unknown').toString(),
    nullable: f.nullable !== false,
    ...(f.new ? { new: true } : {})
  }));
}

export function buildFieldMap(fields = []) {
  const map = {};
  normalizeFieldsArray(fields).forEach(f => {
    map[f.name] = { ...f };
  });
  return map;
}

/**
 * diffSchemas(oldSchema, newSchema)
 * returns { added:[], removed:[], changed:[], unchanged:[] }
 * changed entry example:
 * { name, before: {type, nullable}, after: {type, nullable}, diffs: ['type','nullable'] }
 */
export function diffSchemas(oldSchema = { fields: [] }, newSchema = { fields: [] }) {
  const oldMap = buildFieldMap(oldSchema.fields);
  const newMap = buildFieldMap(newSchema.fields);

  const added = [];
  const removed = [];
  const changed = [];
  const unchanged = [];

  const allNames = Array.from(new Set([...Object.keys(oldMap), ...Object.keys(newMap)]));

  allNames.forEach(name => {
    const a = oldMap[name];
    const b = newMap[name];

    if (!a && b) {
      added.push(b);
      return;
    }
    if (a && !b) {
      removed.push(a);
      return;
    }
    // both exist — compare
    const diffs = [];
    if ((a.type || '') !== (b.type || '')) diffs.push('type');
    if ((!!a.nullable) !== (!!b.nullable)) diffs.push('nullable');

    if (diffs.length) {
      changed.push({
        name,
        before: { type: a.type, nullable: a.nullable },
        after: { type: b.type, nullable: b.nullable },
        diffs
      });
    } else {
      unchanged.push(b);
    }
  });

  return { added, removed, changed, unchanged };
}

/**
 * computeStability(history)
 * history: array of schema versions oldest -> newest
 * returns score 0..100 where 100 means no changes across history
 */
export function computeStability(history = []) {
  if (!history || history.length < 2) return 100;
  let totalChecks = 0;
  let stableChecks = 0;

  for (let i = 1; i < history.length; i++) {
    const prev = history[i - 1];
    const cur = history[i];
    const diff = diffSchemas(prev, cur);
    totalChecks++;
    if (diff.added.length + diff.removed.length + diff.changed.length === 0) stableChecks++;
  }
  return Math.round((stableChecks / totalChecks) * 100);
}

/**
 * fieldTimeline(fieldName, history)
 * returns timeline array ordered oldest -> newest:
 * [{ version, created_at, present: bool, type, nullable }]
 */
export function fieldTimeline(fieldName, history = []) {
  return (history || []).map(s => {
    const fields = s.schema?.fields || s.schema || [];
    const map = buildFieldMap(fields);
    const f = map[fieldName];
    return {
      version: s.version || s?.version || s?.id || 'v?',
      created_at: s.created_at || s?.timestamp || null,
      present: !!f,
      type: f?.type || null,
      nullable: f ? !!f.nullable : null
    };
  });
}
