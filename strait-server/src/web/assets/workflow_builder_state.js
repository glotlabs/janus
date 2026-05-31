export function createCatalogLookup(catalog) {
  const catalogById = new Map(catalog.map((runner) => [runner.id, runner]));

  function getRunner(runnerId) {
    return catalogById.get(runnerId) || null;
  }

  function getRunnerJobs(runnerId) {
    const runner = getRunner(runnerId);
    return runner ? runner.jobs : [];
  }

  function getJobDefinition(runnerId, jobName) {
    return getRunnerJobs(runnerId).find((job) => job.name === jobName) || null;
  }

  return { getRunner, getRunnerJobs, getJobDefinition };
}

export function buildDerivedJobs(rows, getRunner) {
  return rows.map((row, index) => {
    const runnerId = row.querySelector('[data-field="runner_id"]').value.trim();
    const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value.trim();
    const runner = getRunner(runnerId);
    return {
      row,
      runner,
      jobIndex: index,
      runnerId,
      runnerJobName,
      name: runner && runnerJobName ? `${runner.name} / ${runnerJobName}` : (runnerJobName || `job-${index + 1}`)
    };
  });
}

export function inferBinding(inputName, kind, rawValue) {
  if (kind === 'artifact') {
    if (rawValue && rawValue.kind === 'source_artifact') return { mode: 'source_artifact', value: 'source.tar.gz' };
    if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {
      return { mode: 'output_artifact', value: JSON.stringify(rawValue) };
    }
    if (inputName === 'source') return { mode: 'source_artifact', value: 'source.tar.gz' };
    return { mode: 'source_artifact', value: 'source.tar.gz' };
  }
  if (kind === 'string') {
    if (rawValue && rawValue.kind === 'commit') return { mode: 'commit', value: '' };
    if (rawValue && rawValue.kind === 'branch') return { mode: 'branch', value: '' };
    if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {
      return { mode: 'output_value', value: JSON.stringify(rawValue) };
    }
    if (inputName === 'commit') return { mode: 'commit', value: '' };
    if (inputName === 'branch') return { mode: 'branch', value: '' };
    if (rawValue && rawValue.kind === 'literal') return { mode: 'literal', value: typeof rawValue.value === 'string' ? rawValue.value : '' };
    return { mode: 'literal', value: '' };
  }
  if ((kind === 'boolean' || kind === 'integer' || kind === 'json')
    && rawValue && typeof rawValue === 'object'
    && rawValue.kind === 'job_output') {
    return { mode: 'output_value', value: JSON.stringify(rawValue) };
  }
  if (kind === 'boolean') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' && rawValue.value === true ? 'true' : 'false' };
  if (kind === 'integer') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' };
  if (kind === 'json') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? JSON.stringify(rawValue.value) : '' };
  return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' };
}

export function readInputBinding(inputRow) {
  const kind = inputRow.dataset.inputKind;
  const name = inputRow.dataset.inputName;
  const modeSelect = inputRow.querySelector('[data-binding-mode]');
  const valueField = inputRow.querySelector('[data-binding-value]');
  const mode = modeSelect ? modeSelect.value : 'literal';
  if (kind === 'artifact') {
    if (mode === 'source_artifact') return [name, { kind: 'source_artifact' }];
    return [name, JSON.parse(valueField.value)];
  }
  if (kind === 'string') {
    if (mode === 'commit') return [name, { kind: 'commit' }];
    if (mode === 'branch') return [name, { kind: 'branch' }];
    if (mode === 'output_value') return [name, JSON.parse(valueField.value)];
    return [name, { kind: 'literal', value: valueField ? valueField.value : '' }];
  }
  if (mode === 'output_value') {
    return [name, JSON.parse(valueField.value)];
  }
  if (kind === 'boolean') {
    return [name, { kind: 'literal', value: valueField.value === 'true' }];
  }
  if (kind === 'integer') {
    const raw = valueField.value.trim();
    const parsed = Number.parseInt(raw, 10);
    return [name, { kind: 'literal', value: Number.isFinite(parsed) ? parsed : raw }];
  }
  if (kind === 'json') {
    const raw = valueField.value.trim();
    if (!raw) return [name, { kind: 'literal', value: {} }];
    try {
      return [name, { kind: 'literal', value: JSON.parse(raw) }];
    } catch (_error) {
      return [name, { kind: 'literal', value: raw }];
    }
  }
  return [name, { kind: 'literal', value: valueField ? valueField.value : '' }];
}

export function outputOptionsFor(currentRow, derivedJobs, expectedKind, getJobDefinition) {
  const options = [];
  const currentIndex = derivedJobs.findIndex((job) => job.row === currentRow);
  for (const job of derivedJobs) {
    if (job.row === currentRow) continue;
    if (currentIndex !== -1 && derivedJobs.indexOf(job) >= currentIndex) continue;
    const definition = getJobDefinition(job.runnerId, job.runnerJobName);
    const outputs = definition ? Object.entries(definition.outputs || {}) : [];
    for (const [outputName, outputDef] of outputs) {
      if ((outputDef.type || '') !== expectedKind) continue;
      options.push({
        value: JSON.stringify({ kind: 'job_output', job_index: job.jobIndex, output_name: outputName }),
        label: `${job.name} -> ${outputName}`
      });
    }
  }
  return options;
}

export function parseOutputBinding(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

export function findDerivedJobByIndex(derivedJobs, jobIndex) {
  return derivedJobs.find((job) => job.jobIndex === jobIndex) || null;
}

export function literalHintFor(kind, mode) {
  if (mode !== 'literal') return '';
  if (kind === 'string') return 'Enter a plain string value.';
  if (kind === 'integer') return 'Enter a signed integer like 42.';
  if (kind === 'boolean') return 'Choose true or false.';
  if (kind === 'json') return 'Enter valid non-null JSON.';
  return '';
}

export function bindingModesFor(kind) {
  if (kind === 'artifact') return [
    ['source_artifact', 'Source archive'],
    ['output_artifact', 'Output artifact']
  ];
  if (kind === 'string') return [
    ['literal', 'Literal'],
    ['output_value', 'Job output'],
    ['commit', 'Current commit'],
    ['branch', 'Current branch']
  ];
  if (kind === 'integer' || kind === 'boolean' || kind === 'json') return [
    ['literal', 'Literal'],
    ['output_value', 'Job output']
  ];
  return [['literal', 'Literal']];
}
