import {
  labelWrap,
  makeInput,
  makeSelect,
  sectionHeader,
  tableHead,
} from '/assets/workflow_builder_dom.js';

(() => {
  const list = document.getElementById('workflow-job-list');
  const addButton = document.getElementById('workflow-add-job');
  const jobsJsonField = document.getElementById('workflow-jobs-json');
  const catalog = JSON.parse(document.getElementById('workflow-runner-catalog').textContent || '[]');
  const initialJobs = JSON.parse(document.getElementById('workflow-initial-jobs').textContent || '[]');
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

  function fillRunnerOptions(select, selectedRunnerId) {
    select.replaceChildren();
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = catalog.length <= 1 ? 'Select runner' : 'Choose runner';
    placeholder.selected = !selectedRunnerId;
    select.appendChild(placeholder);
    for (const runner of catalog) {
      const option = document.createElement('option');
      option.value = runner.id;
      option.textContent = runner.name + ' (' + runner.id + ')';
      option.selected = runner.id === selectedRunnerId;
      select.appendChild(option);
    }
    if (!select.value && catalog.length === 1) {
      select.value = catalog[0].id;
    }
  }

  function takenJobNames(runnerId, currentRow) {
    const rows = [...list.querySelectorAll('[data-workflow-job-row]')];
    const currentIndex = rows.indexOf(currentRow);
    return new Set(
      rows
        .slice(0, currentIndex === -1 ? rows.length : currentIndex)
        .filter((row) => row !== currentRow)
        .map((row) => {
          const rowRunnerId = row.querySelector('[data-field="runner_id"]').value;
          const rowJobName = row.querySelector('[data-field="runner_job_name"]').value;
          return rowRunnerId === runnerId ? rowJobName : '';
        })
        .filter(Boolean)
    );
  }

  function fillJobOptions(runnerSelect, jobSelect, selectedJobName) {
    jobSelect.replaceChildren();
    const jobs = getRunnerJobs(runnerSelect.value);
    const currentRow = runnerSelect.closest('[data-workflow-job-row]');
    const taken = takenJobNames(runnerSelect.value, currentRow);
    const availableJobs = jobs.filter((job) => !taken.has(job.name) || job.name === selectedJobName);
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = availableJobs.length <= 1 ? 'Select job' : 'Choose job';
    placeholder.selected = !selectedJobName;
    jobSelect.appendChild(placeholder);
    for (const job of availableJobs) {
      const option = document.createElement('option');
      option.value = job.name;
      option.textContent = job.name;
      option.selected = job.name === selectedJobName;
      jobSelect.appendChild(option);
    }
    jobSelect.disabled = availableJobs.length === 0;
    if (!jobSelect.value && availableJobs.length === 1) {
      jobSelect.value = availableJobs[0].name;
    }
  }

  function buildDerivedJobs(rows) {
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

  function inferBinding(inputName, kind, rawValue) {
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

  function readInputBinding(inputRow) {
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

  function syncJobsJson() {
    const rows = [...list.querySelectorAll('[data-workflow-job-row]')];
    const derivedJobs = buildDerivedJobs(rows);
    const jobs = derivedJobs.map((job) => {
      const inputsMap = [...job.row.querySelectorAll('[data-input-row]')]
        .map(readInputBinding)
        .reduce((acc, [key, value]) => {
          acc[key] = value;
          return acc;
        }, {});
      return {
        runner_id: job.runnerId,
        runner_job_name: job.runnerJobName,
        inputs: inputsMap,
        allow_failure: Boolean(job.row._allowFailure)
      };
    }).filter((job) => job.runner_id || job.runner_job_name || Object.keys(job.inputs).length > 0);
    jobsJsonField.value = JSON.stringify(jobs);
    renderOutputBindingOptions(derivedJobs);
    renderOutputTable(derivedJobs);
  }

  function outputOptionsFor(currentRow, derivedJobs, expectedKind) {
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

  function parseOutputBinding(value) {
    if (!value) return null;
    try {
      return JSON.parse(value);
    } catch (_error) {
      return null;
    }
  }

  function findDerivedJobByIndex(derivedJobs, jobIndex) {
    return derivedJobs.find((job) => job.jobIndex === jobIndex) || null;
  }

  function literalHintFor(kind, mode) {
    if (mode !== 'literal') return '';
    if (kind === 'string') return 'Enter a plain string value.';
    if (kind === 'integer') return 'Enter a signed integer like 42.';
    if (kind === 'boolean') return 'Choose true or false.';
    if (kind === 'json') return 'Enter valid non-null JSON.';
    return '';
  }

  function outputBindingHint(row, inputRow, mode, derivedJobs) {
    if (mode !== 'output_artifact' && mode !== 'output_value') return '';
    const reference = inputRow.dataset.bindingValue || '';
    const kind = inputRow.dataset.inputKind;
    const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
    const options = outputOptionsFor(row, derivedJobs, expectedKind);
    if (options.length === 0) {
      return `No earlier jobs expose matching ${expectedKind} outputs yet.`;
    }
    if (!reference) return `Select a ${expectedKind} output from an earlier job.`;
    const binding = parseOutputBinding(reference);
    const sourceJob = binding ? findDerivedJobByIndex(derivedJobs, binding.job_index) : null;
    if (sourceJob) {
      return `Binding to ${sourceJob.name}.`;
    }
    return '';
  }

  function renderOutputBindingOptions(derivedJobs) {
    for (const job of derivedJobs) {
      for (const inputRow of job.row.querySelectorAll('[data-input-row]')) {
        const mode = inputRow.querySelector('[data-binding-mode]').value;
        const valueField = inputRow.querySelector('[data-binding-value]');
        const kind = inputRow.dataset.inputKind;
        if (!valueField || valueField.tagName !== 'SELECT') continue;
        if (mode !== 'output_artifact' && mode !== 'output_value') continue;
        const selected = inputRow.dataset.bindingValue || valueField.value;
        valueField.replaceChildren();
        const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
        const options = outputOptionsFor(job.row, derivedJobs, expectedKind);
        if (options.length === 0) {
          const option = document.createElement('option');
          option.value = '';
          option.textContent = `No ${expectedKind} outputs available`;
          option.selected = true;
          valueField.appendChild(option);
        }
        for (const optionData of options) {
          const option = document.createElement('option');
          option.value = optionData.value;
          option.textContent = optionData.label;
          option.selected = optionData.value === selected;
          valueField.appendChild(option);
        }
        if (!valueField.value && valueField.options.length > 0) {
          valueField.value = valueField.options[0].value;
        }
        inputRow.dataset.bindingValue = valueField.value;
      }
    }
  }

  function bindingModesFor(kind) {
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

  function buildValueField(kind, binding, row) {
    if (kind === 'artifact') {
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      if (binding.mode === 'source_artifact') {
        const option = document.createElement('option');
        option.value = 'source.tar.gz';
        option.textContent = 'source.tar.gz';
        select.appendChild(option);
      }
      row.dataset.bindingValue = binding.value || '';
      return select;
    }
    if (binding.mode === 'output_value') {
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      row.dataset.bindingValue = binding.value || '';
      return select;
    }
    if (kind === 'string' && binding.mode !== 'literal') {
      const note = document.createElement('div');
      note.className = 'muted';
      note.textContent = binding.mode === 'commit'
        ? '<commit>'
        : '<branch>';
      return note;
    }
    if (kind === 'boolean') {
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      for (const value of ['true', 'false']) {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value;
        option.selected = value === String(binding.value || 'false');
        select.appendChild(option);
      }
      return select;
    }
    if (kind === 'json') {
      const textarea = document.createElement('textarea');
      textarea.rows = 3;
      textarea.value = binding.value || '';
      textarea.setAttribute('data-binding-value', 'true');
      return textarea;
    }
    const input = makeInput(kind === 'integer' ? 'number' : 'text', binding.value || '');
    input.setAttribute('data-binding-value', 'true');
    return input;
  }

  function renderInputTable(row) {
    const derivedJobs = buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]);
    const wrap = row.querySelector('[data-inputs-wrap]');
    const summary = row.querySelector('[data-input-summary]');
    wrap.replaceChildren();
    const runnerId = row.querySelector('[data-field="runner_id"]').value;
    const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value;
    const definition = getJobDefinition(runnerId, runnerJobName);
    const inputs = definition ? Object.entries(definition.inputs || {}) : [];
    if (inputs.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'muted';
      empty.textContent = 'This runner job does not declare inputs.';
      wrap.appendChild(empty);
      if (summary) summary.textContent = 'No inputs';
      return;
    }
    if (summary) summary.textContent = `${inputs.length} input${inputs.length === 1 ? '' : 's'}`;
    const table = document.createElement('table');
    table.className = 'inputs-table';
    table.appendChild(tableHead(['Input', 'Type', 'Binding', 'Value']));
    const tbody = document.createElement('tbody');
    for (const [inputName, inputDef] of inputs) {
      const inputRow = document.createElement('tr');
      inputRow.setAttribute('data-input-row', 'true');
      inputRow.dataset.inputName = inputName;
      inputRow.dataset.inputKind = inputDef.type;
      const binding = inferBinding(inputName, inputDef.type, row._inputs[inputName]);
      inputRow.dataset.bindingValue = binding.value || '';

      const nameCell = document.createElement('td');
      const nameStrong = document.createElement('strong');
      nameStrong.textContent = inputName;
      nameCell.appendChild(nameStrong);
      if (inputDef.required) {
        const requiredBadge = document.createElement('span');
        requiredBadge.className = 'badge badge-warning';
        requiredBadge.textContent = 'required';
        nameCell.append(' ', requiredBadge);
      }
      const typeCell = document.createElement('td');
      typeCell.textContent = inputDef.type;
      const modeCell = document.createElement('td');
      const modeSelect = makeSelect();
      modeSelect.setAttribute('data-binding-mode', 'true');
      for (const [value, label] of bindingModesFor(inputDef.type)) {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = label;
        option.selected = value === binding.mode;
        modeSelect.appendChild(option);
      }
      const valueCell = document.createElement('td');
      modeCell.appendChild(modeSelect);

      function paintValueField() {
        valueCell.replaceChildren();
        const currentBinding = {
          mode: modeSelect.value,
          value: inputRow.dataset.bindingValue || binding.value || ''
        };
        const field = buildValueField(inputDef.type, currentBinding, inputRow);
        if ('value' in field) {
          field.addEventListener('input', () => {
            inputRow.dataset.bindingValue = field.value || '';
            syncJobsJson();
          });
          field.addEventListener('change', () => {
            inputRow.dataset.bindingValue = field.value || '';
            syncJobsJson();
          });
        }
        valueCell.appendChild(field);
        const hint = literalHintFor(inputDef.type, modeSelect.value)
          || outputBindingHint(row, inputRow, modeSelect.value, derivedJobs);
        if (hint) {
          const note = document.createElement('div');
          note.className = 'muted';
          note.textContent = hint;
          valueCell.appendChild(note);
        }
      }

      modeSelect.addEventListener('change', () => {
        inputRow.dataset.bindingValue = '';
        paintValueField();
        syncJobsJson();
      });
      paintValueField();

      inputRow.append(nameCell, typeCell, modeCell, valueCell);
      tbody.appendChild(inputRow);
    }
    table.appendChild(tbody);
    wrap.appendChild(table);
  }

  function renderOutputTable(derivedJobs) {
    for (const job of derivedJobs) {
      const wrap = job.row.querySelector('[data-outputs-wrap]');
      const summary = job.row.querySelector('[data-output-summary]');
      if (!wrap) continue;
      wrap.replaceChildren();
      const definition = getJobDefinition(job.runnerId, job.runnerJobName);
      const outputs = definition ? Object.entries(definition.outputs || {}) : [];
      if (summary) summary.textContent = outputs.length === 0
        ? 'No outputs'
        : `${outputs.length} output${outputs.length === 1 ? '' : 's'}`;
      if (outputs.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'muted';
        empty.textContent = 'This runner job does not declare outputs.';
        wrap.appendChild(empty);
        continue;
      }
      const table = document.createElement('table');
      table.className = 'inputs-table';
      table.appendChild(tableHead(['Output', 'Type', 'Required']));
      const tbody = document.createElement('tbody');
      for (const [outputName, outputDef] of outputs) {
        const outputRow = document.createElement('tr');
        const nameCell = document.createElement('td');
        const outputStrong = document.createElement('strong');
        outputStrong.textContent = outputName;
        nameCell.appendChild(outputStrong);
        const typeCell = document.createElement('td');
        typeCell.textContent = outputDef.type || 'unknown';
        const requiredCell = document.createElement('td');
        requiredCell.textContent = outputDef.required ? 'required' : 'optional';
        outputRow.append(nameCell, typeCell, requiredCell);
        tbody.appendChild(outputRow);
      }
      table.appendChild(tbody);
      wrap.appendChild(table);
    }
  }

  function addRow(job) {
    const row = document.createElement('fieldset');
    row.setAttribute('data-workflow-job-row', 'true');
    row.className = 'job-builder-row';
    row._inputs = job.inputs || {};
    row._allowFailure = Boolean(job.allow_failure);

    const runnerSelect = makeSelect();
    runnerSelect.setAttribute('data-field', 'runner_id');
    const jobSelect = makeSelect();
    jobSelect.setAttribute('data-field', 'runner_job_name');
    const inputSummary = document.createElement('button');
    inputSummary.type = 'button';
    inputSummary.className = 'input-summary-trigger ghost';
    inputSummary.setAttribute('data-input-summary', 'true');
    const outputSummary = document.createElement('button');
    outputSummary.type = 'button';
    outputSummary.className = 'input-summary-trigger ghost';
    outputSummary.setAttribute('data-output-summary', 'true');
    const inputsDialog = document.createElement('dialog');
    inputsDialog.className = 'inputs-dialog';
    const dialogCard = document.createElement('div');
    dialogCard.className = 'dialog-card';
    const dialogHeader = document.createElement('div');
    dialogHeader.className = 'section-head';
    const dialogHeaderText = sectionHeader(
      'Inputs',
      'Configure job inputs',
      'Bindings are saved back into the workflow when you submit the form.'
    );
    const dialogCloseTop = document.createElement('button');
    dialogCloseTop.type = 'button';
    dialogCloseTop.textContent = 'Close';
    dialogCloseTop.className = 'ghost';
    dialogHeader.append(dialogHeaderText, dialogCloseTop);
    const inputsWrap = document.createElement('div');
    inputsWrap.className = 'inputs-wrap';
    inputsWrap.setAttribute('data-inputs-wrap', 'true');
    const dialogActions = document.createElement('div');
    dialogActions.className = 'actions';
    const dialogDone = document.createElement('button');
    dialogDone.type = 'button';
    dialogDone.textContent = 'Done';
    dialogActions.appendChild(dialogDone);
    dialogCard.append(dialogHeader, inputsWrap, dialogActions);
    inputsDialog.appendChild(dialogCard);
    const outputsDialog = document.createElement('dialog');
    outputsDialog.className = 'inputs-dialog';
    const outputsDialogCard = document.createElement('div');
    outputsDialogCard.className = 'dialog-card';
    const outputsDialogHeader = document.createElement('div');
    outputsDialogHeader.className = 'section-head';
    const outputsDialogHeaderText = sectionHeader(
      'Outputs',
      'Declared job outputs',
      'Runner jobs can expose artifact or typed outputs, which downstream jobs can consume.'
    );
    const outputsDialogCloseTop = document.createElement('button');
    outputsDialogCloseTop.type = 'button';
    outputsDialogCloseTop.textContent = 'Close';
    outputsDialogCloseTop.className = 'ghost';
    outputsDialogHeader.append(outputsDialogHeaderText, outputsDialogCloseTop);
    const outputsWrap = document.createElement('div');
    outputsWrap.className = 'inputs-wrap';
    outputsWrap.setAttribute('data-outputs-wrap', 'true');
    const outputsDialogActions = document.createElement('div');
    outputsDialogActions.className = 'actions';
    const outputsDialogDone = document.createElement('button');
    outputsDialogDone.type = 'button';
    outputsDialogDone.textContent = 'Done';
    outputsDialogActions.appendChild(outputsDialogDone);
    outputsDialogCard.append(outputsDialogHeader, outputsWrap, outputsDialogActions);
    outputsDialog.appendChild(outputsDialogCard);
    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove job';
    removeButton.className = 'ghost';
    const removeButtonWrap = document.createElement('div');
    removeButtonWrap.className = 'job-row-remove';
    removeButtonWrap.appendChild(removeButton);

    fillRunnerOptions(runnerSelect, job.runner_id);
    fillJobOptions(runnerSelect, jobSelect, job.runner_job_name);

    runnerSelect.addEventListener('change', () => {
      fillJobOptions(runnerSelect, jobSelect, '');
      row._inputs = {};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
      syncJobsJson();
    });
    jobSelect.addEventListener('change', () => {
      row._inputs = {};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
      syncJobsJson();
    });
    inputSummary.addEventListener('click', () => {
      if (typeof inputsDialog.showModal === 'function') {
        inputsDialog.showModal();
      } else {
        inputsDialog.setAttribute('open', 'open');
      }
    });
    outputSummary.addEventListener('click', () => {
      if (typeof outputsDialog.showModal === 'function') {
        outputsDialog.showModal();
      } else {
        outputsDialog.setAttribute('open', 'open');
      }
    });
    for (const closeButton of [dialogCloseTop, dialogDone]) {
      closeButton.addEventListener('click', () => inputsDialog.close());
    }
    for (const closeButton of [outputsDialogCloseTop, outputsDialogDone]) {
      closeButton.addEventListener('click', () => outputsDialog.close());
    }
    removeButton.addEventListener('click', () => {
      inputsDialog.close();
      outputsDialog.close();
      row.remove();
      syncJobsJson();
    });

    row.append(
      labelWrap('Runner', runnerSelect),
      labelWrap('Job', jobSelect),
      labelWrap('Inputs', inputSummary),
      labelWrap('Outputs', outputSummary),
      inputsDialog,
      outputsDialog,
      removeButtonWrap
    );
    list.appendChild(row);
    renderInputTable(row);
    renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')]));
    syncJobsJson();
  }

  addButton.addEventListener('click', () => addRow({
    runner_id: catalog.length === 1 ? catalog[0].id : '',
    runner_job_name: '',
    inputs: {}
  }));

  for (const job of initialJobs) {
    addRow(job);
  }
})();
