import {
  cloneTemplate,
  makeInput,
  makeSelect,
  replaceOptions,
} from '/assets/workflow_builder_dom.js';
import {
  bindingModesFor,
  buildDerivedJobs,
  createCatalogLookup,
  findDerivedJobByIndex,
  inferBinding,
  literalHintFor,
  outputOptionsFor,
  parseOutputBinding,
  readInputBinding,
} from '/assets/workflow_builder_state.js';

(() => {
  const list = document.getElementById('workflow-job-list');
  const addButton = document.getElementById('workflow-add-job');
  const jobsJsonField = document.getElementById('workflow-jobs-json');
  const jobRowTemplate = document.getElementById('workflow-job-row-template');
  const inputsEmptyTemplate = document.getElementById('workflow-inputs-empty-template');
  const outputsEmptyTemplate = document.getElementById('workflow-outputs-empty-template');
  const inputsTableTemplate = document.getElementById('workflow-inputs-table-template');
  const outputsTableTemplate = document.getElementById('workflow-outputs-table-template');
  const catalog = JSON.parse(document.getElementById('workflow-runner-catalog').textContent || '[]');
  const initialJobs = JSON.parse(document.getElementById('workflow-initial-jobs').textContent || '[]');
  const { getRunner, getRunnerJobs, getJobDefinition } = createCatalogLookup(catalog);

  function fillRunnerOptions(select, selectedRunnerId) {
    replaceOptions(
      select,
      catalog.map((runner) => ({ value: runner.id, label: `${runner.name} (${runner.id})` })),
      selectedRunnerId,
      {
        placeholder: { label: catalog.length <= 1 ? 'Select runner' : 'Choose runner' },
        selectFirst: catalog.length === 1
      }
    );
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
    const jobs = getRunnerJobs(runnerSelect.value);
    const currentRow = runnerSelect.closest('[data-workflow-job-row]');
    const taken = takenJobNames(runnerSelect.value, currentRow);
    const availableJobs = jobs.filter((job) => !taken.has(job.name) || job.name === selectedJobName);
    replaceOptions(
      jobSelect,
      availableJobs.map((job) => ({ value: job.name, label: job.name })),
      selectedJobName,
      {
        placeholder: { label: availableJobs.length <= 1 ? 'Select job' : 'Choose job' },
        selectFirst: availableJobs.length === 1
      }
    );
    jobSelect.disabled = availableJobs.length === 0;
  }

  function syncJobsJson() {
    const rows = [...list.querySelectorAll('[data-workflow-job-row]')];
    const derivedJobs = buildDerivedJobs(rows, getRunner);
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

  function outputBindingHint(row, inputRow, mode, derivedJobs) {
    if (mode !== 'output_artifact' && mode !== 'output_value') return '';
    const reference = inputRow.dataset.bindingValue || '';
    const kind = inputRow.dataset.inputKind;
    const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
    const options = outputOptionsFor(row, derivedJobs, expectedKind, getJobDefinition);
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
        const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
        const options = outputOptionsFor(job.row, derivedJobs, expectedKind, getJobDefinition);
        replaceOptions(valueField, options, selected, {
          placeholder: options.length === 0
            ? { label: `No ${expectedKind} outputs available`, selected: true }
            : null,
          selectFirst: true
        });
        inputRow.dataset.bindingValue = valueField.value;
      }
    }
  }

  function buildValueField(kind, binding, row) {
    if (kind === 'artifact') {
      const select = makeSelect();
      select.setAttribute('data-binding-value', 'true');
      if (binding.mode === 'source_artifact') {
        replaceOptions(select, [{ value: 'source.tar.gz', label: 'source.tar.gz' }], 'source.tar.gz');
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
      replaceOptions(
        select,
        ['true', 'false'].map((value) => ({ value, label: value })),
        String(binding.value || 'false')
      );
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
    const derivedJobs = buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner);
    const wrap = row.querySelector('[data-inputs-wrap]');
    const summary = row.querySelector('[data-input-summary]');
    wrap.replaceChildren();
    const runnerId = row.querySelector('[data-field="runner_id"]').value;
    const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value;
    const definition = getJobDefinition(runnerId, runnerJobName);
    const inputs = definition ? Object.entries(definition.inputs || {}) : [];
    if (inputs.length === 0) {
      wrap.appendChild(cloneTemplate(inputsEmptyTemplate));
      if (summary) summary.textContent = 'No inputs';
      return;
    }
    if (summary) summary.textContent = `${inputs.length} input${inputs.length === 1 ? '' : 's'}`;
    const table = cloneTemplate(inputsTableTemplate);
    const tbody = table.querySelector('[data-table-body]');
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
      replaceOptions(
        modeSelect,
        bindingModesFor(inputDef.type).map(([value, label]) => ({ value, label })),
        binding.mode
      );
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
        wrap.appendChild(cloneTemplate(outputsEmptyTemplate));
        continue;
      }
      const table = cloneTemplate(outputsTableTemplate);
      const tbody = table.querySelector('[data-table-body]');
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
      wrap.appendChild(table);
    }
  }

  function addRow(job) {
    const row = cloneTemplate(jobRowTemplate);
    row._inputs = job.inputs || {};
    row._allowFailure = Boolean(job.allow_failure);

    const runnerSelect = row.querySelector('[data-field="runner_id"]');
    const jobSelect = row.querySelector('[data-field="runner_job_name"]');
    const inputSummary = row.querySelector('[data-input-summary]');
    const outputSummary = row.querySelector('[data-output-summary]');
    const inputsDialog = row.querySelector('[data-inputs-dialog]');
    const outputsDialog = row.querySelector('[data-outputs-dialog]');
    const removeButton = row.querySelector('[data-remove-job]');

    fillRunnerOptions(runnerSelect, job.runner_id);
    fillJobOptions(runnerSelect, jobSelect, job.runner_job_name);

    runnerSelect.addEventListener('change', () => {
      fillJobOptions(runnerSelect, jobSelect, '');
      row._inputs = {};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner));
      syncJobsJson();
    });
    jobSelect.addEventListener('change', () => {
      row._inputs = {};
      renderInputTable(row);
      renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner));
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
    for (const closeButton of row.querySelectorAll('[data-dialog-close="inputs"]')) {
      closeButton.addEventListener('click', () => inputsDialog.close());
    }
    for (const closeButton of row.querySelectorAll('[data-dialog-close="outputs"]')) {
      closeButton.addEventListener('click', () => outputsDialog.close());
    }
    removeButton.addEventListener('click', () => {
      inputsDialog.close();
      outputsDialog.close();
      row.remove();
      syncJobsJson();
    });

    list.appendChild(row);
    renderInputTable(row);
    renderOutputTable(buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner));
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
