import {
  cloneTemplate,
  replaceOptions,
} from './workflow_builder_dom.js';
import {
  OUTCOME_POLICY_OPTIONS,
  buildDerivedJobs,
  outcomePolicyFromJob,
} from './workflow_builder_state.js';

export function addWorkflowRow({
  job,
  catalog,
  list,
  jobRowTemplate,
  getRunner,
  getRunnerJobs,
  renderInputTable,
  renderOutputTable,
  syncJobsJson,
}) {
  const row = cloneTemplate(jobRowTemplate);
  row._inputs = job.inputs || {};
  row._outcomePolicy = outcomePolicyFromJob(job);

  const controls = rowControls(row);
  fillRunnerOptions(controls.runnerSelect, catalog, job.runner_id);
  fillJobOptions({
    runnerSelect: controls.runnerSelect,
    jobSelect: controls.jobSelect,
    selectedJobName: job.runner_job_name,
    list,
    getRunnerJobs,
  });
  fillOutcomePolicyOptions(controls.outcomePolicySelect, row._outcomePolicy);

  controls.runnerSelect.addEventListener('change', () => {
    fillJobOptions({
      runnerSelect: controls.runnerSelect,
      jobSelect: controls.jobSelect,
      selectedJobName: '',
      list,
      getRunnerJobs,
    });
    row._inputs = {};
    renderInputTable(row);
    renderOutputTable(currentDerivedJobs(list, getRunner));
    syncJobsJson();
  });
  controls.jobSelect.addEventListener('change', () => {
    row._inputs = {};
    renderInputTable(row);
    renderOutputTable(currentDerivedJobs(list, getRunner));
    syncJobsJson();
  });
  controls.outcomePolicySelect.addEventListener('change', () => {
    row._outcomePolicy = controls.outcomePolicySelect.value;
    syncJobsJson();
  });
  controls.inputSummary.addEventListener('click', () => openDialog(controls.inputsDialog));
  controls.outputSummary.addEventListener('click', () => openDialog(controls.outputsDialog));
  for (const closeButton of row.querySelectorAll('[data-dialog-close="inputs"]')) {
    closeButton.addEventListener('click', () => controls.inputsDialog.close());
  }
  for (const closeButton of row.querySelectorAll('[data-dialog-close="outputs"]')) {
    closeButton.addEventListener('click', () => controls.outputsDialog.close());
  }
  controls.removeButton.addEventListener('click', () => {
    controls.inputsDialog.close();
    controls.outputsDialog.close();
    row.remove();
    syncJobsJson();
  });

  list.appendChild(row);
  renderInputTable(row);
  renderOutputTable(currentDerivedJobs(list, getRunner));
  syncJobsJson();
  return row;
}

export function fillOutcomePolicyOptions(select, selectedPolicy) {
  replaceOptions(
    select,
    OUTCOME_POLICY_OPTIONS.map(([value, label]) => ({ value, label })),
    selectedPolicy
  );
}

export function fillRunnerOptions(select, catalog, selectedRunnerId) {
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

export function fillJobOptions({
  runnerSelect,
  jobSelect,
  selectedJobName,
  list,
  getRunnerJobs,
}) {
  const jobs = getRunnerJobs(runnerSelect.value);
  const currentRow = runnerSelect.closest('[data-workflow-job-row]');
  const taken = takenJobNames(list, runnerSelect.value, currentRow);
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

function rowControls(row) {
  return {
    runnerSelect: row.querySelector('[data-field="runner_id"]'),
    jobSelect: row.querySelector('[data-field="runner_job_name"]'),
    outcomePolicySelect: row.querySelector('[data-field="outcome_policy"]'),
    inputSummary: row.querySelector('[data-input-summary]'),
    outputSummary: row.querySelector('[data-output-summary]'),
    inputsDialog: row.querySelector('[data-inputs-dialog]'),
    outputsDialog: row.querySelector('[data-outputs-dialog]'),
    removeButton: row.querySelector('[data-remove-job]'),
  };
}

function takenJobNames(list, runnerId, currentRow) {
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

function currentDerivedJobs(list, getRunner) {
  return buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner);
}

function openDialog(dialog) {
  if (typeof dialog.showModal === 'function') {
    dialog.showModal();
  } else {
    dialog.setAttribute('open', 'open');
  }
}
