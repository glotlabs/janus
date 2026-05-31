import assert from 'node:assert/strict';
import test from 'node:test';

import {
  addWorkflowRow,
  fillOutcomePolicyOptions,
  fillJobOptions,
  fillRunnerOptions,
} from './workflow_builder_rows.js';
import { OutcomePolicy } from './workflow_builder_state.js';
import { fakeElement, withFakeDocument } from './workflow_builder_test_dom.mjs';

function rowTemplate() {
  const row = fakeElement('fieldset');
  row.setAttribute('data-workflow-job-row', 'true');
  const runnerSelect = fakeElement('select');
  runnerSelect.setAttribute('data-field', 'runner_id');
  const jobSelect = fakeElement('select');
  jobSelect.setAttribute('data-field', 'runner_job_name');
  const outcomePolicySelect = fakeElement('select');
  outcomePolicySelect.setAttribute('data-field', 'outcome_policy');
  const inputSummary = fakeElement('button');
  inputSummary.setAttribute('data-input-summary', 'true');
  const outputSummary = fakeElement('button');
  outputSummary.setAttribute('data-output-summary', 'true');
  const inputsDialog = fakeElement('dialog');
  inputsDialog.setAttribute('data-inputs-dialog', 'true');
  const outputsDialog = fakeElement('dialog');
  outputsDialog.setAttribute('data-outputs-dialog', 'true');
  const inputClose = fakeElement('button');
  inputClose.setAttribute('data-dialog-close', 'inputs');
  const outputClose = fakeElement('button');
  outputClose.setAttribute('data-dialog-close', 'outputs');
  const removeButton = fakeElement('button');
  removeButton.setAttribute('data-remove-job', 'true');

  row.append(
    runnerSelect,
    jobSelect,
    outcomePolicySelect,
    inputSummary,
    outputSummary,
    inputsDialog,
    outputsDialog,
    inputClose,
    outputClose,
    removeButton
  );
  return {
    content: {
      firstElementChild: row
    }
  };
}

const catalog = [
  {
    id: 'runner-1',
    name: 'Linux',
    jobs: [
      { name: 'build' },
      { name: 'test' }
    ]
  }
];

function getRunner(runnerId) {
  return catalog.find((runner) => runner.id === runnerId) || null;
}

function getRunnerJobs(runnerId) {
  return getRunner(runnerId)?.jobs || [];
}

test('fillRunnerOptions auto-selects one runner and uses select placeholder', () => withFakeDocument(() => {
  const select = fakeElement('select');

  fillRunnerOptions(select, catalog, '');

  assert.deepEqual(select.options.map((option) => ({
    value: option.value,
    textContent: option.textContent,
    selected: option.selected
  })), [
    { value: '', textContent: 'Select runner', selected: true },
    { value: 'runner-1', textContent: 'Linux', selected: false }
  ]);
  assert.equal(select.value, 'runner-1');
}));

test('fillOutcomePolicyOptions renders string outcome policy choices', () => withFakeDocument(() => {
  const select = fakeElement('select');

  fillOutcomePolicyOptions(select, OutcomePolicy.ALLOWED);

  assert.deepEqual(select.options.map((option) => ({
    value: option.value,
    textContent: option.textContent,
    selected: option.selected
  })), [
    { value: 'required', textContent: 'Must succeed', selected: false },
    { value: 'allowed_to_fail', textContent: 'Can fail', selected: true }
  ]);
  assert.equal(select.value, 'allowed_to_fail');
}));

test('fillJobOptions excludes earlier selected jobs for the same runner', () => withFakeDocument(() => {
  const list = fakeElement('div');
  const earlierRow = rowTemplate().content.firstElementChild.cloneNode(true);
  earlierRow.querySelector('[data-field="runner_id"]').value = 'runner-1';
  earlierRow.querySelector('[data-field="runner_job_name"]').value = 'build';
  const currentRow = rowTemplate().content.firstElementChild.cloneNode(true);
  const runnerSelect = currentRow.querySelector('[data-field="runner_id"]');
  const jobSelect = currentRow.querySelector('[data-field="runner_job_name"]');
  runnerSelect.value = 'runner-1';
  list.append(earlierRow, currentRow);

  fillJobOptions({
    runnerSelect,
    jobSelect,
    selectedJobName: '',
    list,
    getRunnerJobs,
  });

  assert.deepEqual(jobSelect.options.map((option) => option.value), ['', 'test']);
  assert.equal(jobSelect.value, 'test');
}));

test('addWorkflowRow renders, opens dialogs, closes dialogs, and removes row', () => withFakeDocument(() => {
  const list = fakeElement('div');
  let renderInputs = 0;
  let renderOutputs = 0;
  let syncs = 0;

  const row = addWorkflowRow({
    job: {
      runner_id: 'runner-1',
      runner_job_name: 'build',
      inputs: {},
      outcome_policy: 'allowed_to_fail'
    },
    catalog,
    list,
    jobRowTemplate: rowTemplate(),
    getRunner,
    getRunnerJobs,
    renderInputTable() {
      renderInputs += 1;
    },
    renderOutputTable() {
      renderOutputs += 1;
    },
    syncJobsJson() {
      syncs += 1;
    },
  });

  const inputSummary = row.querySelector('[data-input-summary]');
  const outputSummary = row.querySelector('[data-output-summary]');
  const outcomePolicySelect = row.querySelector('[data-field="outcome_policy"]');
  const inputsDialog = row.querySelector('[data-inputs-dialog]');
  const outputsDialog = row.querySelector('[data-outputs-dialog]');
  const inputClose = row.querySelector('[data-dialog-close="inputs"]');
  const outputClose = row.querySelector('[data-dialog-close="outputs"]');
  const removeButton = row.querySelector('[data-remove-job]');

  assert.equal(list.children.length, 1);
  assert.equal(row._outcomePolicy, OutcomePolicy.ALLOWED);
  assert.equal(outcomePolicySelect.value, 'allowed_to_fail');
  assert.equal(renderInputs, 1);
  assert.equal(renderOutputs, 1);
  assert.equal(syncs, 1);

  inputSummary.listeners.click[0]();
  outputSummary.listeners.click[0]();
  assert.equal(inputsDialog.showModalCalled, true);
  assert.equal(outputsDialog.showModalCalled, true);

  inputClose.listeners.click[0]();
  outputClose.listeners.click[0]();
  assert.equal(inputsDialog.closeCalled, true);
  assert.equal(outputsDialog.closeCalled, true);

  removeButton.listeners.click[0]();
  assert.equal(row.removed, true);
  assert.equal(list.children.length, 0);
  assert.equal(syncs, 2);
}));

test('addWorkflowRow syncs when outcome policy changes', () => withFakeDocument(() => {
  const list = fakeElement('div');
  let syncs = 0;

  const row = addWorkflowRow({
    job: { runner_id: 'runner-1', runner_job_name: 'build', inputs: {}, outcome_policy: 'required' },
    catalog,
    list,
    jobRowTemplate: rowTemplate(),
    getRunner,
    getRunnerJobs,
    renderInputTable() {},
    renderOutputTable() {},
    syncJobsJson() {
      syncs += 1;
    },
  });

  const outcomePolicySelect = row.querySelector('[data-field="outcome_policy"]');
  outcomePolicySelect.value = OutcomePolicy.ALLOWED;
  outcomePolicySelect.listeners.change[0]();

  assert.equal(row._outcomePolicy, OutcomePolicy.ALLOWED);
  assert.equal(syncs, 2);
}));

test('addWorkflowRow rerenders and syncs on runner and job changes', () => withFakeDocument(() => {
  const list = fakeElement('div');
  let renderInputs = 0;
  let renderOutputs = 0;
  let syncs = 0;

  const row = addWorkflowRow({
    job: { runner_id: 'runner-1', runner_job_name: 'build', inputs: { stale: true } },
    catalog,
    list,
    jobRowTemplate: rowTemplate(),
    getRunner,
    getRunnerJobs,
    renderInputTable() {
      renderInputs += 1;
    },
    renderOutputTable() {
      renderOutputs += 1;
    },
    syncJobsJson() {
      syncs += 1;
    },
  });

  const runnerSelect = row.querySelector('[data-field="runner_id"]');
  const jobSelect = row.querySelector('[data-field="runner_job_name"]');
  runnerSelect.listeners.change[0]();
  assert.deepEqual(row._inputs, {});
  jobSelect.listeners.change[0]();

  assert.equal(renderInputs, 3);
  assert.equal(renderOutputs, 3);
  assert.equal(syncs, 3);
}));
