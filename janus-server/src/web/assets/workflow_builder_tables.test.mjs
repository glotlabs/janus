import assert from 'node:assert/strict';
import test from 'node:test';

import {
  renderInputTable,
  renderOutputTable,
} from './workflow_builder_tables.js';
import { fakeElement, withFakeDocument } from './workflow_builder_test_dom.mjs';

function templateFrom(element) {
  return {
    content: {
      firstElementChild: element
    }
  };
}

function tableTemplate() {
  const table = fakeElement('table');
  const tbody = fakeElement('tbody');
  tbody.setAttribute('data-table-body', 'true');
  table.appendChild(tbody);
  return templateFrom(table);
}

function emptyTemplate(text) {
  const element = fakeElement('div');
  element.className = 'muted';
  element.textContent = text;
  return templateFrom(element);
}

function workflowRow({ runnerId = 'runner-1', jobName = 'build', inputs = {} } = {}) {
  const inputsWrap = fakeElement('div');
  const outputsWrap = fakeElement('div');
  const inputSummary = fakeElement('button');
  const outputSummary = fakeElement('button');
  const fields = {
    runner_id: { value: runnerId },
    runner_job_name: { value: jobName }
  };
  return {
    _inputs: inputs,
    elements: {
      inputsWrap,
      outputsWrap,
      inputSummary,
      outputSummary
    },
    querySelector(selector) {
      const fieldMatch = selector.match(/^\[data-field="(.+)"\]$/);
      if (fieldMatch) return fields[fieldMatch[1]];
      if (selector === '[data-inputs-wrap]') return inputsWrap;
      if (selector === '[data-outputs-wrap]') return outputsWrap;
      if (selector === '[data-input-summary]') return inputSummary;
      if (selector === '[data-output-summary]') return outputSummary;
      return null;
    }
  };
}

const templates = {
  inputsEmpty: emptyTemplate('None'),
  outputsEmpty: emptyTemplate('None'),
  inputsTable: tableTemplate(),
  outputsTable: tableTemplate()
};

const getJobDefinition = (_runnerId, jobName) => {
  if (jobName === 'empty') return { inputs: {}, outputs: {} };
  if (jobName === 'build') {
    return {
      inputs: {
        message: { type: 'string', required: true },
        count: { type: 'integer', required: false }
      },
      outputs: {
        artifact: { type: 'artifact', required: true },
        version: { type: 'string', required: false }
      }
    };
  }
  return null;
};

test('renderInputTable renders empty state and summary', () => withFakeDocument(() => {
  const row = workflowRow({ jobName: 'empty' });

  renderInputTable({
    row,
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'empty', jobIndex: 0, name: 'empty' }],
    getJobDefinition,
    templates,
    onBindingChanged() {}
  });

  assert.equal(row.elements.inputSummary.textContent, 'None');
  assert.equal(row.elements.inputsWrap.children[0].textContent, 'None');
}));

test('renderInputTable renders input rows, required badge, and literal hint', () => withFakeDocument(() => {
  const row = workflowRow();

  renderInputTable({
    row,
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'build' }],
    getJobDefinition,
    templates,
    onBindingChanged() {}
  });

  const table = row.elements.inputsWrap.children[0];
  const tbody = table.querySelector('[data-table-body]');
  const [messageRow, countRow] = tbody.children;
  const [nameCell, typeCell, modeCell, valueCell] = messageRow.children;

  assert.equal(row.elements.inputSummary.textContent, '2 inputs');
  assert.equal(nameCell.children[0].textContent, 'message');
  assert.equal(nameCell.children[2].textContent, 'required');
  assert.equal(typeCell.textContent, 'string');
  assert.equal(modeCell.children[0].tagName, 'SELECT');
  assert.equal(valueCell.children[0].tagName, 'INPUT');
  assert.equal(valueCell.children[1].textContent, 'Enter a plain string value.');
  assert.equal(countRow.children[0].children[0].textContent, 'count');
}));

test('renderInputTable value field changes update binding value and call callback', () => withFakeDocument(() => {
  let calls = 0;
  const row = workflowRow();

  renderInputTable({
    row,
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'build' }],
    getJobDefinition,
    templates,
    onBindingChanged() {
      calls += 1;
    }
  });

  const inputRow = row.elements.inputsWrap.children[0].querySelector('[data-table-body]').children[0];
  const valueField = inputRow.children[3].children[0];
  valueField.value = 'hello';
  valueField.listeners.input[0]();

  assert.equal(inputRow.dataset.bindingValue, 'hello');
  assert.equal(calls, 1);
}));

test('renderInputTable mode changes clear binding value and call callback', () => withFakeDocument(() => {
  let calls = 0;
  const row = workflowRow({ inputs: { message: { kind: 'literal', value: 'hello' } } });

  renderInputTable({
    row,
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'build' }],
    getJobDefinition,
    templates,
    onBindingChanged() {
      calls += 1;
    }
  });

  const inputRow = row.elements.inputsWrap.children[0].querySelector('[data-table-body]').children[0];
  const modeSelect = inputRow.children[2].children[0];
  inputRow.dataset.bindingValue = 'hello';
  modeSelect.value = 'commit';
  modeSelect.listeners.change[0]();

  assert.equal(inputRow.dataset.bindingValue, '');
  assert.equal(calls, 1);
}));

test('renderInputTable hides unavailable output binding select', () => withFakeDocument(() => {
  const row = workflowRow();

  renderInputTable({
    row,
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'build' }],
    getJobDefinition,
    templates,
    onBindingChanged() {}
  });

  const inputRow = row.elements.inputsWrap.children[0].querySelector('[data-table-body]').children[0];
  const modeSelect = inputRow.children[2].children[0];
  modeSelect.value = 'output_value';
  modeSelect.listeners.change[0]();

  assert.equal(inputRow.children[3].children[0].hidden, true);
  assert.equal(inputRow.children[3].children[1].textContent, 'No outputs available');
}));

test('renderOutputTable renders empty state and summary', () => withFakeDocument(() => {
  const row = workflowRow({ jobName: 'empty' });

  renderOutputTable({
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'empty', jobIndex: 0, name: 'empty' }],
    getJobDefinition,
    templates
  });

  assert.equal(row.elements.outputSummary.textContent, 'None');
  assert.equal(row.elements.outputsWrap.children[0].textContent, 'None');
}));

test('renderOutputTable renders output rows', () => withFakeDocument(() => {
  const row = workflowRow();

  renderOutputTable({
    derivedJobs: [{ row, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'build' }],
    getJobDefinition,
    templates
  });

  const table = row.elements.outputsWrap.children[0];
  const tbody = table.querySelector('[data-table-body]');
  const [artifactRow, versionRow] = tbody.children;

  assert.equal(row.elements.outputSummary.textContent, '2 outputs');
  assert.equal(artifactRow.children[0].children[0].textContent, 'artifact');
  assert.equal(artifactRow.children[1].textContent, 'artifact');
  assert.equal(artifactRow.children[2].textContent, 'required');
  assert.equal(versionRow.children[0].children[0].textContent, 'version');
  assert.equal(versionRow.children[2].textContent, 'optional');
}));
