import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildValueField,
  outputBindingHint,
  renderOutputBindingOptions,
} from './workflow_builder_bindings.js';
import { withFakeDocument } from './workflow_builder_test_dom.mjs';

function inputRow({ kind, mode, value = '', tagName = 'SELECT' }) {
  const valueField = {
    tagName,
    value,
    children: [],
    replaceChildren(...children) {
      this.children = children;
      this.value = children.find((child) => child.selected)?.value || '';
    },
    appendChild(child) {
      this.children.push(child);
      if (child.selected) this.value = child.value;
      return child;
    },
    get options() {
      return this.children;
    }
  };
  return {
    dataset: {
      inputKind: kind,
      bindingValue: value
    },
    valueField,
    querySelector(selector) {
      if (selector === '[data-binding-mode]') return { value: mode };
      if (selector === '[data-binding-value]') return valueField;
      return null;
    }
  };
}

function jobRow(inputRows = []) {
  return {
    querySelectorAll(selector) {
      return selector === '[data-input-row]' ? inputRows : [];
    }
  };
}

const getJobDefinition = (_runnerId, jobName) => {
  if (jobName !== 'build') return null;
  return {
    outputs: {
      artifact: { type: 'artifact' },
      version: { type: 'string' }
    }
  };
};

test('outputBindingHint describes missing earlier matching outputs', () => {
  const currentRow = jobRow();
  const currentInput = inputRow({ kind: 'artifact', mode: 'output_artifact' });
  const hint = outputBindingHint({
    row: currentRow,
    inputRow: currentInput,
    mode: 'output_artifact',
    derivedJobs: [{ row: currentRow, runnerId: 'runner-1', runnerJobName: 'deploy', jobIndex: 0, name: 'deploy' }],
    getJobDefinition
  });

  assert.equal(hint, 'No outputs available');
});

test('outputBindingHint describes selected source job', () => {
  const sourceRow = jobRow();
  const currentRow = jobRow();
  const currentInput = inputRow({
    kind: 'string',
    mode: 'output_value',
    value: '{"kind":"job_output","job_index":0,"output_name":"version"}'
  });
  const hint = outputBindingHint({
    row: currentRow,
    inputRow: currentInput,
    mode: 'output_value',
    derivedJobs: [
      { row: sourceRow, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'Linux / build' },
      { row: currentRow, runnerId: 'runner-1', runnerJobName: 'deploy', jobIndex: 1, name: 'Linux / deploy' }
    ],
    getJobDefinition
  });

  assert.equal(hint, 'Binding to Linux / build.');
});

test('renderOutputBindingOptions renders unavailable placeholder', () => withFakeDocument(() => {
  const currentInput = inputRow({ kind: 'artifact', mode: 'output_artifact' });
  const currentRow = jobRow([currentInput]);

  renderOutputBindingOptions({
    derivedJobs: [
      { row: currentRow, runnerId: 'runner-1', runnerJobName: 'deploy', jobIndex: 0, name: 'Linux / deploy' }
    ],
    getJobDefinition
  });

  assert.deepEqual(currentInput.valueField.options.map((option) => ({
    value: option.value,
    textContent: option.textContent,
    selected: option.selected
  })), [
    { value: '', textContent: 'No outputs available', selected: true }
  ]);
  assert.equal(currentInput.valueField.hidden, true);
}));

test('renderOutputBindingOptions selects the first available earlier output', () => withFakeDocument(() => {
  const sourceRow = jobRow();
  const currentInput = inputRow({ kind: 'artifact', mode: 'output_artifact' });
  const currentRow = jobRow([currentInput]);

  renderOutputBindingOptions({
    derivedJobs: [
      { row: sourceRow, runnerId: 'runner-1', runnerJobName: 'build', jobIndex: 0, name: 'Linux / build' },
      { row: currentRow, runnerId: 'runner-1', runnerJobName: 'deploy', jobIndex: 1, name: 'Linux / deploy' }
    ],
    getJobDefinition
  });

  assert.equal(currentInput.valueField.options.length, 1);
  assert.equal(currentInput.valueField.value, '{"kind":"job_output","job_index":0,"output_name":"artifact"}');
  assert.equal(currentInput.dataset.bindingValue, '{"kind":"job_output","job_index":0,"output_name":"artifact"}');
  assert.equal(currentInput.valueField.hidden, false);
}));

test('buildValueField renders commit and branch markers', () => withFakeDocument(() => {
  const row = { dataset: {} };

  assert.deepEqual(
    ['commit', 'branch'].map((mode) => {
      const field = buildValueField('string', { mode, value: '' }, row);
      return {
        tagName: field.tagName,
        className: field.className,
        textContent: field.textContent
      };
    }),
    [
      { tagName: 'DIV', className: 'muted', textContent: '<commit>' },
      { tagName: 'DIV', className: 'muted', textContent: '<branch>' }
    ]
  );
}));

test('buildValueField renders boolean select options', () => withFakeDocument(() => {
  const row = { dataset: {} };
  const field = buildValueField('boolean', { mode: 'literal', value: 'true' }, row);

  assert.equal(field.tagName, 'SELECT');
  assert.deepEqual(field.options.map((option) => ({
    value: option.value,
    textContent: option.textContent,
    selected: option.selected
  })), [
    { value: 'true', textContent: 'true', selected: true },
    { value: 'false', textContent: 'false', selected: false }
  ]);
}));
