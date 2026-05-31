import assert from 'node:assert/strict';
import test from 'node:test';

import {
  cloneTemplate,
  makeInput,
  makeSelect,
  replaceOptions,
} from './workflow_builder_dom.js';
import { fakeElement, withFakeDocument } from './workflow_builder_test_dom.mjs';

test('makeInput creates text-like inputs with string values', () => withFakeDocument(() => {
  const input = makeInput('text', 'hello');

  assert.equal(input.tagName, 'INPUT');
  assert.equal(input.type, 'text');
  assert.equal(input.value, 'hello');
  assert.equal(input.checked, false);
}));

test('makeInput creates checkbox inputs with checked state', () => withFakeDocument(() => {
  const input = makeInput('checkbox', true);

  assert.equal(input.tagName, 'INPUT');
  assert.equal(input.type, 'checkbox');
  assert.equal(input.checked, true);
  assert.equal(input.value, '');
}));

test('makeSelect creates select elements', () => withFakeDocument(() => {
  const select = makeSelect();

  assert.equal(select.tagName, 'SELECT');
  assert.deepEqual(select.options, []);
}));

test('replaceOptions renders placeholder and selected option', () => withFakeDocument(() => {
  const select = makeSelect();

  replaceOptions(
    select,
    [
      { value: 'runner-1', label: 'Runner 1' },
      { value: 'runner-2', label: 'Runner 2' }
    ],
    'runner-2',
    { placeholder: { label: 'Choose runner' } }
  );

  assert.deepEqual(select.options.map((option) => ({
    value: option.value,
    textContent: option.textContent,
    selected: option.selected
  })), [
    { value: '', textContent: 'Choose runner', selected: false },
    { value: 'runner-1', textContent: 'Runner 1', selected: false },
    { value: 'runner-2', textContent: 'Runner 2', selected: true }
  ]);
  assert.equal(select.value, 'runner-2');
}));

test('replaceOptions selects placeholder when no value is selected', () => withFakeDocument(() => {
  const select = makeSelect();

  replaceOptions(
    select,
    [{ value: 'build', label: 'build' }],
    '',
    { placeholder: { label: 'Select job' } }
  );

  assert.equal(select.options[0].selected, true);
  assert.equal(select.value, '');
}));

test('replaceOptions can select the first real option', () => withFakeDocument(() => {
  const select = makeSelect();

  replaceOptions(
    select,
    [{ value: 'build', label: 'build' }],
    '',
    {
      placeholder: { label: 'Select job' },
      selectFirst: true
    }
  );

  assert.equal(select.value, 'build');
}));

test('cloneTemplate deep-clones the template first element', () => {
  const child = fakeElement('span');
  child.textContent = 'child';
  const root = fakeElement('div');
  root.value = 'original';
  root.appendChild(child);
  const template = {
    content: {
      firstElementChild: root
    }
  };

  const clone = cloneTemplate(template);

  assert.notEqual(clone, root);
  assert.equal(clone.tagName, 'DIV');
  assert.equal(clone.children.length, 1);
  assert.notEqual(clone.children[0], child);
  assert.equal(clone.children[0].textContent, 'child');
});
