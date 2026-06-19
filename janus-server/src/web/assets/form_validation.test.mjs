import assert from 'node:assert/strict';
import test from 'node:test';

import { validationMessageFor } from './form_validation.js';

function field(value, dataset = {}) {
  return { value, dataset };
}

test('validationMessageFor rejects trim-required whitespace', () => {
  assert.equal(
    validationMessageFor(field('   ', { trimRequired: 'true' })),
    'This field is required.'
  );
});

test('validationMessageFor rejects invalid usernames', () => {
  assert.equal(
    validationMessageFor(field('ab', { username: 'true' })),
    'Username must be at least 3 characters.'
  );
  assert.equal(
    validationMessageFor(field('bad name', { username: 'true' })),
    'Username can only contain letters, numbers, underscores, hyphens, and dots.'
  );
});

test('validationMessageFor rejects branch and ref whitespace', () => {
  assert.equal(
    validationMessageFor(field('refs/heads/main branch', { noWhitespace: 'true' })),
    'This field must not contain whitespace.'
  );
  assert.equal(
    validationMessageFor(field('main,release', { singleBranch: 'true' })),
    'Enter only one branch.'
  );
});
