const USERNAME_PATTERN = /^[A-Za-z0-9_.-]+$/;

export function validationMessageFor(field) {
  const value = 'value' in field ? String(field.value ?? '') : '';
  const trimmed = value.trim();

  if (field.dataset.trimRequired === 'true' && trimmed === '') {
    return 'This field is required.';
  }
  if (field.dataset.username === 'true') {
    if (trimmed.length > 0 && trimmed.length < 3) {
      return 'Username must be at least 3 characters.';
    }
    if (trimmed.length > 0 && !USERNAME_PATTERN.test(trimmed)) {
      return 'Username can only contain letters, numbers, underscores, hyphens, and dots.';
    }
  }
  if (field.dataset.noWhitespace === 'true' && trimmed !== '' && /\s/.test(trimmed)) {
    return 'This field must not contain whitespace.';
  }
  if (field.dataset.singleBranch === 'true' && trimmed.includes(',')) {
    return 'Enter only one branch.';
  }
  return '';
}

export function validateField(field) {
  field.setCustomValidity(validationMessageFor(field));
  return field.checkValidity();
}

export function validateForm(form) {
  const fields = [...form.querySelectorAll('[data-validate]')];
  const valid = fields.every(validateField) && form.checkValidity();
  if (!valid) form.reportValidity();
  return valid;
}

function initFormValidation(documentRef = document) {
  const fields = [...documentRef.querySelectorAll('[data-validate]')];
  for (const field of fields) {
    field.addEventListener('input', () => validateField(field));
    field.addEventListener('change', () => validateField(field));
  }
  for (const form of documentRef.querySelectorAll('form')) {
    form.addEventListener('submit', (event) => {
      if (!validateForm(form)) event.preventDefault();
    });
  }
}

if (typeof document !== 'undefined') {
  initFormValidation();
}
