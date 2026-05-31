export function makeInput(type, value) {
  const input = document.createElement('input');
  input.type = type;
  if (type === 'checkbox') {
    input.checked = Boolean(value);
  } else {
    input.value = value || '';
  }
  return input;
}

export function makeSelect() {
  return document.createElement('select');
}

export function cloneTemplate(template) {
  return template.content.firstElementChild.cloneNode(true);
}

export function replaceOptions(select, options, selectedValue, config = {}) {
  select.replaceChildren();
  if (config.placeholder) {
    const placeholder = document.createElement('option');
    placeholder.value = config.placeholder.value || '';
    placeholder.textContent = config.placeholder.label;
    placeholder.selected = !selectedValue || Boolean(config.placeholder.selected);
    select.appendChild(placeholder);
  }
  for (const optionData of options) {
    const option = document.createElement('option');
    option.value = optionData.value;
    option.textContent = optionData.label;
    option.selected = optionData.value === selectedValue;
    select.appendChild(option);
  }
  if (config.selectFirst && !select.value && options.length > 0) {
    select.value = options[0].value;
  }
}
