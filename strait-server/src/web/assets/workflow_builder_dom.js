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

export function labelWrap(text, input) {
  const label = document.createElement('label');
  const caption = document.createElement('span');
  caption.textContent = text;
  label.appendChild(caption);
  label.appendChild(input);
  return label;
}

export function tableHead(labels) {
  const thead = document.createElement('thead');
  const row = document.createElement('tr');
  for (const label of labels) {
    const cell = document.createElement('th');
    cell.textContent = label;
    row.appendChild(cell);
  }
  thead.appendChild(row);
  return thead;
}

export function sectionHeader(eyebrowText, titleText, noteText) {
  const wrap = document.createElement('div');
  const eyebrow = document.createElement('div');
  eyebrow.className = 'eyebrow';
  eyebrow.textContent = eyebrowText;
  const title = document.createElement('h3');
  title.textContent = titleText;
  const note = document.createElement('p');
  note.className = 'muted';
  note.textContent = noteText;
  wrap.append(eyebrow, title, note);
  return wrap;
}
