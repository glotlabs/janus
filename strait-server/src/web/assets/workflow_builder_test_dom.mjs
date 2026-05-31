export function fakeElement(tagName) {
  return {
    tagName: tagName.toUpperCase(),
    children: [],
    attributes: {},
    dataset: {},
    className: '',
    rows: 0,
    parent: null,
    removed: false,
    open: false,
    append(...children) {
      for (const child of children) {
        this.appendChild(child);
      }
    },
    appendChild(child) {
      if (child && typeof child === 'object') {
        child.parent = this;
      }
      this.children.push(child);
      if (this.tagName === 'SELECT' && child.selected) {
        this.value = child.value;
      }
      return child;
    },
    replaceChildren(...children) {
      this.children = [];
      this.value = '';
      for (const child of children) {
        this.appendChild(child);
      }
    },
    querySelector(selector) {
      const matches = (element) => {
        const attributeMatch = selector.match(/^\[([^=]+)="([^"]+)"\]$/);
        if (attributeMatch) {
          return element.attributes[attributeMatch[1]] === attributeMatch[2];
        }
        const attributePresenceMatch = selector.match(/^\[([^=\]]+)\]$/);
        if (attributePresenceMatch) {
          return element.attributes[attributePresenceMatch[1]] !== undefined;
        }
        return false;
      };
      const visit = (element) => {
        if (matches(element)) return element;
        for (const child of element.children) {
          const found = visit(child);
          if (found) return found;
        }
        return null;
      };
      return visit(this);
    },
    querySelectorAll(selector) {
      const results = [];
      const matches = (element) => {
        const attributeMatch = selector.match(/^\[([^=]+)="([^"]+)"\]$/);
        if (attributeMatch) {
          return element.attributes[attributeMatch[1]] === attributeMatch[2];
        }
        const attributePresenceMatch = selector.match(/^\[([^=\]]+)\]$/);
        if (attributePresenceMatch) {
          return element.attributes[attributePresenceMatch[1]] !== undefined;
        }
        return false;
      };
      const visit = (element) => {
        if (matches(element)) results.push(element);
        for (const child of element.children) {
          visit(child);
        }
      };
      visit(this);
      return results;
    },
    closest(selector) {
      const matches = (element) => {
        const attributePresenceMatch = selector.match(/^\[([^=\]]+)\]$/);
        return attributePresenceMatch
          ? element.attributes[attributePresenceMatch[1]] !== undefined
          : false;
      };
      let current = this;
      while (current) {
        if (matches(current)) return current;
        current = current.parent;
      }
      return null;
    },
    addEventListener(type, handler) {
      this.listeners[type] = [...(this.listeners[type] || []), handler];
    },
    setAttribute(name, value) {
      this.attributes[name] = String(value);
      if (name === 'open') this.open = true;
    },
    remove() {
      this.removed = true;
      if (!this.parent) return;
      this.parent.children = this.parent.children.filter((child) => child !== this);
      this.parent = null;
    },
    showModal() {
      this.open = true;
      this.showModalCalled = true;
    },
    close() {
      this.open = false;
      this.closeCalled = true;
    },
    cloneNode(deep) {
      const clone = fakeElement(tagName);
      clone.value = this.value;
      clone.type = this.type;
      clone.checked = this.checked;
      clone.textContent = this.textContent;
      clone.selected = this.selected;
      clone.className = this.className;
      clone.rows = this.rows;
      clone.open = this.open;
      clone.attributes = { ...this.attributes };
      clone.dataset = { ...this.dataset };
      if (deep) {
        for (const child of this.children) {
          clone.appendChild(child && typeof child === 'object' ? child.cloneNode(true) : child);
        }
      }
      return clone;
    },
    get options() {
      return this.children;
    },
    value: '',
    type: '',
    checked: false,
    textContent: '',
    selected: false,
    listeners: {},
    showModalCalled: false,
    closeCalled: false
  };
}

export function withFakeDocument(fn) {
  const previousDocument = globalThis.document;
  globalThis.document = {
    createElement(tagName) {
      return fakeElement(tagName);
    }
  };
  try {
    return fn();
  } finally {
    if (previousDocument === undefined) {
      delete globalThis.document;
    } else {
      globalThis.document = previousDocument;
    }
  }
}
