(() => {
  const script = document.currentScript;
  const pipelineId = script ? script.dataset.pipelineId : '';
  if (!pipelineId) return;

  const events = new EventSource(`/pipelines/${encodeURIComponent(pipelineId)}/events`);
  events.onmessage = (message) => {
    console.log(message.data);
  };
})();
