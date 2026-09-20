/* Shared modeled pace curve. Values are seconds/km; grades are percentages. */
(function (root) {
  'use strict';

  function evaluate(model, grade, factor = 1) {
    if (!model || model.version !== 'asymmetric-quadratic-v1'
        || !['minimum', 'left', 'right', 'optimum', 'x_min', 'x_max'].every(key => Number.isFinite(model[key]))
        || model.minimum <= 0 || model.left < 0 || model.right < 0
        || !Number.isFinite(grade) || !Number.isFinite(factor) || factor <= 0
        || grade < model.x_min || grade > model.x_max) return null;
    const delta = (grade - model.optimum) / 30;
    return (model.minimum + model[delta < 0 ? 'left' : 'right'] * delta * delta) * factor;
  }

  function create(points, model = null, factor = 1) {
    const nodes = (points || []).filter(p => Number.isFinite(p.x) && Number.isFinite(p.pace) && p.pace > 0)
      .map(p => ({ ...p })).sort((a, b) => a.x - b.x)
      .filter((p, i, all) => !i || p.x !== all[i - 1].x);
    // Fitted references use the formula itself, never a second visual smoothing.
    if (model && evaluate(model, model.x_min, factor) !== null) {
      const at = grade => evaluate(model, grade, factor);
      const grades = new Set([model.x_min, model.x_max]);
      for (let x = model.x_min; x < model.x_max; x += .5) grades.add(x);
      if (model.optimum >= model.x_min && model.optimum <= model.x_max) grades.add(model.optimum);
      nodes.forEach(p => { if (at(p.x) !== null) grades.add(p.x); });
      return {
        nodes: nodes.filter(p => at(p.x) !== null).map(p => ({...p, pace: at(p.x)})),
        samples: [...grades].sort((a,b) => a-b).map(x => ({x, pace:at(x)})), at,
      };
    }
    const widths = nodes.slice(1).map((p, i) => p.x - nodes[i].x);
    const slopes = nodes.slice(1).map((p, i) => (p.pace - nodes[i].pace) / widths[i]);
    // Shape-preserving Hermite interpolation: no overshoot between anchors.
    const tangents = nodes.map((p, i) => {
      if (nodes.length < 2) return 0;
      if (i === 0) return slopes[0];
      if (i === nodes.length - 1) return slopes[i - 1];
      const before = slopes[i - 1], after = slopes[i];
      if (before * after <= 0) return 0;
      const w1 = 2 * widths[i] + widths[i - 1];
      const w2 = widths[i] + 2 * widths[i - 1];
      return (w1 + w2) / (w1 / before + w2 / after);
    });
    function at(grade) {
      if (!nodes.length || !Number.isFinite(grade)) return null;
      if (grade <= nodes[0].x) return nodes[0].pace;
      if (grade >= nodes[nodes.length - 1].x) return nodes[nodes.length - 1].pace;
      const right = nodes.findIndex(p => p.x >= grade), left = right - 1;
      const h = widths[left], t = (grade - nodes[left].x) / h;
      return (2*t*t*t - 3*t*t + 1)*nodes[left].pace
        + (t*t*t - 2*t*t + t)*h*tangents[left]
        + (-2*t*t*t + 3*t*t)*nodes[right].pace
        + (t*t*t - t*t)*h*tangents[right];
    }
    const samples = [];
    nodes.forEach((p, i) => {
      if (i) {
        const start = nodes[i - 1].x;
        const steps = Math.ceil((p.x - start) / 0.5);
        for (let j = 1; j < steps; j++) {
          const x = start + (p.x - start) * j / steps;
          samples.push({ x, pace: at(x) });
        }
      }
      samples.push({ ...p });
    });
    return { nodes, samples, at };
  }

  function targetAdjustment(baseMovingSeconds, stopSeconds, targetSeconds) {
    if (targetSeconds == null) return { factor: 1, targetSeconds: null, invalid: false };
    const moving = targetSeconds - Math.max(0, stopSeconds);
    const invalid = !Number.isFinite(moving) || moving < 60 || !Number.isFinite(baseMovingSeconds) || baseMovingSeconds <= 0;
    return { factor: invalid ? 1 : moving / baseMovingSeconds, targetSeconds, invalid };
  }

  const api = { create, evaluate, targetAdjustment };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.PaceCurve = api;
})(globalThis);
