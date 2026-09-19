/* An in-place activity chart: actual measurements, explicit missing intervals. */
(() => {
  const canvas = document.getElementById('glucoseActivityChart');
  const source = document.getElementById('activity-glucose-series');
  if (!canvas || !source || typeof Chart === 'undefined') return;
  const points = JSON.parse(source.textContent);
  const band = {
    id: 'activityGlucoseBand',
    beforeDatasetsDraw(chart) {
      const {ctx, chartArea, scales: {y}} = chart;
      if (!chartArea) return;
      ctx.save();
      const top = y.getPixelForValue(180), bottom = y.getPixelForValue(70);
      ctx.fillStyle = '#edf6f0';
      ctx.fillRect(chartArea.left, top, chartArea.width, bottom - top);
      ctx.strokeStyle = '#92b9a5';
      ctx.setLineDash([4, 4]);
      for (const py of [top, bottom]) {
        ctx.beginPath(); ctx.moveTo(chartArea.left, py); ctx.lineTo(chartArea.right, py); ctx.stroke();
      }
      ctx.restore();
    }
  };
  lazyChart(canvas, {
    type: 'line',
    data: {datasets: [{
      label: 'Glycémie', data: points, parsing: false,
      borderColor: '#388873', borderWidth: 2.5,
      pointRadius: 0, pointHoverRadius: 4, pointHitRadius: 12,
      spanGaps: false, tension: 0, fill: false,
    }]},
    options: {
      animation: false, responsive: true, maintainAspectRatio: false,
      interaction: {mode: 'nearest', axis: 'x', intersect: false},
      plugins: {legend: {display: false}, tooltip: {callbacks: {
        title: items => `${items[0].parsed.x.toFixed(1)} min depuis le départ`,
        label: item => `${Math.round(item.parsed.y)} mg/dL`,
      }}},
      scales: {
        x: {type: 'linear', title: {display: true, text: 'Temps · min'}, grid: {display: false}, ticks: {maxTicksLimit: 7}},
        y: {suggestedMin: 50, suggestedMax: 220, title: {display: true, text: 'mg/dL'}, grid: {color: '#e5ebe7'}, ticks: {maxTicksLimit: 6}},
      },
    },
    plugins: [band],
  });
})();
