const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const PaceCurve = require('../static/js/pace-curve.js');
const points = [{ x:-45, pace:1500 }, { x:-27.5, pace:620 }, { x:-7.5, pace:330 }, { x:2.5, pace:360 }, { x:17.5, pace:790 }, { x:45, pace:1600 }];
const near = (actual, expected) => assert.ok(Math.abs(actual - expected) < 1e-8, `${actual} != ${expected}`);

test('curve preserves modeled anchors, extrema and positive paces on irregular grades', () => {
  const curve = PaceCurve.create(points);
  points.forEach(p => near(curve.at(p.x), p.pace));
  for (let i=1;i<points.length;i++) {
    const left=points[i-1], right=points[i];
    for (let x=left.x;x<right.x;x+=0.1) {
      assert.ok(curve.at(x) >= Math.min(left.pace,right.pace)-1e-8);
      assert.ok(curve.at(x) <= Math.max(left.pace,right.pace)+1e-8);
    }
  }
  near(curve.at(-60),points[0].pace);
  near(curve.at(60),points.at(-1).pace);
});

test('target time and changed aid stops scale the complete curve without changing its shape', () => {
  const distances=[1,3,4,7,2,1];
  const baseSeconds=points.reduce((sum,p,i)=>sum+p.pace*distances[i],0);
  const base=PaceCurve.create(points);
  for (const stops of [0,900,1800]) {
    const adjustment=PaceCurve.targetAdjustment(baseSeconds,stops,18000);
    assert.equal(adjustment.invalid,false);
    const scaled=PaceCurve.create(points.map(p=>({...p,pace:p.pace*adjustment.factor})));
    near(points.reduce((sum,p,i)=>sum+scaled.at(p.x)*distances[i],0)+stops,18000);
    for(let grade=-45;grade<=45;grade+=0.25) near(scaled.at(grade),base.at(grade)*adjustment.factor);
  }
  assert.equal(PaceCurve.targetAdjustment(baseSeconds,18000,18000).invalid,true);
  assert.equal(PaceCurve.targetAdjustment(0,0,18000).invalid,true);
  near(PaceCurve.targetAdjustment(baseSeconds,900,null).factor,1);
});

test('no missing profile is silently replaced with made-up paces', () => {
  assert.equal(PaceCurve.create([]).at(0),null);
  const curve=PaceCurve.create([{x:0,pace:NaN},{x:1,pace:-10},{x:2,pace:360}]);
  near(curve.at(50),360);
});

const template=fs.readFileSync('templates/user_dashboard.html','utf8');
function functionSource(name) {
  const start=template.indexOf(`    function ${name}(`);
  assert.ok(start>=0,name);
  const end=template.indexOf('\n    function ',start+1);
  return template.slice(start,end);
}

test('actual plan renderer uses the modeled curve in automatic and target modes, with accurate hover coordinates', () => {
  const listeners={};
  const tooltip={classList:{toggle(){}},style:{}};
  const svg={addEventListener:(name,fn)=>listeners[name]=fn,getBoundingClientRect:()=>({left:0,top:0,width:680,height:220})};
  const curveBox={querySelector:selector=>selector==='svg'?svg:tooltip};
  const host={hidden:true,innerHTML:'',querySelector:()=>curveBox};
  const context={PaceCurve,paceReference:{sources:{}},targetPacingCurveHost:host,targetPaceMultiplier:1,
    manualProfileIsActive:()=>false,manualPaceForSlope:()=>9999,
    runnerModeledPaceLookup:Object.fromEntries(points.map((p,i)=>['s'+i,{'Zone 2':p.pace}])),
    // Deliberately divergent observations: they must never leak into the plan.
    runnerProfileData:{zones:{'Zone 2':{s0:{avg_pace_s_per_km:9999}}}},
    slopeIds:points.map((p,i)=>'s'+i),slopeCenters:Object.fromEntries(points.map((p,i)=>['s'+i,p.x])),
    slopeLabels:Object.fromEntries(points.map((p,i)=>['s'+i,`${p.x}%`])),
    paceWithCourseDifficulty:(pace,coefficient)=>pace*(2-coefficient/100),
    requestedTargetSeconds:()=>null,formatDuration:seconds=>`${seconds}s`,
  };
  vm.createContext(context);
  for(const name of ['projectedPaceForSlope','formatPace','paceScaleBounds','paceScaleTicks','renderTargetPacingCurve']) vm.runInContext(functionSource(name),context);
  const render=()=>context.renderTargetPacingCurve({zone:'Zone 2',courseDifficulty:100,movingSeconds:12000,stopSeconds:900});
  render();
  assert.equal(host.hidden,false);
  assert.match(host.innerHTML,/Allures recalculées pour ce parcours/);
  const path=host.innerHTML.match(/class="curve-line" d="([^"]+)"/)[1];
  assert.ok((path.match(/L/g)||[]).length>100,'smooth samples must replace the few straight segments');
  listeners.pointermove({clientX:52,clientY:100});
  assert.match(tooltip.textContent,/-45.0 % · 25:00/);
  listeners.pointermove({clientX:664,clientY:100});
  assert.match(tooltip.textContent,/\+45.0 % · 26:40/);
  context.requestedTargetSeconds=()=>18000;
  context.targetPaceMultiplier=1.2;
  render();
  assert.match(host.innerHTML,/Allures recalculées pour le chrono cible/);
  listeners.pointermove({clientX:52,clientY:100});
  assert.match(tooltip.textContent,/-45.0 % · 30:00/);
  context.targetPaceMultiplier=0.02;
  render();
  listeners.pointermove({clientX:52,clientY:100});
  assert.match(tooltip.textContent,/-45.0 % · 0:30/,'display must not silently clamp the time multiplier');
  context.paceReference.curves = {'Zone 2':formula};
  context.runnerModeledPaceLookup = Object.fromEntries(points.map((p,i)=>['s'+i,{'Zone 2':PaceCurve.evaluate(formula,p.x)}]));
  context.targetPaceMultiplier = 1.2;
  render();
  listeners.pointermove({clientX:52+33/90*612,clientY:100});
  assert.match(tooltip.textContent,/-12.0 % · 6:00/,'target curve must evaluate the global formula at its optimum');
  assert.equal(context.projectedPaceForSlope('s0','Zone 2').pace,PaceCurve.evaluate(formula,-45));
  delete context.runnerModeledPaceLookup.s0;
  assert.equal(context.projectedPaceForSlope('s0','Zone 2').pace,null);
});

test('profile GPX projection uses modeled paces for slope totals and kilometer splits, and rejects a missing model', async () => {
  const profileTemplate=fs.readFileSync('templates/runner_profile.html','utf8');
  const start=profileTemplate.indexOf('  const runnerHrZones =');
  const source=profileTemplate.slice(start,profileTemplate.indexOf('</script>',start));
  const fixture={hr_zones:['Zone 2'],
    modeled_pace_lookup_by_slope:{S0_5:{'Zone 2':360},S5_10:{'Zone 2':600}},
    pace_lookup_by_slope:{S0_5:{'Zone 2':9999},S5_10:{'Zone 2':9999}}};
  const script=source.replace(/\{\{\s*(\w+)[\s\S]*?\}\}/g,(_,key)=>JSON.stringify(fixture[key]??null));
  const parts=[{slope_id:'S0_5',label:'0–5 %',distance_km:.5},{slope_id:'S5_10',label:'5–10 %',distance_km:.5}];
  const segment={km_index:1,distance_km:1,cumulative_km:1,slope_distribution:parts};
  const result={innerHTML:''},feedback={textContent:''};
  const context={FormData:class {append(){}},
    document:{getElementById:id=>({'projection-gpx':{files:[{}]},'projection-feedback':feedback,'projection-result':result}[id]||null)},
    fetch:async()=>({ok:true,json:async()=>({total_distance_km:1,slope_distribution:parts,km_splits:[segment]})}),
  };
  vm.createContext(context);vm.runInContext(script,context);
  context.segment=segment;
  const times=vm.runInContext('computeZoneTimesForSplit(segment,runnerPaceLookup)',context);
  near(times['Zone 2'],480);
  await context.runSlopeAnalysis(1,'run','all');
  assert.equal(feedback.textContent,'');
  assert.match(result.innerHTML,/8:00/);
  assert.doesNotMatch(result.innerHTML,/2h46/);
  vm.runInContext('delete runnerPaceLookup.S5_10',context);
  await context.runSlopeAnalysis(1,'run','all');
  assert.match(feedback.textContent,/modèle d’allure est insuffisant/);
  assert.equal(result.innerHTML,'');
});

const formula = {version:'asymmetric-quadratic-v1',minimum:300,left:400,right:260,optimum:-12,x_min:-45,x_max:45};
test('fitted formula has one rounded optimum and overrides noisy graphic anchors', () => {
  const curve=PaceCurve.create(points,formula);
  near(curve.at(-12),300);
  near(curve.at(-42),700);
  near(curve.at(18),560);
  assert.notEqual(curve.at(-45),points[0].pace);
  assert.equal(curve.at(-46),null);
  assert.equal(curve.at(46),null);
  assert.equal(PaceCurve.evaluate({...formula,minimum:-1},0),null);
  for (let x=-44.9;x<=45;x+=.1) {
    if(x<=-12)assert.ok(curve.at(x)<=curve.at(x-.1)+1e-8);
    if(x>=-11.9)assert.ok(curve.at(x)>=curve.at(x-.1)-1e-8);
  }
  const distances=[1,3,4,7,2,1];
  const base=points.reduce((sum,p,i)=>sum+curve.at(p.x)*distances[i],0);
  for(const stops of [0,900,1800]) {
    const {factor}=PaceCurve.targetAdjustment(base,stops,18000);
    const adjusted=PaceCurve.create(points,formula,factor);
    near(points.reduce((sum,p,i)=>sum+adjusted.at(p.x)*distances[i],0)+stops,18000);
    for(let x=-45;x<=45;x+=.25)near(adjusted.at(x),curve.at(x)*factor);
  }
});

test('profile bar and curve views use the same model and update with the selected cardio zone', () => {
  const profileTemplate=fs.readFileSync('templates/runner_profile.html','utf8');
  const start=profileTemplate.indexOf('              const renderPaceCurve =');
  const end=profileTemplate.indexOf('              const renderVamBars =',start);
  const listeners={}, buttons=['bar','line'].map(mode=>({dataset:{paceView:mode},attributes:{},setAttribute(k,v){this.attributes[k]=v;},addEventListener(event,fn){this.click=fn;}}));
  const select={value:'Zone 2',addEventListener:(event,fn)=>listeners[event]=fn};
  const canvas={getContext:()=>({}),setAttribute(){}};
  const nodes={paceSlopeChart:canvas,paceZone:select,'pace-chart-title':{},'pace-chart-reading':{},'pace-terrain-legend':{},'pace-reference-source':{}};
  let config, destroyed=0;
  const context={PaceCurve,
    document:{getElementById:id=>nodes[id],querySelectorAll:()=>buttons},
    slopesOrder:[['down','−10 à −5 %'],['flat','0–5 %'],['up','10–15 %'],['missing','>40 %']],
    slopeCenters:{down:-7.5,flat:2.5,up:12.5,missing:45},
    paceReference:{sources:{},curves:{'Zone 2':formula,'Zone 3':{...formula,minimum:270,left:360,right:234}}},
    modeledPaceFor:(zone,id)=>id==='missing'?null:PaceCurve.evaluate(formula,{down:-7.5,flat:2.5,up:12.5}[id])*(zone==='Zone 3'?.9:1),
    barColorFor:id=>({down:'#63b7d3',flat:'#84a964',up:'#f3ad3d'}[id]||'#f3ad3d'),
    lazyChart:(target,configuration)=>{config=configuration;return{destroy(){destroyed++;}};},
  };
  vm.createContext(context);vm.runInContext(profileTemplate.slice(start,end),context);
  assert.equal(config.type,'bar');
  assert.equal(config.options.scales.y.beginAtZero,true);
  assert.equal(config.options.scales.y.reverse,false);
  assert.equal(config.data.datasets[0].data[3],null,'missing pace must not become a zero-height estimate');
  const original=config.data.datasets[0].data.slice();
  buttons[1].click();
  assert.equal(config.type,'line');assert.equal(buttons[1].attributes['aria-pressed'],'true');
  assert.equal(config.options.scales.y.reverse,true);
  near(config.data.datasets[0].data.find(p=>p.x===-7.5).y,original[0]);
  select.value='Zone 3';listeners.change();
  near(config.data.datasets[0].data.find(p=>p.x===-7.5).y,original[0]*.9);
  buttons[0].click();
  near(config.data.datasets[0].data[0],original[0]*.9);
  assert.equal(config.options.plugins.tooltip.callbacks.title([{dataIndex:0}]),'−10 à −5 %');
  assert.equal(destroyed,3,'switching views must replace rather than stack canvas charts');
});
