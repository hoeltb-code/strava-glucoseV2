const {test}=require('node:test');const assert=require('node:assert/strict');
const {terrain,cardiac}=require('../static/js/energy.js');
const {intervalStats}=require('../static/js/activity-data.js');
test('terrain keeps cost per distance constant when target pace changes',()=>{
 const a=terrain(70,10,500,2),b=terrain(70,10,250,2);
 assert.equal(a.kcal,b.kcal);assert.equal(b.kcal_hour,a.kcal_hour*2);
 assert.equal(terrain(70,46,500),null);assert.equal(terrain(70,0,0),null);
 assert.equal(cardiac(70,150,60,'male',190),null);
 assert.ok(cardiac(70,150,35,'male',190)>0);
});
test('selected glucose interval respects gaps, boundaries and time weighting',()=>{
 const p=[{start:0,end:1,mgdl:60},{start:1,end:3,mgdl:180},{start:10,end:11,mgdl:200}];
 const a=intervalStats(p,0,11);assert.equal(a.observed,4);assert.equal(a.tir,50);assert.equal(a.low,1);
 assert.equal(intervalStats(p,3,10).tir,null);assert.equal(intervalStats(p,2,1),null);
 assert.equal(intervalStats(p,1.5,2.5).tir,100);
});
