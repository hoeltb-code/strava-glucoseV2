/* Render advanced charts only as they enter the viewport. */
window.lazyChart=function(target,config){
  const canvas=target?.canvas||target;
  if(!canvas||typeof Chart==='undefined')return;
  if(!('IntersectionObserver' in window))return new Chart(target,config);
  let chart=null;
  const observer=new IntersectionObserver(entries=>{if(entries.some(e=>e.isIntersecting)){observer.disconnect();chart=new Chart(target,config);}},{rootMargin:'150px'});
  observer.observe(canvas);
  return {get data(){return chart?chart.data:config.data;},get options(){return chart?chart.options:config.options;},update(mode){chart?.update(mode);},resize(){chart?.resize();},destroy(){observer.disconnect();chart?.destroy();}};
};
let activityMapLibrary;
window.loadActivityMapLibrary=function(){
  if(window.maplibregl)return Promise.resolve();
  if(activityMapLibrary)return activityMapLibrary;
  activityMapLibrary=new Promise((resolve,reject)=>{
    const css=document.createElement('link');css.rel='stylesheet';css.href='https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.css';document.head.append(css);
    const script=document.createElement('script');script.src='https://unpkg.com/maplibre-gl@5.24.0/dist/maplibre-gl.js';script.onload=resolve;script.onerror=()=>{activityMapLibrary=null;reject(new Error('Carte indisponible'));};document.head.append(script);
  });return activityMapLibrary;
};
