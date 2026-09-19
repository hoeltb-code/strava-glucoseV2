(function(root){
  'use strict';
  function terrain(weight,grade,pace,distance=1,gait='run') {
    if (![weight,grade,pace,distance].every(Number.isFinite) || weight<20 || weight>300 || Math.abs(grade)>45 || pace<=0 || distance<0 || !['run','walk'].includes(gait)) return null;
    const g=grade/100;
    const cost=gait==='walk' ? 280.5*g**5-58.7*g**4-76.8*g**3+51.9*g**2+19.6*g+2.5 : 155.4*g**5-30.4*g**4-43.3*g**3+46.3*g**2+19.5*g+3.6;
    const perKm=cost*weight*1000/4184;
    return {kcal:perKm*distance,kcal_km:perKm,kcal_hour:perKm*3600/pace,flat_ratio:cost/(gait==='walk'?2.5:3.6)};
  }
  function cardiac(weight,hr,age,sex,maxHr) {
    if (![weight,hr,age,maxHr].every(Number.isFinite)||weight<47||weight>120||age<18||age>45||maxHr<=0||hr/maxHr<.57||hr/maxHr>.90) return null;
    let kj;
    if(['m','male','homme'].includes(String(sex).toLowerCase())) kj=-55.0969+.6309*hr+.1988*weight+.2017*age;
    else if(['f','female','femme'].includes(String(sex).toLowerCase())) kj=-20.4022+.4472*hr-.1263*weight+.074*age;
    else return null;
    return kj>0?kj/4.184*60:null;
  }
  const api={terrain,cardiac};
  if(typeof module!=='undefined'&&module.exports) {module.exports=api;return;}
  root.EnergyModel=api;
  document.addEventListener('DOMContentLoaded',()=>document.querySelectorAll('[data-energy-explorer]').forEach(host=>{
    const input=name=>host.querySelector(`[name="${name}"]`);
    const out=name=>host.querySelector(`[data-energy-value="${name}"]`);
    const chart=host.querySelector('svg');
    const config=JSON.parse(host.querySelector('[type="application/json"]').textContent);
    const reference=config.reference||{};
    const centers={Sneg40p:-45,Sneg30_40:-35,Sneg25_30:-27.5,Sneg20_25:-22.5,Sneg15_20:-17.5,Sneg10_15:-12.5,Sneg5_10:-7.5,Sneg0_5:-2.5,S0_5:2.5,S5_10:7.5,S10_15:12.5,S15_20:17.5,S20_25:22.5,S25_30:27.5,S30_40:35,S40p:45};
    const paceLabel=seconds=>`${Math.floor(Math.round(seconds)/60)}:${String(Math.round(seconds)%60).padStart(2,'0')}/km`;
    function render(){
      const weight=Number(input('weight').value),grade=Number(input('grade').value),hr=Number(input('heart_rate').value);
      const maxHr=Number(config.max_hr)||0;
      const zone=hr/maxHr<.6?'Zone 1':hr/maxHr<.7?'Zone 2':hr/maxHr<.8?'Zone 3':hr/maxHr<.9?'Zone 4':'Zone 5';
      let pace=Number(input('pace').value)*60;
      let referenceUsed=false;
      if(input('reference').checked && maxHr && root.PaceCurve){
        const curve=root.PaceCurve.create(Object.entries(centers).map(([id,x])=>({x,pace:Number(reference[id]?.[zone])})));
        const slopeId=Object.entries(centers).reduce((best,item)=>Math.abs(item[1]-grade)<Math.abs(best[1]-grade)?item:best)[0];
        const modeled=Number(reference[slopeId]?.[zone])>0?curve.at(grade):null;
        if(modeled>0){pace=modeled;referenceUsed=true;}
      }
      input('pace').disabled=referenceUsed;
      const result=terrain(weight,grade,pace,1,input('gait').value);
      out('weight').textContent=weight+' kg';out('grade').textContent=(grade>0?'+':'')+grade+' %';
      out('pace').textContent=paceLabel(pace);out('heart_rate').textContent=hr+' bpm';
      out('source').textContent=referenceUsed?`${zone} · allure issue de la courbe de référence`:'Allure simulée · réglage libre';
      out('per_km').textContent=result?Math.round(result.kcal_km):'—';out('per_hour').textContent=result?Math.round(result.kcal_hour):'—';
      out('ratio').textContent=result?result.flat_ratio.toFixed(1)+' ×':'—';
      out('intensity').textContent=maxHr?`${Math.round(hr/maxHr*100)} % de FC max · ${zone}`:'Renseigne ta FC maximale dans ton compte pour situer l’intensité.';
      const cardio=cardiac(weight,hr,Number(config.age),config.sex,maxHr);
      out('cardiac').textContent=cardio?`Repère cardio indépendant : ≈ ${Math.round(cardio)} kcal/h. Ne pas l’ajouter au modèle terrain.`:'Repère cardio indisponible : âge, sexe, poids ou intensité hors du domaine de l’équation.';
      out('message').textContent=!result?'Paramètres hors du domaine du modèle.':grade>3?'La montée augmente le coût de chaque kilomètre. Ralentir réduit les kcal par heure, sans effacer le coût du relief.':grade<-20?'La descente raide coûte à nouveau davantage : le freinage musculaire compte aussi.':grade<-3?'Une descente modérée peut coûter moins que le plat. Le terrain technique et la fatigue ne sont pas mesurés ici.':'Sur le plat, accélérer augmente surtout les kcal par heure : davantage de kilomètres sont parcourus.';
      const w=Math.max(260,chart.clientWidth),h=220,left=48,right=20,top=18,bottom=38;
      chart.setAttribute('viewBox',`0 0 ${w} ${h}`);
      const values=Array.from({length:91},(_,i)=>({x:i-45,y:terrain(weight,i-45,pace,1,input('gait').value)?.kcal_km||0}));
      const max=Math.max(...values.map(p=>p.y))*1.1;
      const x=v=>left+(v+45)/90*(w-left-right),y=v=>h-bottom-v/max*(h-top-bottom);
      const grid=[0,.5,1].map(r=>`<line x1="${left}" x2="${w-right}" y1="${y(max*r)}" y2="${y(max*r)}" stroke="#dce5df"/><text x="${left-7}" y="${y(max*r)+4}" text-anchor="end">${Math.round(max*r)}</text>`).join('');
      const path=values.map((p,i)=>`${i?'L':'M'}${x(p.x)},${y(p.y)}`).join(' ');
      chart.innerHTML=`<title>Coût estimé par kilomètre selon la pente</title>${grid}<path d="${path}" fill="none" stroke="#287b65" stroke-width="3"/>${result?`<circle cx="${x(grade)}" cy="${y(result.kcal_km)}" r="6" fill="#d99529"/>`:''}${[-40,0,40].map(g=>`<text x="${x(g)}" y="${h-18}" text-anchor="middle">${g>0?'+':''}${g}%</text>`).join('')}<text x="0" y="12">kcal/km</text><text x="${w-right}" y="${h-1}" text-anchor="end">Pente</text>`;
    }
    host.addEventListener('input',render);host.addEventListener('change',render);
    host.querySelectorAll('[data-energy-preset]').forEach(button=>button.addEventListener('click',()=>{
      input('grade').value=button.dataset.energyPreset;render();
    }));
    new ResizeObserver(render).observe(chart);render();
  }));
})(globalThis);
