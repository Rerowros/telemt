(()=>{'use strict';
// One canonical GET carrier layout shared by every route and the diagnostic sideband.
const get=(()=>{
 let counter=0;
 const keys={'X-Up-Seq':'s','X-Down-Cursor':'c','X-Lane-ID':'l','X-Carrier-Capabilities':'k','X-Carrier-Attempt':'a','X-Carrier-Failure':'f','X-Telemt-Up-Window':'w','X-Telemt-Up-Confirmed':'u'};
 function nonce(){if(counter>=Number.MAX_SAFE_INTEGER)throw new Error('get nonce exhausted');return String(++counter)}
 function encode(data){
  const bytes=data instanceof Uint8Array?data:new Uint8Array(data);
  let binary='';
  for(let i=0;i<bytes.length;i+=8190)binary+=String.fromCharCode.apply(null,bytes.subarray(i,i+8190));
  return btoa(binary).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
 }
 function chunks(data,raw){
  const bytes=data instanceof Uint8Array?data:new Uint8Array(data),parts=[];
  for(let i=0;i<bytes.length;i+=raw)parts.push(encode(bytes.subarray(i,i+raw)));
  return parts;
 }
 function url(target,headers,nonce,close,data,part,total){
  const params=new URLSearchParams();
  if(close)params.set('op','close');
  params.set('t',(headers.Authorization||'').replace(/^Bearer /,''));
  params.set('n',nonce);
  for(const header in keys)if(headers[header]!==undefined)params.set(keys[header],String(headers[header]));
  if(data!==undefined){params.set('d',data);if(total>1){params.set('p',String(part));params.set('pn',String(total))}}
  return target+'?'+params.toString();
 }
 return Object.freeze({nonce,encode,chunks,url});
})();
function create(settings){
 const pause=(milliseconds,signal)=>new Promise((resolve,reject)=>{
  if(signal&&signal.aborted){reject(new Error('request aborted'));return}
  const timer=setTimeout(done,milliseconds);function done(){if(signal)signal.removeEventListener('abort',abort);resolve()}
  function abort(){clearTimeout(timer);signal.removeEventListener('abort',abort);reject(new Error('request aborted'))}
  if(signal)signal.addEventListener('abort',abort,{once:true});
 });
 const options=(method,token,body,headers,signal,keepalive)=>({
  method,body,signal,keepalive:!!keepalive,mode:'same-origin',credentials:'omit',cache:'no-store',redirect:'error',referrerPolicy:'no-referrer',
  headers:Object.assign(token?{Authorization:'Bearer '+token}:{},body?{'Content-Type':'application/octet-stream'}:{},headers||{})
 });
 function retryAfterMs(response){
  const header=response.headers.get('Retry-After');
  if(!header)return 0;
  const seconds=Number(header);
  if(Number.isFinite(seconds)&&seconds>=0)return Math.min(seconds*1000,30000);
  const when=Date.parse(header);
  if(Number.isFinite(when)){const delta=when-Date.now();return delta>0?Math.min(delta,30000):0}
  return 0;
 }
 function retryableStatus(status){return status===408||status===429||status===502||status===503||status===504}
 function responsePolicy(path,status){
  if(path==='/api/v1/session'&&status===200)return {limit:8,exact:true,reason:'protocol'};
  if(path==='/api/v1/down'&&status===200)return {limit:settings.batchLimit(),exact:false,reason:'protocol'};
  if(status===204&&(path==='/api/v1/up'||path==='/api/v1/down'))return {limit:0,exact:true,reason:'protocol'};
  return {limit:0,exact:true,reason:'http'};
 }
 async function send(path,frozenOptions,remainingBudget,maxAttempts,receiver){
  if(settings.method&&settings.method()==='GET')return sendGet(path,frozenOptions,remainingBudget,maxAttempts,receiver);
  let delay=250,attempt=0,lastReason='network';maxAttempts=maxAttempts||9;
  const attempts=()=>typeof maxAttempts==='function'?maxAttempts():maxAttempts;
  const initialBudget=remainingBudget?Math.min(settings.retryMs(),remainingBudget()):settings.retryMs();
  const deadline=Date.now()+Math.max(0,initialBudget),external=frozenOptions.signal;
  const attemptLimit=path==='/api/v1/down'?settings.longPollMs()+settings.requestMs():settings.requestMs();
  while(attempt<attempts()){
   if(settings.closed()||(external&&external.aborted))throw new Error('request aborted');
   const remaining=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);if(remaining<=0)break;attempt++;
   const controller=new AbortController(),abort=()=>controller.abort();let timedOut=false;
   if(external)external.addEventListener('abort',abort,{once:true});
   const requestOptions=Object.assign({},frozenOptions,{signal:controller.signal});
   const timer=setTimeout(()=>{timedOut=true;controller.abort()},Math.max(1,Math.min(attemptLimit,remaining)));
   let response=null,wait=0;
   try{
    const fetched=await fetch(settings.base()+path,requestOptions);
    if(retryableStatus(fetched.status)){
     lastReason='http';wait=retryAfterMs(fetched);settings.cancel(fetched);
    }else{
     const policy=responsePolicy(path,fetched.status);let body;
     try{body=receiver&&(fetched.status===200||fetched.status===204)?await receiver(fetched,controller.signal):await settings.read(fetched,policy.limit,policy.exact,controller.signal);}
     catch(error){
      controller.abort();
      if(external&&external.aborted)throw error;
      if(timedOut)throw settings.failure('timeout','response deadline exceeded');
      throw settings.failure(settings.reason(error,policy.reason),error&&error.message);
     }
     response={status:fetched.status,headers:fetched.headers,body};return response;
    }
   }catch(error){
    controller.abort();
    if(settings.closed()||(external&&external.aborted))throw error;
    if(timedOut)lastReason='timeout';
    if(settings.reason(error,'')==='protocol')throw error;
   }finally{clearTimeout(timer);if(external)external.removeEventListener('abort',abort)}
   const after=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);if(attempt>=attempts()||after<=0)break;
   settings.retrying();
   const backoff=wait||delay+Math.floor(Math.random()*Math.max(1,delay/4));
   await pause(Math.min(backoff,after),external);delay=Math.min(delay*2,2000);
  }
  throw settings.failure(lastReason,'carrier retry limit reached');
 }
 // Fragmented GET uplinks replay from part zero; identical parts are idempotent server-side.
 async function sendGet(path,frozenOptions,remainingBudget,maxAttempts,receiver){
  const headers=frozenOptions.headers||{},close=frozenOptions.method==='DELETE',body=frozenOptions.body;
  const getFrozen=Object.assign({},frozenOptions,{method:'GET',body:undefined});
  const widest='9'.repeat(24);
  let parts=null;
  if(body&&body.byteLength){
   const overhead=get.url(settings.base()+path,headers,widest,close,'',4095,4096).length;
   const raw=Math.floor((settings.getUrlBytes()-overhead)/4)*3;
   if(raw<1)throw settings.failure('protocol','get url budget exceeded');
   parts=get.chunks(body,raw);
   if(parts.length>4096)throw settings.failure('protocol','get part limit exceeded');
  }
  const total=parts?parts.length:1;
  let delay=250,attempt=0,part=0,response=null,lastReason='network';maxAttempts=maxAttempts||9;
  const attempts=()=>typeof maxAttempts==='function'?maxAttempts():maxAttempts;
  // The whole-operation retry budget scales with its part count and stays
  // capped by the absolute outer recovery budget.
  const initialBudget=Math.min(settings.retryMs()*total,remainingBudget?remainingBudget():Infinity);
  const deadline=Date.now()+Math.max(0,initialBudget),external=getFrozen.signal;
  const attemptLimit=path==='/api/v1/down'?settings.longPollMs()+settings.requestMs():settings.requestMs();
  while(part<total&&attempt<attempts()){
   if(settings.closed()||(external&&external.aborted))throw new Error('request aborted');
   const remaining=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);if(remaining<=0)break;attempt++;
   // The URL is composed before the abort listener so a budget or nonce
   // failure can never leave an external listener installed.
   const data=parts?parts[part]:undefined;
   const target=get.url(settings.base()+path,headers,get.nonce(),close,data,part,total);
   if(target.length>settings.getUrlBytes())throw settings.failure('protocol','get url budget exceeded');
   const controller=new AbortController(),abort=()=>controller.abort();let timedOut=false;
   if(external)external.addEventListener('abort',abort,{once:true});
   const requestOptions=Object.assign({},getFrozen,{signal:controller.signal});
   const timer=setTimeout(()=>{timedOut=true;controller.abort()},Math.max(1,Math.min(attemptLimit,remaining)));
   let wait=0;
   try{
    const fetched=await fetch(target,requestOptions);
    if(retryableStatus(fetched.status)){
     lastReason='http';wait=retryAfterMs(fetched);settings.cancel(fetched);
    }else{
     const policy=responsePolicy(path,fetched.status);let payload;
     const partReceiver=part===total-1?receiver:(next,signal)=>settings.read(next,0,true,signal);
     try{payload=partReceiver&&(fetched.status===200||fetched.status===204)?await partReceiver(fetched,controller.signal):await settings.read(fetched,policy.limit,policy.exact,controller.signal);}
     catch(error){
      controller.abort();
      if(external&&external.aborted)throw error;
      if(timedOut)throw settings.failure('timeout','response deadline exceeded');
      throw settings.failure(settings.reason(error,policy.reason),error&&error.message);
     }
     response={status:fetched.status,headers:fetched.headers,body:payload};
     if(part<total-1){
      // A non-204 intermediate response is terminal: the transfer stops and
      // the actual response is returned without sending remaining parts.
      if(response.status!==204)return response;
      if(response.headers.get('X-Up-Part')!==String(part)||response.headers.get('X-Up-Ack')!==null)throw settings.failure('protocol','uplink part rejected');
     }else if(path==='/api/v1/up'&&response.status===204&&response.headers.get('X-Up-Part')!==String(part))throw settings.failure('protocol','uplink part rejected');
     part++;attempt=0;delay=250;continue;
    }
   }catch(error){
    controller.abort();
    if(settings.closed()||(external&&external.aborted))throw error;
    if(timedOut)lastReason='timeout';
    if(settings.reason(error,'')==='protocol')throw error;
   }finally{clearTimeout(timer);if(external)external.removeEventListener('abort',abort)}
   const after=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);if(attempt>=attempts()||after<=0)break;
   settings.retrying();
   const backoff=wait||delay+Math.floor(Math.random()*Math.max(1,delay/4));
   await pause(Math.min(backoff,after),external);delay=Math.min(delay*2,2000);
  }
  if(part>=total)return response;
  throw settings.failure(lastReason,'carrier retry limit reached');
 }
 function url(path,frozen,close){
  if(!(settings.method&&settings.method()==='GET'))return path;
  const headers=frozen.headers||{},data=frozen.body?get.encode(frozen.body):undefined;
  const target=get.url(settings.base()+path,headers,get.nonce(),close||frozen.method==='DELETE',data,0,1);
  if(target.length>settings.getUrlBytes())throw settings.failure('protocol','get url budget exceeded');
  return target;
 }
 return Object.freeze({options,pause,send,url});
}
globalThis.TelemtBridgeRequest=Object.freeze({create,get});
})();
