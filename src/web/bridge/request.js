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
 // Raw fragment views; each part is encoded only when its request is built
 // so a large batch never blocks the page thread in one encoding burst.
 function slices(data,raw){
  const bytes=data instanceof Uint8Array?data:new Uint8Array(data),parts=[];
  for(let i=0;i<bytes.length;i+=raw)parts.push(bytes.subarray(i,i+raw));
  return parts;
 }
 function chunks(data,raw){return slices(data,raw).map(encode)}
 function url(target,headers,nonce,close,data,part,total){
  const params=new URLSearchParams();
  if(close)params.set('op','close');
  params.set('t',(headers.Authorization||'').replace(/^Bearer /,''));
  params.set('n',nonce);
  for(const header in keys)if(headers[header]!==undefined)params.set(keys[header],String(headers[header]));
  if(data!==undefined){params.set('d',data);if(total>1){params.set('p',String(part));params.set('pn',String(total))}}
  return target+'?'+params.toString();
 }
 return Object.freeze({nonce,encode,slices,chunks,url});
})();
function create(settings){
 const pause=(milliseconds,signal)=>new Promise((resolve,reject)=>{
  if(signal&&signal.aborted){reject(new Error('request aborted'));return}
  const timer=setTimeout(done,milliseconds);function done(){if(signal)signal.removeEventListener('abort',abort);resolve()}
  function abort(){clearTimeout(timer);signal.removeEventListener('abort',abort);reject(new Error('request aborted'))}
  if(signal)signal.addEventListener('abort',abort,{once:true});
 });
 // Combines two abort signals so queued and in-flight parts share one trigger.
 const mergeSignals=(a,b)=>{
  if(!a||!b)return a||b;
  const merged=new AbortController(),fire=()=>merged.abort();
  a.addEventListener('abort',fire,{once:true});b.addEventListener('abort',fire,{once:true});
  if(a.aborted||b.aborted)fire();
  return merged.signal;
 };
 // Page-wide GET uplink scheduler: multiplexed edges allow the configured K
 // parts per operation with a shared ceiling, while HTTP/1.1 edges preserve
 // the browser six-connection envelope for the polls that share it.
 const getScheduler=(()=>{
  let protocol='',upInflight=0,bypassInflight=0,downInflight=0,activeOps=0,ranks=0,deferred=null;const queue=[];
  // The last same-origin API resource entry pins the hop protocol; before
  // any API exchange completes the conservative HTTP/1.1 envelope applies.
  function detect(){
   if(protocol)return;
   try{
    const prefix=settings.base()+'/api/',entries=performance.getEntriesByType('resource');
    for(let i=entries.length-1;i>=0;i--){const entry=entries[i];if(entry&&typeof entry.name==='string'&&entry.name.startsWith(prefix)&&entry.nextHopProtocol){protocol=entry.nextHopProtocol;return}}
   }catch(error){}
  }
  function multiplexed(){return protocol==='h2'||protocol==='h3'||protocol==='http/2'||protocol==='http/3'}
  // HTTP/1.1 parts share what the held long polls and in-flight unpooled
  // uplinks (single-part operations, closing parts) leave of the six
  // connections, minus one kept free so control traffic never waits for a
  // bulk fragment to finish; at least one part always progresses.
  function cap(){detect();return multiplexed()?Math.min((settings.parallelParts?settings.parallelParts():1)*Math.max(1,activeOps),32):Math.max(1,6-downInflight-bypassInflight-1)}
  // The oldest operation is served first so conveyor heads complete before
  // later operations and the window keeps advancing.
  function pump(){
   while(queue.length&&upInflight<cap()){
    let best=0;for(let i=1;i<queue.length;i++)if(queue[i].rank<queue[best].rank)best=i;
    upInflight++;queue.splice(best,1)[0].grant();
   }
  }
  function acquire(signal,rank){
   return new Promise((resolve,reject)=>{
    if(signal&&signal.aborted){reject(new Error('request aborted'));return}
    const entry={rank,grant:()=>{if(signal)signal.removeEventListener('abort',abort);resolve()}};
    function abort(){const index=queue.indexOf(entry);if(index>=0)queue.splice(index,1);reject(new Error('request aborted'))}
    if(signal)signal.addEventListener('abort',abort,{once:true});
    queue.push(entry);pump();
   });
  }
  // The last acknowledged fragment of an operation hands its connection to
  // the closing part that follows at once, so no queued fragment takes it.
  function release(handoff){upInflight--;if(!handoff)pump()}
  function rank(){return ++ranks}
  function bypassOpen(){bypassInflight++}
  function bypassClose(){bypassInflight--;pump()}
  function downOpen(){downInflight++}
  // A finished poll usually re-polls within the same task; granting its
  // connection only after that lets the next poll keep it instead of
  // queueing behind a fragment inside the browser connection pool.
  function downClose(){downInflight--;if(deferred===null)deferred=setTimeout(()=>{deferred=null;pump()},0)}
  function opOpen(){activeOps++}
  function opClose(){activeOps--;pump()}
  return{acquire,release,rank,bypassOpen,bypassClose,downOpen,downClose,opOpen,opClose};
 })();
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
 // Non-final GET fragments run through a bounded worker pool where each part
 // retries inside the shared operation budget and replays stay idempotent.
 async function sendGet(path,frozenOptions,remainingBudget,maxAttempts,receiver){
  const headers=frozenOptions.headers||{},close=frozenOptions.method==='DELETE',body=frozenOptions.body;
  const getFrozen=Object.assign({},frozenOptions,{method:'GET',body:undefined});
  const widest='9'.repeat(24);
  let parts=null;
  if(body&&body.byteLength){
   const overhead=get.url(settings.base()+path,headers,widest,close,'',4095,4096).length;
   const raw=Math.floor((settings.getUrlBytes()-overhead)/4)*3;
   if(raw<1)throw settings.failure('protocol','get url budget exceeded');
   parts=get.slices(body,raw);
   if(parts.length>4096)throw settings.failure('protocol','get part limit exceeded');
  }
  const total=parts?parts.length:1;maxAttempts=maxAttempts||9;
  const attempts=()=>typeof maxAttempts==='function'?maxAttempts():maxAttempts;
  // The whole-operation retry budget scales with its part count and stays
  // capped by the absolute outer recovery budget.
  const initialBudget=Math.min(settings.retryMs()*total,remainingBudget?remainingBudget():Infinity);
  const deadline=Date.now()+Math.max(0,initialBudget),external=getFrozen.signal;
  const attemptLimit=path==='/api/v1/down'?settings.longPollMs()+settings.requestMs():settings.requestMs();
  const uplink=path==='/api/v1/up',downlink=path==='/api/v1/down';
  let rank=0,partsAcked=0;
  if(downlink)getScheduler.downOpen();
  try{
   // One physical attempt for one part; the op signal additionally cancels
   // sibling parts when a terminal response arrives early.
   async function attempt(part,opSignal){
    const remaining=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);
    if(remaining<=0)return{kind:'budget'};
    // The URL is composed before the abort listener so a budget or nonce
    // failure can never leave an external listener installed.
    const data=parts?get.encode(parts[part]):undefined;
    const target=get.url(settings.base()+path,headers,get.nonce(),close,data,part,total);
    if(target.length>settings.getUrlBytes())throw settings.failure('protocol','get url budget exceeded');
    const controller=new AbortController(),abort=()=>controller.abort();let timedOut=false,slot=false,bypass=false,acked=false;
    if(external)external.addEventListener('abort',abort,{once:true});
    if(opSignal)opSignal.addEventListener('abort',abort,{once:true});
    const requestOptions=Object.assign({},getFrozen,{signal:controller.signal});
    let wait=0,timer=null;
    try{
     // Only non-final parts of a fragmented uplink share the parallel pool:
     // single-part control traffic and the closing part go around it so
     // small requests never queue behind a bulk upload; they still count
     // against the HTTP/1.1 envelope while they are in flight.
     if(uplink&&total>1&&part<total-1){await getScheduler.acquire(controller.signal,rank);slot=true}
     else if(uplink){getScheduler.bypassOpen();bypass=true}
     // The request deadline covers the network exchange only: time spent
     // queued behind an older operation is bounded by the operation budget.
     const left=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);
     if(left<=0)return{kind:'budget'};
     timer=setTimeout(()=>{timedOut=true;controller.abort()},Math.max(1,Math.min(attemptLimit,left)));
     const fetched=await fetch(target,requestOptions);
     if(retryableStatus(fetched.status)){wait=retryAfterMs(fetched);settings.cancel(fetched);return{kind:'retry',reason:'http',wait}}
     const policy=responsePolicy(path,fetched.status);let payload;
     const partReceiver=part===total-1?receiver:(next,signal)=>settings.read(next,0,true,signal);
     try{payload=partReceiver&&(fetched.status===200||fetched.status===204)?await partReceiver(fetched,controller.signal):await settings.read(fetched,policy.limit,policy.exact,controller.signal);}
     catch(error){
      controller.abort();
      if(external&&external.aborted)throw error;
      if(opSignal&&opSignal.aborted)throw error;
      if(timedOut)throw settings.failure('timeout','response deadline exceeded');
      throw settings.failure(settings.reason(error,policy.reason),error&&error.message);
     }
     const response={status:fetched.status,headers:fetched.headers,body:payload};
     if(part<total-1){
      // A non-204 intermediate response is terminal: the transfer stops and
      // the actual response is returned without sending remaining parts.
      if(response.status!==204)return{kind:'terminal',response};
      if(response.headers.get('X-Up-Part')!==String(part)||response.headers.get('X-Up-Ack')!==null)throw settings.failure('protocol','uplink part rejected');
      acked=true;partsAcked++;
      return{kind:'part'};
     }
     if(uplink&&response.status===204&&response.headers.get('X-Up-Part')!==String(part))throw settings.failure('protocol','uplink part rejected');
     return{kind:'done',response};
    }catch(error){
     controller.abort();
     if(settings.closed()||(external&&external.aborted)||(opSignal&&opSignal.aborted))throw error;
     if(settings.reason(error,'')==='protocol')throw error;
     if(timedOut)return{kind:'retry',reason:'timeout',wait:0};
     return{kind:'retry',reason:'network',wait};
    }finally{
     if(timer!==null)clearTimeout(timer);
     if(slot)getScheduler.release(acked&&partsAcked===total-1);
     if(bypass)getScheduler.bypassClose();
     if(external)external.removeEventListener('abort',abort);
     if(opSignal)opSignal.removeEventListener('abort',abort);
    }
   }
   // Retries one fragment inside the shared operation budget until the part
   // attempt cap ends it; terminal and aborted outcomes unwind immediately.
   async function sendPart(part,opSignal){
    // Every GET exchange is one short request, so a multiplexed edge that
    // retires its connection (an HTTP/2 GOAWAY after its per-connection
    // request cap) can fail one in-flight request without any carrier fault.
    // Idempotent uplink parts and downlink polls replay that request once at
    // once instead of escalating the whole operation to session recovery.
    let delay=250,attemptCount=0,lastReason='network',reissue=uplink||downlink?1:0;
    for(;;){
     if(settings.closed()||(external&&external.aborted))throw new Error('request aborted');
     if(opSignal&&opSignal.aborted)return{kind:'stop',reason:'cancelled',lastReason};
     if(attemptCount>=attempts())return{kind:'stop',reason:'attempts',lastReason};
     const outcome=await attempt(part,opSignal);
     if(outcome.kind!=='retry'&&outcome.kind!=='budget')return outcome;
     if(outcome.kind==='retry'&&outcome.reason==='network'&&reissue>0){reissue--;continue}
     lastReason=outcome.reason||lastReason;
     // Retryable statuses (503 window backpressure above all) wait out the
     // operation deadline instead of spending the attempt cap; transport
     // failures still escalate to the shared recovery path.
     if(outcome.reason!=='http')attemptCount++;
     if(outcome.kind==='budget'||attemptCount>=attempts())return{kind:'stop',reason:'budget',lastReason};
     const after=Math.min(deadline-Date.now(),remainingBudget?remainingBudget():Infinity);
     if(after<=0)return{kind:'stop',reason:'budget',lastReason};
     settings.retrying();
     const backoff=outcome.wait||delay+Math.floor(Math.random()*Math.max(1,delay/4));
     await pause(Math.min(backoff,after),mergeSignals(external,opSignal));
     delay=Math.min(delay*2,2000);
    }
   }
   if(uplink&&parts&&total>1){
    // Non-final parts run through a bounded worker pool; the final part is
    // sent only after every earlier fragment is acknowledged server-side.
    const opAbort=new AbortController(),opSignal=opAbort.signal;
    let next=0,accepted=0,terminal=null,reason='network',closing=false;
    const workers=Math.min(settings.parallelParts?settings.parallelParts():1,total-1);
    rank=getScheduler.rank();getScheduler.opOpen();
    try{
     const pool=[];
     for(let i=0;i<workers;i++)pool.push((async()=>{
      for(;;){
       if(opSignal.aborted)return;
       const part=next++;if(part>=total-1)return;
       let outcome;
       try{outcome=await sendPart(part,opSignal)}catch(error){if(opSignal.aborted)return;throw error}
       if(outcome.kind==='part'){accepted++;continue}
       if(outcome.kind==='terminal'){terminal=outcome.response;opAbort.abort();return}
       reason=outcome.lastReason||reason;return;
      }
     })());
     await Promise.all(pool);
     closing=!terminal&&accepted===total-1;
    }finally{opAbort.abort();if(!closing)getScheduler.opClose()}
    if(terminal)return terminal;
    if(!closing)throw settings.failure(reason,'carrier retry limit reached');
    // The operation stays open until its closing part settles, so the
    // scheduler regrants the handed-off connection only afterwards.
    try{
     const last=await sendPart(total-1,null);
     if(last.kind==='done'||last.kind==='terminal')return last.response;
     throw settings.failure(last.lastReason||'network','carrier retry limit reached');
    }finally{getScheduler.opClose()}
   }
   // Sequential path: single-part and non-uplink operations keep the original
   // one-request-at-a-time retry driver.
   let part=0,lastReason='network';
   for(;;){
    if(part>=total)return null;
    const outcome=await sendPart(part,null);
    if(outcome.kind==='part'){part++;continue}
    if(outcome.kind==='done'||outcome.kind==='terminal')return outcome.response;
    lastReason=outcome.lastReason||lastReason;break;
   }
   throw settings.failure(lastReason,'carrier retry limit reached');
  }finally{if(downlink)getScheduler.downClose()}
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
