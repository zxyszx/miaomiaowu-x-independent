(() => {
 'use strict';
 const el=id=>document.getElementById(id),model=window.NodesModel;
 let transport=null,authorized=false,epoch=0,busy=false,listed=false,rows=[],hosts=[];
 let baseline=null,phase='idle',mode='new';
 const status=text=>{el('node-status').textContent=text;};
 function render(){
  el('plans-fields').disabled=!authorized||!listed||busy||phase!=='ready'||!hosts.length;
  el('node-list').disabled=!authorized||!listed||busy||!rows.length;
  el('refresh-nodes').disabled=!authorized||busy;
  el('new-node').disabled=!authorized||!listed||busy||!hosts.length;
  el('reload-node').disabled=!authorized||busy||!baseline;
  for(const id of ['admin-token','authorize','forget'])el(id).disabled=busy;
  el('plan-form').setAttribute('aria-busy',String(busy));
  el('editor-title').textContent=mode==='new'?'新建节点':'编辑节点 #'+baseline?.id;
  el('save-node').textContent=mode==='new'?'创建节点':'保存修改';
 }
 function options(wantedHost){
  el('node-list').replaceChildren(new Option(rows.length?'请选择节点':'暂无节点',''));
  for(const row of rows)el('node-list').add(new Option(row.name+' · #'+row.id,String(row.id)));
  el('node-list').value=baseline&&rows.some(r=>r.id===baseline.id)?String(baseline.id):'';
  el('server-list').replaceChildren(new Option(hosts.length?'请选择服务器':'暂无服务器',''));
  for(const host of hosts)el('server-list').add(new Option(host.name+' · #'+host.id,String(host.id)));
  el('server-list').value=hosts.some(h=>h.id===Number(wantedHost))?String(wantedHost):'';
  el('list-status').textContent=!hosts.length?'暂无服务器，请先创建服务器。':rows.length?'共 '+rows.length+' 个节点。':'暂无节点，可点击新建节点。';
 }
 function errors(){el('form-error').textContent='';for(const field of el('plan-form').querySelectorAll('[aria-invalid]'))field.removeAttribute('aria-invalid');}
 function load(row){errors();el('node-name').value=row?.name||'';el('server-list').value=row?String(row.server_id):'';}
 async function request(spec,ticket){if(!transport)throw new Error('请先授权');try{return await transport(spec);}catch(error){if(ticket===epoch&&error.code==='auth'){authorized=false;el('auth-status').textContent=error.message;}throw error;}}
 async function refresh({reload=false}={}){
  if(busy||!authorized)return false;
  const ticket=epoch,wantedHost=el('server-list').value;busy=true;listed=false;el('list-status').textContent='正在读取节点与服务器…';render();
  try{
   const raw=await Promise.all([request({method:'GET',path:'/api/nodes'},ticket),request({method:'GET',path:'/api/servers'},ticket)]);
   if(ticket!==epoch||!authorized)return false;
   const next=model.list(raw[0]),servers=model.servers(raw[1]);model.catalog(next,servers);rows=next;hosts=servers;listed=true;options(wantedHost);
   if(baseline){const current=rows.find(r=>r.id===baseline.id);
    if(!current){phase='conflict';status('原节点已不存在。草稿保留，请选择其他节点或新建。');}
    else if(reload){baseline=current;phase='ready';load(current);status('节点已重新读取。');}
    else if(phase==='conflict'||current.version!==baseline.version){phase='conflict';status('节点配置已变化，草稿保留。请重新读取节点后再编辑。');}
    else if(wantedHost&&!hosts.some(h=>h.id===Number(wantedHost))){phase='conflict';status('草稿所属服务器已不存在。请重新读取或选择节点。');}
    else{phase='ready';status('列表已更新，草稿未改动。');}
   }else if(phase==='uncertain')status('创建结果未确认，草稿保留且不能重试。请检查列表并选择已创建节点，或点击新建开始空白表单。');
   else if(wantedHost&&!hosts.some(h=>h.id===Number(wantedHost))){phase='conflict';status('草稿所属服务器已不存在。请点击新建开始空白表单。');}
   else if(phase==='conflict')status('草稿保留，请点击新建开始空白表单。');
   else{phase='ready';status(hosts.length?'可新建节点，或从列表选择节点。':'暂无服务器，暂时不能创建节点。');}
   return true;
  }catch(_){if(ticket===epoch){listed=false;status('列表读取失败，草稿保留且不能保存。请刷新或重新授权。');el('list-status').textContent='节点或服务器列表读取失败。';}return false;}
  finally{if(ticket===epoch){busy=false;render();}}
 }
 function clear(){epoch++;transport=null;authorized=false;listed=false;if(busy)phase=mode==='new'?'uncertain':'unconfirmed';busy=false;el('admin-token').value='';el('auth-status').textContent='当前授权已清除，草稿保留。';render();}
 el('auth-form').addEventListener('submit',async event=>{event.preventDefault();if(busy)return;const ticket=++epoch;transport=null;authorized=false;listed=false;try{transport=window.NodesHTTP.create({baseURL:location.origin,token:el('admin-token').value});}catch(error){el('auth-status').textContent=error.message;render();return;}finally{el('admin-token').value='';}authorized=true;el('auth-status').textContent='正在验证授权…';await refresh();if(ticket===epoch&&authorized&&listed)el('auth-status').textContent='授权有效，仅当前页面内存使用。';render();});
 el('forget').addEventListener('click',()=>{if(!busy)clear();});window.addEventListener('pagehide',clear);
 el('refresh-nodes').addEventListener('click',()=>void refresh());el('reload-node').addEventListener('click',()=>void refresh({reload:true}));
 el('new-node').addEventListener('click',()=>{if(!authorized||!listed||busy||!hosts.length)return;baseline=null;mode='new';phase='ready';options('');load(null);status('填写节点名称并选择服务器后点击创建。');render();el('node-name').focus();});
 el('node-list').addEventListener('change',()=>{if(!authorized||!listed||busy)return;const row=rows.find(r=>r.id===Number(el('node-list').value));if(!row){phase='idle';status('请选择节点，或点击新建。草稿保留且不能保存。');render();return;}baseline=row;mode='edit';phase='ready';load(row);status('节点已读取。');render();});
 el('plan-form').addEventListener('input',()=>{errors();if(authorized&&listed&&!busy&&phase==='ready')status('草稿已修改，尚未保存。');});
 el('plan-form').addEventListener('submit',async event=>{
  event.preventDefault();if(!authorized||!listed||busy||phase!=='ready')return;
  errors();let payload;try{payload=model.form({name:el('node-name').value,server:el('server-list').value});if(!hosts.some(h=>h.id===payload.server_id))throw Object.assign(new Error('所属服务器已不在列表，请刷新。'),{field:'server-list'});}catch(error){el('form-error').textContent=error.message;const field=el(error.field||'node-name');field.setAttribute('aria-invalid','true');field.focus();return;}
  const ticket=epoch,original=baseline?structuredClone(baseline):null,creating=mode==='new';busy=true;status('正在保存并重新读取，请稍候…');render();
  try{
   const ack=model.record(await request({method:creating?'POST':'PUT',path:creating?'/api/nodes':'/api/nodes/'+original.id,body:payload,...(creating?{}:{ifMatch:original.version})},ticket));
   if(ticket!==epoch||!authorized)return;const want={...payload,id:creating?ack.id:original.id};if(!model.same(ack,want))throw new Error('写入响应不匹配');
   const next=model.list(await request({method:'GET',path:'/api/nodes'},ticket));if(ticket!==epoch||!authorized)return;const confirmed=next.find(r=>r.id===want.id);if(!confirmed||!model.same(confirmed,want))throw new Error('保存重读未确认');
   rows=next;listed=true;baseline=confirmed;mode='edit';phase='ready';options(payload.server_id);load(confirmed);status('保存后已重新读取并确认。');
  }catch(error){if(ticket===epoch){phase=creating?'uncertain':error.code==='conflict'?'conflict':'unconfirmed';status(error.code==='conflict'?'节点配置已变化，草稿保留。请重新读取节点后再编辑。':creating?'创建结果未确认，草稿保留且不能重试。请刷新检查列表并选择已创建节点，或点击新建开始空白表单。':'保存结果未确认，草稿保留且不能保存。请重新读取核对；共享归属、端口冲突或服务器变化也可能导致拒绝。');}}
  finally{if(ticket===epoch){busy=false;render();}}
 });
 render();
})();
