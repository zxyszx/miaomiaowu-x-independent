(() => {
  'use strict';
  const el=id=>document.getElementById(id), model=window.PlansModel;
  let transport=null, authorized=false, epoch=0, busy=false, listed=false, rows=[];
  let baseline=null, mode='new', phase='idle', deletion=null;
  const status=text=>{el('plan-status').textContent=text;};
  function render(){
    const editable=authorized&&listed&&!busy&&!deletion&&phase==='ready';
    el('plans-fields').disabled=!editable;
    el('upload-rate').disabled=el('upload-kind').value!=='limited';
    el('download-rate').disabled=el('download-kind').value!=='limited';
    for(const id of ['connection-limit','ip-limit'])el(id).disabled=el('connection-kind').value!=='set';
    el('plan-list').disabled=!authorized||!listed||busy||!!deletion||!rows.length;
    for(const id of ['refresh-plans','new-plan'])el(id).disabled=!authorized||!listed&&id==='new-plan'||busy||!!deletion;
    el('reload-plan').disabled=!authorized||busy||!!deletion||!baseline;
    el('delete-plan').disabled=!editable||!baseline||mode!=='edit';
    el('delete-confirm').hidden=!deletion;
    el('confirm-delete').disabled=!deletion||busy||!authorized||el('delete-id').value!==String(deletion?.id);
    el('delete-id').disabled=busy;el('cancel-delete').disabled=busy;
    for(const id of ['authorize','admin-token'])el(id).disabled=busy||!!deletion;el('forget').disabled=busy;
    el('plan-form').setAttribute('aria-busy',String(busy));
    el('editor-title').textContent=mode==='new'?'新建套餐':'编辑套餐 #'+baseline?.id;
    el('save-plan').textContent=mode==='new'?'创建套餐':'保存修改';
  }
  function options(){
    el('plan-list').replaceChildren(new Option(rows.length?'请选择套餐':'暂无套餐',''));
    for(const row of rows)el('plan-list').add(new Option(row.name+' · #'+row.id,String(row.id)));
    el('plan-list').value=baseline&&rows.some(r=>r.id===baseline.id)?String(baseline.id):'';
    el('list-status').textContent=rows.length?'共 '+rows.length+' 个套餐。':'暂无套餐，可点击新建套餐。';
  }
  function errors(){el('form-error').textContent='';for(const input of el('plan-form').querySelectorAll('[aria-invalid]'))input.removeAttribute('aria-invalid');}
  function load(row){
    errors();el('plan-name').value=row?.name||'';
    for(const direction of ['upload','download']){
      const value=row?.bandwidth[direction+'_bps']??null;
      el(direction+'-kind').value=value===null?'unlimited':'limited';
      el(direction+'-rate').value=value===null?'':window.MiaomiaowuFieldValues.bytesToMbps(value);
    }
    el('connection-kind').value=row?.connections?'set':'preserve';
    el('connection-limit').value=row?.connections?.max_connections??'';
    el('ip-limit').value=row?.connections?.max_ips??'';
    el('connection-current').textContent=row?.connections?'当前：连接数 '+(row.connections.max_connections??'不限')+'，在线 IP '+(row.connections.max_ips??'不限')+'。':'当前：默认设置。';
  }
  async function request(spec,ticket){
    if(!transport)throw new Error('请先授权');
    try{return await transport(spec);}
    catch(error){if(ticket===epoch&&error.code==='auth'){authorized=false;el('auth-status').textContent=error.message;}throw error;}
  }
  async function refresh({reload=false}={}){
    if(busy||deletion||!authorized)return false;
    const ticket=epoch;busy=true;listed=false;el('list-status').textContent='正在读取套餐列表…';render();
    try{
      const next=model.list(await request({method:'GET',path:'/api/plans'},ticket));
      if(ticket!==epoch||!authorized)return false;
      rows=next;listed=true;options();
      if(baseline){
        const current=rows.find(r=>r.id===baseline.id);
        if(!current){phase='conflict';status('原套餐已不存在。草稿保留，请选择其他套餐或新建。');}
        else if(reload){baseline=current;phase='ready';load(current);status('套餐已重新读取。');}
        else if(phase==='delete-blocked'){status('套餐仍绑定用户或被用作到期降级目标。草稿保留，请处理依赖后重新读取。');}
        else if(phase==='delete-unconfirmed'){status('删除结果未确认，草稿保留且不能重试。请重新读取套餐核对。');}
        else if(phase==='conflict'){status('套餐配置已变化，草稿保留。请重新读取套餐后再编辑。');}
        else if(current.version!==baseline.version){phase='conflict';status('套餐配置已变化，草稿保留。请重新读取套餐后再编辑。');}
        else{phase='ready';status('列表已更新，草稿未改动。');}
      }else if(phase==='deleted'){status('套餐已删除并重新读取确认。请选择其他套餐或新建。');}
      else if(phase==='uncertain'){status('创建结果未确认，草稿保留且不能重试。请先检查列表，选择已创建套餐或点击新建开始空白表单。');}
      else{phase='ready';status('可填写新套餐，或从列表选择套餐。');}
      return true;
    }catch(_){if(ticket===epoch){listed=false;status('列表读取失败，草稿保留且不能保存。请刷新或重新授权。');el('list-status').textContent='套餐列表读取失败。';}return false;}
    finally{if(ticket===epoch){busy=false;render();}}
  }
  function clear(){
    const deleting=!!deletion;deletion=null;el('delete-id').value='';
    epoch++;transport=null;authorized=false;listed=false;
    if(busy)phase=deleting?'delete-unconfirmed':mode==='new'?'uncertain':'unconfirmed';busy=false;
    el('admin-token').value='';el('auth-status').textContent='当前授权已清除，草稿保留。';render();
  }
  el('auth-form').addEventListener('submit',async event=>{
    event.preventDefault();if(busy||deletion)return;
    const ticket=++epoch;authorized=false;listed=false;transport=null;
    try{transport=window.PlansHTTP.create({baseURL:location.origin,token:el('admin-token').value});}
    catch(error){el('auth-status').textContent=error.message;render();return;}
    finally{el('admin-token').value='';}
    authorized=true;el('auth-status').textContent='正在验证授权…';
    await refresh();if(ticket===epoch&&authorized&&listed)el('auth-status').textContent='授权有效，仅当前页面内存使用。';render();
  });
  el('forget').addEventListener('click',()=>{if(!busy)clear();});window.addEventListener('pagehide',clear);
  el('refresh-plans').addEventListener('click',()=>void refresh());
  el('reload-plan').addEventListener('click',()=>void refresh({reload:true}));
  el('new-plan').addEventListener('click',()=>{
    if(busy||deletion||!authorized||!listed)return;baseline=null;mode='new';phase='ready';load(null);options();status('填写新套餐后点击创建。');render();el('plan-name').focus();
  });
  el('plan-list').addEventListener('change',()=>{
    if(busy||deletion||!authorized||!listed)return;
    const current=rows.find(r=>r.id===Number(el('plan-list').value));
    if(!current){phase='idle';status('请选择套餐，或点击新建。草稿保留且不能保存。');render();return;}
    baseline=current;mode='edit';phase='ready';load(current);status('套餐已读取。');render();
  });
  function cancelDeletion(){
    if(busy||!deletion)return;deletion=null;el('delete-id').value='';el('delete-error').textContent='';render();el('delete-plan').focus();
  }
  el('delete-plan').addEventListener('click',()=>{
    if(!authorized||!listed||busy||phase!=='ready'||!baseline||mode!=='edit'||deletion)return;
    deletion=structuredClone(baseline);el('delete-id').value='';el('delete-error').textContent='';
    el('delete-target').textContent='套餐：'+deletion.name+' · #'+deletion.id;render();el('delete-id').focus();
  });
  el('cancel-delete').addEventListener('click',cancelDeletion);
  el('delete-confirm').addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();cancelDeletion();}});
  el('delete-id').addEventListener('input',()=>{el('delete-error').textContent='';render();});
  el('delete-form').addEventListener('submit',async event=>{
    event.preventDefault();
    if(!deletion||!authorized||!listed||busy||phase!=='ready'||!baseline||deletion.id!==baseline.id||deletion.version!==baseline.version)return;
    if(el('delete-id').value!==String(deletion.id)){el('delete-error').textContent='请输入完全一致的套餐编号。';el('delete-id').focus();return;}
    const original=structuredClone(deletion),ticket=epoch;busy=true;status('正在删除并重新读取，请稍候…');render();
    try{
      const ack=await request({method:'DELETE',path:'/api/plans/'+original.id,ifMatch:original.version},ticket);
      if(ticket!==epoch||!authorized)return;
      if(!ack||ack.deleted!==true||Object.keys(ack).length!==1)throw new Error('删除响应无效');
      const next=model.list(await request({method:'GET',path:'/api/plans'},ticket));
      if(ticket!==epoch||!authorized)return;
      if(next.some(row=>row.id===original.id))throw new Error('删除重读未确认');
      rows=next;listed=true;baseline=null;mode='new';phase='deleted';load(null);options();status('套餐已删除并重新读取确认。请选择其他套餐或新建。');
    }catch(error){
      if(ticket===epoch){phase=error.code==='conflict'?'conflict':error.code==='dependency'?'delete-blocked':'delete-unconfirmed';
        status(error.code==='conflict'?'套餐配置已变化，草稿保留。请重新读取套餐后再确认删除。':error.code==='dependency'?'套餐仍绑定用户或被用作到期降级目标。草稿保留，请处理依赖后重新读取。':'删除结果未确认，草稿保留且不能重试。请重新读取套餐核对。');}
    }finally{if(ticket===epoch){busy=false;deletion=null;el('delete-id').value='';render();}}
  });
  el('plan-form').addEventListener('input',()=>{errors();if(authorized&&listed&&!busy&&phase==='ready')status('草稿已修改，尚未保存。');render();});
  el('plan-form').addEventListener('change',()=>render());
  el('plan-form').addEventListener('submit',async event=>{
    event.preventDefault();if(!authorized||!listed||busy||deletion||phase!=='ready')return;
    errors();let payload;
    try{
      payload=model.form({name:el('plan-name').value,uploadKind:el('upload-kind').value,upload:el('upload-rate').value,
        downloadKind:el('download-kind').value,download:el('download-rate').value,connectionKind:el('connection-kind').value,
        connections:el('connection-limit').value,ips:el('ip-limit').value});
    }catch(error){el('form-error').textContent=error.message;const field=el(error.field||'plan-name');field.setAttribute('aria-invalid','true');field.focus();return;}
    const ticket=epoch, original=baseline?structuredClone(baseline):null, creating=mode==='new';
    busy=true;status('正在保存并重新读取，请稍候…');render();
    try{
      const ack=model.record(await request({method:creating?'POST':'PUT',path:creating?'/api/plans':'/api/plans/'+original.id,body:payload,
        ...(creating?{}:{ifMatch:original.version})},ticket));
      if(ticket!==epoch||!authorized)return;
      const want=model.expected(payload,original,creating?ack.id:original.id);
      if(!model.same(ack,want))throw new Error('写入响应不匹配');
      const next=model.list(await request({method:'GET',path:'/api/plans'},ticket));
      if(ticket!==epoch||!authorized)return;
      const confirmed=next.find(r=>r.id===want.id);
      if(!confirmed||!model.same(confirmed,want))throw new Error('保存重读不匹配');
      rows=next;listed=true;baseline=confirmed;mode='edit';phase='ready';load(confirmed);options();status('保存后已重新读取并确认。');
    }catch(error){
      if(ticket===epoch){phase=creating?'uncertain':error.code==='conflict'?'conflict':'unconfirmed';
        status(error.code==='conflict'?'套餐配置已变化，草稿保留。请重新读取套餐后再编辑。':creating?'创建结果未确认，草稿保留且不能重试。请先刷新并检查列表，选择已创建套餐或点击新建开始空白表单。':'保存结果未确认，草稿保留且不能保存。请重新读取核对。');}
    }finally{if(ticket===epoch){busy=false;render();}}
  });
  render();
})();
