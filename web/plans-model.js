(function(root,factory){const values=typeof module==='object'&&module.exports?require('./field-values.js'):root.MiaomiaowuFieldValues;const api=factory(values);if(typeof module==='object'&&module.exports)module.exports=api;else root.PlansModel=api;})(globalThis,function(values){
  'use strict';
  const clone=x=>JSON.parse(JSON.stringify(x));
  const exact=(x,required,optional=[])=>{if(!x||typeof x!=='object'||Array.isArray(x)||required.some(k=>!Object.hasOwn(x,k))||Object.keys(x).some(k=>!required.includes(k)&&!optional.includes(k)))throw new Error('套餐字段结构无效');};
  const validVersion=v=>typeof v==='string'&&/^"pl1-[0-9a-f]{64}"$/.test(v);
  function count(v){if(!Number.isInteger(v)||v<1||v>100000)throw new Error('连接数和在线 IP 上限应为1–100000的整数');return v;}
  function body(x){
    exact(x,['name','bandwidth'],['connections']);
    if(typeof x.name!=='string'||[...x.name.trim()].length<1||[...x.name.trim()].length>120)throw new Error('名称应为1–120个字符');
    if(x.bandwidth===null)throw new Error('套餐带宽不能继承');values.bandwidthToForm(x.bandwidth);
    if(Object.hasOwn(x,'connections')){exact(x.connections,['max_connections'],['max_ips']);if(x.connections.max_connections!==null)count(x.connections.max_connections);if(Object.hasOwn(x.connections,'max_ips'))count(x.connections.max_ips);}
    return {...clone(x),name:x.name.trim()};
  }
  function record(x){exact(x,['id','name','bandwidth','version'],['connections']);if(!Number.isSafeInteger(x.id)||x.id<1||!validVersion(x.version))throw new Error('套餐身份或版本无效');body({name:x.name,bandwidth:x.bandwidth,...(Object.hasOwn(x,'connections')?{connections:x.connections}:{})});return clone(x);}
  function list(x){if(!Array.isArray(x))throw new Error('套餐列表无效');const rows=x.map(record);if(new Set(rows.map(r=>r.id)).size!==rows.length)throw new Error('套餐编号重复');return rows;}
  function integer(text){if(typeof text!=='string'||!/^\d+$/.test(text)||text.length>6)throw new Error('请输入1–100000的整数');return count(Number(text));}
  function form(x){
    const at=(field,fn)=>{try{return fn();}catch(error){error.field=field;throw error;}};
    at('plan-name',()=>body({name:x.name,bandwidth:{upload_bps:null,download_bps:null}}));
    const rate=(mode,text)=>mode==='unlimited'?null:mode==='limited'?values.mbpsToBytes(text):(()=>{throw new Error('请选择限速方式');})();
    const result={name:x.name,bandwidth:{upload_bps:at('upload-rate',()=>rate(x.uploadKind,x.upload)),download_bps:at('download-rate',()=>rate(x.downloadKind,x.download))}};
    if(x.connectionKind==='set')result.connections={max_connections:x.connections.trim()===''?null:at('connection-limit',()=>integer(x.connections)),...(x.ips.trim()===''?{}:{max_ips:at('ip-limit',()=>integer(x.ips))})};
    else if(x.connectionKind!=='preserve')throw new Error('请选择连接设置方式');
    return body(result);
  }
  function expected(payload,baseline,id){return {id,name:payload.name,bandwidth:payload.bandwidth,...(payload.connections?{connections:payload.connections}:baseline?.connections?{connections:baseline.connections}:{})};}
  function same(row,want){const clean=x=>JSON.stringify([x.id,x.name,x.bandwidth.upload_bps,x.bandwidth.download_bps,Object.hasOwn(x,'connections'),x.connections?.max_connections,x.connections?.max_ips]);return clean(row)===clean(want);}
  return Object.freeze({body,record,list,form,validVersion,expected,same});
});
