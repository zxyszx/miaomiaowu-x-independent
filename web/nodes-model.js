(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.NodesModel=api;})(globalThis,function(){
 'use strict';
 const clone=x=>JSON.parse(JSON.stringify(x));
 const exact=(x,keys)=>{if(!x||typeof x!=='object'||Array.isArray(x)||Object.keys(x).sort().join(',')!==keys.slice().sort().join(','))throw new Error('节点字段结构无效');};
 const id=x=>Number.isSafeInteger(x)&&x>0;
 const validVersion=v=>typeof v==='string'&&/^"nd1-[0-9a-f]{64}"$/.test(v);
 const name=x=>{if(typeof x!=='string'||[...x.trim()].length<1||[...x.trim()].length>120)throw new Error('名称应为1–120个字符');return x.trim();};
 function body(x){exact(x,['name','server_id']);if(!id(x.server_id))throw new Error('请选择有效服务器');return {name:name(x.name),server_id:x.server_id};}
 function record(x){exact(x,['id','name','server_id','version']);if(!id(x.id)||!validVersion(x.version))throw new Error('节点身份或版本无效');body({name:x.name,server_id:x.server_id});return clone(x);}
 function unique(rows){if(new Set(rows.map(r=>r.id)).size!==rows.length)throw new Error('目录编号重复');return rows;}
 function list(x){if(!Array.isArray(x))throw new Error('节点列表无效');return unique(x.map(record));}
 function servers(x){if(!Array.isArray(x))throw new Error('服务器列表无效');return unique(x.map(r=>{exact(r,['id','name']);if(!id(r.id))throw new Error('服务器身份无效');name(r.name);return clone(r);}));}
 function catalog(nodes,hosts){if(nodes.some(n=>!hosts.some(h=>h.id===n.server_id)))throw new Error('节点所属服务器不在目录');}
 function form(x){try{name(x.name);}catch(error){error.field='node-name';throw error;}if(typeof x.server!=='string'||! /^[1-9]\d*$/.test(x.server)||!id(Number(x.server))){const error=new Error('请选择所属服务器');error.field='server-list';throw error;}return body({name:x.name,server_id:Number(x.server)});}
 const same=(row,want)=>row.id===want.id&&row.name===want.name&&row.server_id===want.server_id;
 return Object.freeze({body,record,list,servers,catalog,form,validVersion,same});
});
