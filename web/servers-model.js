(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.ServersModel=api;})(globalThis,function(){
 'use strict';
 const exact=(x,keys)=>{if(!x||typeof x!=='object'||Array.isArray(x)||Object.keys(x).sort().join(',')!==keys.slice().sort().join(','))throw new Error('服务器字段结构无效');};
 const validVersion=v=>typeof v==='string'&&/^"sv1-[0-9a-f]{64}"$/.test(v);
 function body(x){exact(x,['name']);if(typeof x.name!=='string'||[...x.name.trim()].length<1||[...x.name.trim()].length>120)throw new Error('名称应为1–120个字符');return {name:x.name.trim()};}
 function record(x){exact(x,['id','name','version']);if(!Number.isSafeInteger(x.id)||x.id<1||!validVersion(x.version))throw new Error('服务器身份或版本无效');body({name:x.name});return {...x};}
 function list(x){if(!Array.isArray(x))throw new Error('服务器列表无效');const rows=x.map(record);if(new Set(rows.map(r=>r.id)).size!==rows.length)throw new Error('目录编号重复');return rows;}
 function form(x){try{return body(x);}catch(error){error.field='server-name';throw error;}}
 const same=(row,want)=>row.id===want.id&&row.name===want.name;
 return Object.freeze({body,record,list,form,validVersion,same});
});
