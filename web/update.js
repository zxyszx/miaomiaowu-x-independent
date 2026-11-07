/* Reuse the pinned runtime's actual dialog, buttons and icons. No duplicate theme. */
(() => {
  if (location.pathname.startsWith('/__preview/')) return;
  let panel, runningVersion, versionRequest;
  const loadVersion = () => versionRequest ||= fetch('/__preview/version', {cache:'no-store'}).then(r => { if (!r.ok) throw Error('version'); return r.json(); }).then(state => { runningVersion=state.running; updateOwnVersion(); }).catch(() => { versionRequest=null; });
  let components;
  const nativeComponents = () => components ||= Promise.all([
    import(document.querySelector('script[type="module"][src*="/assets/index-"]').src),
    import('/assets/vendor-modules-Dvx1fO4V.js')
  ]);
  const open = async opener => {
    if (panel) return;
    panel = document.createElement('div');
    document.body.append(panel);
    const host=panel;
    try {
      const [ui,vendor]=await nativeComponents();
      const React=vendor.r, h=React.createElement;
      const root=vendor.bc.createRoot(host);
      const icon=(kind,props={})=>h(kind==='download'?vendor.as:kind==='success'?vendor.aD:vendor.ay,props);
      const button=(text,click,props={},glyph)=>h(ui.B,{type:'button',onClick:click,...props},glyph&&icon(glyph,{className:'mr-2 size-4'}),text);
      function UpdateDialog(){
        const [status,setStatus]=React.useState({checking:true}), [busy,setBusy]=React.useState(false);
        const [notesOpen,setNotesOpen]=React.useState(false);
        const channel=React.useRef('stable');
        const [selected,setSelected]=React.useState('stable');
        const active=React.useRef(true), blocked=React.useRef(false), checks=React.useRef(0);
        React.useEffect(()=>()=>{active.current=false;},[]);
        async function request(path,options={},timeout=25000){
          const controller=new AbortController(),timer=setTimeout(()=>controller.abort(),timeout);
          try {
            const response=await fetch('/__preview/'+path,{cache:'no-store',...options,signal:controller.signal});
            let data;
            try { data=await response.json(); }
            catch { throw Error(response.ok?'更新服务返回的数据无效，请重新检查':'更新服务暂时不可用（HTTP '+response.status+'），请稍后重试'); }
            if(!data || typeof data!=='object' || Array.isArray(data))throw Error('更新服务返回的数据无效，请重新检查');
            if(!response.ok)throw Error(data.error||'更新失败，请稍后重试');
            return data;
          } catch(error){throw Error(error.name==='AbortError'?'请求超时，请重新检查':error.message);}
          finally{clearTimeout(timer);}
        }
        async function check(next=channel.current){
          if(blocked.current)return;
          const attempt=++checks.current;
          channel.current=next;setSelected(next);setStatus(s=>({...s,checking:true,error:null}));
          try {
            const [state,release]=await Promise.all([request('version'),request('latest?channel='+next)]);
            if(!/^\d+\.\d+\.\d+$/.test(state.running)||!/^\d+\.\d+\.\d+$/.test(release.version))throw Error('版本信息无效，请重新检查');
            const compare=(a,b)=>{a=a.split('.').map(Number);b=b.split('.').map(Number);for(let i=0;i<3;i++)if(a[i]!==b[i])return a[i]-b[i];return 0;};
            if(active.current&&attempt===checks.current)setStatus({state,release,available:compare(release.version,state.running)>0});
          }catch(error){if(active.current&&attempt===checks.current)setStatus(s=>({...s,checking:false,error:error.message}));}
        }
        React.useEffect(()=>{check();},[]);
        async function install(force){
          if(blocked.current)return;
          blocked.current=true;setBusy(true);
          const target=status.state?.pending?status.state.installed:status.release?.version;
          setStatus(s=>({...s,error:null,progress:force?'正在重新安装…':'正在更新…'}));
          try {
            await request('install',{method:'POST',headers:{'Content-Type':'application/json','X-Requested-With':'SubNest-Update'},body:JSON.stringify({force,target,channel:channel.current})},120000);
            const deadline=Date.now()+120000;
            let confirmed=false;
            while(Date.now()<deadline){
              await new Promise(resolve=>setTimeout(resolve,1500));
              try{const state=await request('version',{},5000);if(state.running===target&&state.installed===target&&!state.pending){confirmed=true;break;}}catch{}
            }
            if(!confirmed)throw Error('尚未确认服务恢复，请稍后重新检查');
            if(active.current){setStatus(s=>({...s,progress:'更新成功，即将刷新页面'}));setTimeout(()=>location.reload(),3000);}
          }catch(error){if(active.current)setStatus(s=>({...s,progress:null,error:error.message}));}
          finally{blocked.current=false;if(active.current)setBusy(false);}
        }
        const checking=status.checking;
        let body;
        if(checking)body=h('div',{className:'py-8 text-center'},icon('refresh',{className:'text-primary mx-auto mb-3 size-8 animate-spin'}),h('p',{className:'text-muted-foreground text-sm',role:'status'},'正在检查更新…'));
        else if(status.error)body=h('div',{className:'py-8 text-center'},h('p',{className:'text-destructive text-sm',role:'alert'},status.error));
        else if(busy)body=h('div',{className:'py-8 text-center'},icon('refresh',{className:'text-primary mx-auto mb-3 size-8 animate-spin'}),h('p',{className:'text-sm',role:'status'},status.progress));
        else if(status.available||status.state?.pending)body=h('div',{className:'space-y-4'},
          h('p',{className:'font-medium text-amber-500'},'发现新版本'),
          h('div',{className:'bg-muted/50 space-y-2 rounded-lg p-3'},
            h('div',{className:'flex justify-between text-sm'},h('span',{className:'text-muted-foreground'},'当前版本'),h('span',{className:'font-mono'},'v'+status.state.running)),
            h('div',{className:'flex justify-between text-sm'},h('span',{className:'text-muted-foreground'},'最新版本'),h('span',{className:'font-mono text-green-600'},'v'+status.release.version))),
          status.release.notes&&h('div',{className:'space-y-2 overflow-hidden'},h('p',{className:'text-sm font-medium'},'更新内容'),h('div',{className:'bg-muted/30 max-h-40 overflow-x-hidden overflow-y-auto rounded-lg p-3'},h('p',{className:'text-muted-foreground text-sm break-all whitespace-pre-wrap'},status.release.notes))),
          button('立即更新',()=>install(false),{className:'w-full',disabled:busy},'download'));
          else body=h('div',{className:'py-8 text-center'},
          icon('success',{className:'mx-auto mb-3 size-12 text-green-500'}),
          h('p',{className:'text-lg font-medium',role:'status'},'已是最新版本'),
          h('p',{className:'text-muted-foreground mt-1 text-sm'},'当前版本：v'+status.state.running),
          status.release?.notes&&h('div',{className:'mt-4 text-left'},
            button(notesOpen?'收起更新内容':'查看更新内容',()=>setNotesOpen(value=>!value),{variant:'outline',size:'sm',className:'w-full'}),
            notesOpen&&h('div',{className:'bg-muted/30 mt-2 max-h-40 overflow-x-hidden overflow-y-auto rounded-lg p-3'},
              h('p',{className:'text-muted-foreground text-sm break-all whitespace-pre-wrap'},status.release.notes))),
          button('强制重新安装',()=>install(true),{variant:'outline',size:'sm',className:'mt-4',disabled:busy||status.release.version!==status.state.running},'download'));
        return h(ui.o,{open:true,onOpenChange:value=>{if(!value){active.current=false;root.unmount();host.remove();panel=null;opener?.focus({preventScroll:true});}}},
          h(ui.p,{className:'max-h-[90dvh] overflow-y-auto sm:max-w-md',onCloseAutoFocus:event=>{event.preventDefault();opener?.focus({preventScroll:true});}},
            h(ui.q,null,h(ui.r,{className:'flex items-center gap-2'},icon('refresh',{className:'size-5'}),' 检查更新'),h(ui.s,null,'检查是否有新版本可用')),
            h('div',{className:'min-w-0 space-y-4'},
              h('div',{className:'space-y-2'},h('p',{className:'text-sm font-medium'},'更新通道'),h('div',{className:'grid grid-cols-2 gap-2'},
                button('稳定版',()=>check('stable'),{variant:selected==='stable'?'default':'outline',size:'sm',disabled:busy}),
                button('预发布版',()=>check('prerelease'),{variant:selected==='prerelease'?'default':'outline',size:'sm',disabled:busy})),
                selected==='prerelease'&&h('p',{className:'text-xs text-amber-600 dark:text-amber-400'},'预发布版可能不稳定，仅建议用于测试环境。切换通道不会自动安装更新。')),
              body,button('重新检查',()=>check(),{variant:'outline',className:'w-full',disabled:checking||busy},'refresh'))));
      }
      root.render(h(UpdateDialog));
    }catch(error){host.remove();panel=null;components=null;console.error('Unable to load original update components',error);}
  };
  const label = node => {
    if (!node || !node.closest) return null;
    const item = node.closest('[role="menuitem"],button');
    if (!item) return null;
    const text = item.textContent.trim();
    return /^(检查更新|Check for updates|Check Update|预览更新)$/i.test(text) ? item : null;
  };
  document.addEventListener('click', event => {
    if (!label(event.target)) return;
    const menu=event.target.closest('[role="menu"]');
    const opener=document.getElementById(menu?.getAttribute('aria-labelledby'))||document.activeElement;
    event.preventDefault(); event.stopImmediatePropagation();
    // Dismiss the original account menu through its normal keyboard handler.
    event.target.dispatchEvent(new KeyboardEvent('keydown',{key:'Escape',code:'Escape',bubbles:true}));
    open(opener);
  },true);
  const rename = () => {
    document.querySelectorAll('[role="menuitem"]').forEach(item => {
      if (!/^(检查更新|Check for updates|Check Update|预览更新)$/i.test(item.textContent.trim())) return;
      const walker = document.createTreeWalker(item,NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        if (/^(检查更新|Check for updates|Check Update|预览更新)$/i.test(walker.currentNode.textContent.trim())) {
          if(walker.currentNode.textContent !== '检查更新') walker.currentNode.textContent = '检查更新'; break;
        }
      }
    });
  };
  const updateOwnVersion = () => {
    if (!runningVersion) return;
    document.querySelectorAll('[role="menuitem"]').forEach(item => {
      if (!/版本|Version/i.test(item.textContent)) return;
      const walker=document.createTreeWalker(item,NodeFilter.SHOW_TEXT);
      while(walker.nextNode()) {
        const node=walker.currentNode;
        if (/v?\d+\.\d+\.\d+/.test(node.textContent)) {
          // Upstream may render the v prefix in a separate text node.
          const next=node.textContent.replace(/\d+\.\d+\.\d+/, runningVersion);
          if(next!==node.textContent) node.textContent=next;
        }
      }
    });
  };
  loadVersion();
  new MutationObserver(() => {rename();updateOwnVersion();}).observe(document.documentElement,{childList:true,subtree:true});
  rename();
})();
