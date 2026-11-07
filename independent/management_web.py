"""Static independent management shell; API authentication stays in Handler."""
from pathlib import Path

WEB = Path(__file__).resolve().parents[1] / 'web'
ASSETS = ('bandwidth-manage.js', 'bandwidth-catalog.js', 'bandwidth-http.js',
          'bandwidth-resource.js', 'bandwidth-form.js', 'field-values.js',
          'bandwidth-form.css', 'bandwidth-live.css',
          'plans.js', 'plans-model.js', 'plans-http.js', 'plans.css',
          'nodes.js', 'nodes-model.js', 'nodes-http.js',
          'servers.js', 'servers-model.js', 'servers-http.js')
ROUTES = {'/manage/': 'bandwidth-manage.html', '/manage/bandwidth': 'bandwidth-manage.html',
          '/manage/plans': 'plans.html',
          '/manage/nodes': 'nodes.html',
          '/manage/servers': 'servers.html',
          **{'/manage/assets/'+name: name for name in ASSETS}}


def serve(handler, path):
    name = ROUTES.get(path)
    if name is None:
        return handler.reply(404, {'error': '管理页面不存在'})
    body = (WEB / name).read_bytes()
    mime = 'text/html' if name.endswith('.html') else 'text/css' if name.endswith('.css') else 'text/javascript'
    handler.send_response(200)
    for key, value in {'Content-Type': mime+'; charset=utf-8', 'Content-Length': str(len(body)),
            'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer',
            'Content-Security-Policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src data:; form-action 'none'; base-uri 'none'; frame-ancestors 'none'"}.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(body)
