from werkzeug.contrib.profiler import ProfilerMiddleware
from app import app

app.config['PROFILE'] = True
app.wsgi_app = ProfilerMiddleware(app.wsgi_app, restrictions=[30])
context = ('server.crt', 'server.key')
app.run(debug = True, host='0.0.0.0', ssl_context=context)
