import logging

from dropbox.client import DropboxOAuth2Flow, DropboxClient
from flask import abort, Flask, redirect, request, render_template, session, url_for
from secrets import *
from pudb import set_trace

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login')
def login():
    return dropbox_auth_start()

@app.route('/dropbox-auth-finish')
def dropbox_auth_finish():
    try:
        access_token, user_id, url_state = get_dropbox_auth_flow().finish(request.args)
    except DropboxOAuth2Flow.BadRequestException, e:
        abort(400)
    except DropboxOAuth2Flow.BadStateException, e:
        # Start the auth flow again.
        return redirect("/login")
    except DropboxOAuth2Flow.CsrfException, e:
        abort(403)
    except DropboxOAuth2Flow.NotApprovedException, e:
        return redirect("/")
    except DropboxOAuth2Flow.ProviderException, e:
        print "Auth error: provider exception"
        abort(403)

    session['access_token'] = access_token
    return redirect(url_for('overview'))

@app.route('/overview')
def overview():
    if 'access_token' not in session:
        abort(400)
    try:
        client = DropboxClient(session['access_token'])
    except ErrorResponse, e:
        abort(401)

    account = client.account_info()
    session['username'] = account['display_name']

    return render_template('overview.html', username=session['username'])

def get_dropbox_auth_flow():
    redirect_uri = url_for('dropbox_auth_finish', _external=True)
    return DropboxOAuth2Flow( DROPBOX_APP_KEY
                            , DROPBOX_APP_SECRET
                            , redirect_uri
                            , session
                            , "dropbox-auth-csrf-token" )

def dropbox_auth_start():
    authorize_url = get_dropbox_auth_flow().start()
    return redirect(authorize_url)

if __name__ == '__main__':
    app.run()
