import logging

from dropbox.client import DropboxOAuth2Flow, DropboxClient
from flask import abort, Flask, jsonify, redirect, request, render_template, session, url_for
from secrets import *
from os.path import basename
import connector as DBC

# from pudb import set_trace

app = Flask(__name__)
app.secret_key = FLASK_SECRET_KEY

@app.route('/')
def index():
    if 'access_token' in session:
        return redirect(url_for('overview'))
    else:
        return render_template('index.html')

@app.route('/login')
def login():
    return dropbox_auth_start()

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    session.pop('access_token', None)
    return redirect(url_for('index'))

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
    session['user_id'] = user_id
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

@app.route( '/get_filetree')
def get_filetree():
    client = DropboxClient(session['access_token'])
    tree = walk_tree(client, '/', 2)
    DBC.store(session['user_id'], tree)
    return jsonify(tree)

# if stopdepth is -1, walk_tree will crawl the entire
# file tree, otherwise it stops after the specified stopdepth 
def walk_tree(client, path, stopdepth):
    metadata = client.metadata(path)

    # skeleton output structure
    node = { 'name': basename(metadata['path'])
           , 'path': path
           , 'hash': metadata['hash']
           , 'size': metadata['bytes'] }

    if (stopdepth > 0 or stopdepth == -1) and metadata['is_dir']:
        cumulative_size = 0
        node['children'] = []
        for dirent in metadata['contents']:
            if dirent['is_dir']:
                child_node = walk_tree(client, dirent['path'], stopdepth-1)
                node['children'].append(child_node)
                cumulative_size += child_node['size']
            else:
                child_node = { 'name': basename(dirent['path'])
                             , 'path': dirent['path']
                             , 'hash': None
                             , 'size': dirent['bytes']
                             }
                node['children'].append(child_node)
                cumulative_size += child_node['size']
        node['size'] = cumulative_size

    return node

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
