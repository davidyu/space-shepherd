# core routing logic for the Space Shepherd app

import logging

from dropbox.client import DropboxOAuth2Flow, DropboxClient
from flask import abort, Flask, jsonify, redirect, request, render_template, session, url_for
from secrets import *
from os.path import dirname, basename
import connector as DBC

from pudb import set_trace

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
    if 'access_token' not in session:
        abort(400)
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
    if 'access_token' not in session:
        abort(400)
    client = DropboxClient(session['access_token'])
    cached_tree = DBC.read(session['user_id'])
    if not cached_tree:
        tree = crawl_all_deltas(client)
        DBC.store(session['user_id'], tree)
        cached_tree = tree
    return jsonify(cached_tree)

# updates the file tree using Dropbox's delta API
# if changes were made, return the new tree
# otherwise, return nothing
@app.route( '/update_filetree')
def update_filetree():
    if 'access_token' not in session:
        abort(400)
    client = DropboxClient(session['access_token'])
    user_id = session['user_id']

    has_more = True
    cursor = DBC.get_delta_cursor(user_id)
    changed = False

    while has_more:
        delta = client.delta(cursor)
        has_more = delta['has_more']
        cursor = delta['cursor']

        if delta['reset'] is True:
            DBC.clear(user_id)

        if len(delta['entries']) > 0:
            changed = True

        for entry in delta['entries']:
            [path, metadata] = entry
            if not metadata:
                DBC.delete_path(user_id, path)
            else:
                DBC.update_path(user_id, metadata['path'], metadata)

        DBC.set_delta_cursor(user_id, cursor)

    result = { 'changed': changed }

    if changed:
        result['tree'] = DBC.read(session['user_id'])

    return jsonify(result)
    
# crawls all deltas for the given client starting from
# the beginning. Do this in memory using a dictionary
# and write the results into the database at the end
def crawl_all_deltas(client):
    metadata = client.metadata('/')

    # skeleton output structure
    root = { 'name': '/'
           , 'is_dir': metadata['is_dir']
           , 'path': '/'
           , 'size': metadata['bytes']
           , 'children' : [] }

    tab = {}
    tab['/'] = root

    # adds parent folders to the table, if necessary
    def add_parent_folders(path, preserved_path):
        if dirname(path) is not path: # while we haven't reached the root (/)
            if path in tab:
                return tab[path]
            else:
                parent = add_parent_folders(dirname(path), dirname(preserved_path))
                fold = { 'name': basename(preserved_path)
                       , 'is_dir': True
                       , 'path': preserved_path
                       , 'size': 0
                       , 'children' : [] }
                tab[path] = fold
                parent['children'].append(fold)
                return fold
        else:
            return tab['/']

    # increments the parent folder size by some given bytes
    def increment_parent_folder_size(parent_path, addition):
        tab[parent_path]['size'] += addition
        if dirname(parent_path) is not parent_path: # while we haven't reached the root (/)
            increment_parent_folder_size(dirname(parent_path), addition)

    has_more = True
    cursor = None
    changed = False

    while has_more:
        delta = client.delta(cursor)
        has_more = delta['has_more']
        cursor = delta['cursor']

        if delta['reset'] is True:
            root = { 'name': '/'
                   , 'is_dir': metadata['is_dir']
                   , 'path': '/'
                   , 'size': metadata['bytes']
                   , 'children' : [] }

            tab = {}
            tab['/'] = root

        for entry in delta['entries']:
            [lowercase_path, metadata] = entry

            parent = add_parent_folders(dirname(lowercase_path), dirname(metadata['path']))
            if metadata is None:
                tab.pop( lowercase_path, None )
                d = lowercase_path + '/'
                for p in tab.keys():
                    if p.startswith( d ):
                        del tab[p]
            else:
                node = { 'name': basename(metadata['path'])
                       , 'is_dir': metadata['is_dir']
                       , 'path': metadata['path']
                       , 'size': metadata['bytes'] }

                if node['is_dir']:
                    node['children'] = []
                else:
                    increment_parent_folder_size(dirname(lowercase_path), node['size'])

                tab[lowercase_path] = node
                parent['children'].append(node)

    return tab['/']

def get_dropbox_auth_flow():
    redirect_uri = url_for('dropbox_auth_finish', _external=True, _scheme='https')
    return DropboxOAuth2Flow( DROPBOX_APP_KEY
                            , DROPBOX_APP_SECRET
                            , redirect_uri
                            , session
                            , "dropbox-auth-csrf-token" )

def dropbox_auth_start():
    authorize_url = get_dropbox_auth_flow().start()
    return redirect(authorize_url)

if __name__ == '__main__':
    context = ('server.crt', 'server.key')
    app.run(host='0.0.0.0', ssl_context=context)
