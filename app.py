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

# main page router
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

# prunes the given tree to be at most maxdepth levels
def prune(tree, maxdepth):
    return prune_r(tree, maxdepth, 1)

def prune_r(node, maxdepth, curdepth):
    if curdepth >= maxdepth:
        if 'children' in node:
            node.pop('children', None)
    else:
        if 'children' in node:
            for child in node['children']:
                prune_r(child, maxdepth, curdepth+1)

    return node

MAX_DIRECTORY_DEPTH = 4

@app.route('/get_filetree')
def get_filetree():
    if 'access_token' not in session:
        abort(400)
    client = DropboxClient(session['access_token'])
    user_id = session['user_id']
    cursor = None
    if DBC.user_exists(user_id):
        result = update_filetree()
        if result['changed']:
            cached_tree = result['tree']
        else:
            cached_tree = DBC.read(user_id, MAX_DIRECTORY_DEPTH)
        cursor = result['cursor']
    else:
        tree, cursor = crawl_all_deltas(client)
        DBC.store(session['user_id'], tree, cursor)
        cached_tree = prune(tree, MAX_DIRECTORY_DEPTH)

    used, total = get_quota_usage(client)

    result = { 'tree'  : cached_tree
             , 'used'  : used
             , 'total' : total
             , 'cursor': cursor }

    return jsonify(result)

# updates the file tree using Dropbox's delta API
# returns a dictionary with two keys:
# 'changed', which correspond to True if the tree was successfully updated
# 'tree', which will be the updated tree if applicable (and None otherwise)
@app.route('/update_filetree')
def update_filetree_json():
    result = update_filetree()

    if result['changed']:
        client = DropboxClient(session['access_token'])
        used, total = get_quota_usage(client)
        result['used'] = used
        result['total'] = total
        
    return jsonify(result)

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

        print "%s entries in delta" % len(delta['entries'])
        for entry in delta['entries']:
            [path, metadata] = entry
            if metadata is None:
                # print "processed a deletion entry"
                DBC.delete_path(user_id, path)
            else:
                # print "processed an update entry"
                DBC.update_path(user_id, metadata['path'], metadata)

    DBC.set_delta_cursor(user_id, cursor)

    result = { 'changed': changed
             , 'tree'   : DBC.read(session['user_id'], MAX_DIRECTORY_DEPTH)
             , 'cursor' : cursor }

    return result
    
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
    def add_parent_folders(lowercase_path, preserved_path):
        if dirname(lowercase_path) is not lowercase_path: # while we haven't reached the root (/)
            if lowercase_path in tab:
                return tab[lowercase_path]
            else:
                parent = add_parent_folders(dirname(lowercase_path), dirname(preserved_path))
                fold = { 'name': basename(preserved_path)
                       , 'is_dir': True
                       , 'path': preserved_path
                       , 'size': 0
                       , 'children' : [] }
                tab[lowercase_path] = fold
                parent['children'].append(fold)
                return fold
        else:
            return tab['/']

    def adjust_parent_folder_size(parent_path, delta):
        tab[parent_path]['size'] += delta
        # recursively adjust parent sizes until we reach root (/)
        if dirname(parent_path) is not parent_path:
            adjust_parent_folder_size(dirname(parent_path), delta)

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

            if metadata is None:
                deleted = tab.pop( lowercase_path, None )
                d = lowercase_path + '/'
                for p in tab.keys():
                    if p.startswith( d ):
                        del tab[p]
                if deleted is not None:
                    adjust_parent_folder_size(dirname(lowercase_path), -deleted['size'])
            else:
                parent = add_parent_folders(dirname(lowercase_path), dirname(metadata['path']))

                node = { 'name': basename(metadata['path'])
                       , 'is_dir': metadata['is_dir']
                       , 'path': metadata['path']
                       , 'size': metadata['bytes'] }

                if node['is_dir']:
                    if lowercase_path in tab:
                        if tab[lowercase_path]['is_dir']:
                            # just copy the metadata that (potentially) changed
                            tab[lowercase_path]['name'] = node['name']
                            tab[lowercase_path]['path'] = node['path']
                        else:
                            # replace the file with me
                            adjust_parent_folder_size(dirname(lowercase_path), -tab[lowercase_path]['size'])
                            tab[lowercase_path]['name'] = node['name']
                            tab[lowercase_path]['is_dir'] = True
                            tab[lowercase_path]['size'] = 0
                            tab[lowercase_path]['path'] = node['path']
                            tab[lowercase_path]['children'] = []
                    else:
                        # add me to the tree and table
                        # we have a size of 0 so no need to adjust parent folder size
                        parent['children'].append(node)
                        tab[lowercase_path] = node
                        tab[lowercase_path]['children'] = []
                else: # we're a file
                    if lowercase_path in tab:
                        # replace whatever existed at path with me
                        adjust_parent_folder_size(dirname(lowercase_path), -tab[lowercase_path]['size'])
                        tab[lowercase_path]['name'] = node['name']
                        tab[lowercase_path]['is_dir'] = False
                        tab[lowercase_path]['size'] = node['size']
                        tab[lowercase_path]['path'] = node['path']
                        adjust_parent_folder_size(dirname(lowercase_path), node['size'])
                    else:
                        # add me to the tree and table
                        parent['children'].append(node)
                        tab[lowercase_path] = node
                        adjust_parent_folder_size(dirname(lowercase_path), node['size'])

    return tab['/'], cursor

def get_quota_usage(client):
    account = client.account_info()
    used = float(account['quota_info']['normal']) + float(account['quota_info']['shared'])
    total = float(account['quota_info']['quota'])
    return used, total

if __name__ == '__main__':
    context = ('server.crt', 'server.key')
    app.run(host='0.0.0.0', ssl_context=context)
