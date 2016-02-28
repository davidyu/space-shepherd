# the connector module is responsible for communicating with
# the database to store and read file tree and user data requested
# by the Space Shepherd app

import MySQLdb as mdb
import sys
from os.path import dirname, basename

from secrets import *

from pudb import set_trace

def connect():
    return mdb.connect( host        = 'localhost'
                      , user        = MYSQL_USERNAME
                      , passwd      = MYSQL_PASSWORD
                      , db          = MYSQL_DBNAME
                      , charset     = 'utf8'
                      , use_unicode = True )

# updates the cursor for the user if applicable
def set_delta_cursor(user_id, delta_cursor):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("""UPDATE Users SET delta_cursor = %s WHERE user_id = %s""", (delta_cursor, user_id))
        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# returns the cursor for the user if applicable
# returns None if the user or cursor doesn't exist
def get_delta_cursor(user_id):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("""SELECT delta_cursor FROM Users WHERE user_id = %s""", (user_id,))
        result = cur.fetchone()
        if not result:
            return None
        else:
            user_cursor, = result
            return user_cursor
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# returns whether the user exists in our database
def user_exists(user_id):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("""SELECT id FROM Users WHERE user_id = %s""", (user_id,))
        exists = cur.fetchone()
        if not exists:
            return False
        else:
            return True
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# if we have a local cache for the user (identified by user_id),
# return the cache, otherwise return None
def read(user_id, maxdepth = None):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        result = cur.fetchone()
        if not result:
            return None
        else:
            root_id, = result
            if not maxdepth:
                cur.execute("""SELECT * FROM Layout
                               WHERE root_id = %s""", (root_id,))
            else:
                cur.execute("""SELECT * FROM Layout
                               WHERE root_id = %s AND path_depth < %s""", (root_id, maxdepth))
            root = treeify(cur.fetchall())
            root['name'] = '/'
            return root
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# treeifies the db cache
def treeify(rows):
    tab = {}
    return treeify_h(rows, tab)

# two passes: builds disconnected nodes first, then constructs the
# tree structure given the disconnected nodes
def treeify_h(rows, tab):
    # build nodes first
    for row in rows:
        id, root_id, parent_id, path, _, is_dir, size = row
        node = { 'name': basename(path)
               , 'is_dir': is_dir
               , 'path': path
               , 'size': size }

        if is_dir:
            node['children'] = []
        tab[id] = node
    # now build hiearchical tree structure
    for row in rows:
        id, _, parent_id, _, _, _, _ = row
        if parent_id is not None:
            tab[parent_id]['children'].append(tab[id])
    # return the root
    _, root_id, _, _, _, _, _ = rows[0]
    return tab[root_id]

# basically mkdir -p
def add_parent_folders(cur, path, root_id):
    if dirname(path) is not path: # while we haven't reached the root (/)
        cur.execute("""SELECT id, path_depth FROM Layout WHERE root_id = %s AND path = %s""", (root_id, path))
        result = cur.fetchone()
        if result is None:
            # add all parents before me first
            parent_id, parent_depth = add_parent_folders(cur, dirname(path), root_id)
            cur.execute("""INSERT INTO Layout(path, path_depth, root_id, parent_id, dir, size)
                           VALUES(%s,%s,%s,%s,%s,%s)""", (path, parent_depth + 1, root_id, parent_id, True, 0))
            file_id = cur.lastrowid
            return file_id, parent_depth + 1
        else:
            return result
    else:
        return root_id, 0

# increments the parent folder size by some given number
# assumes entries for all parent folders in path exist (IE:
# we called add_parent_folders for path)
def adjust_parent_folder_size(cur, path, delta, root_id):
    # build all parent folders
    folders = [path]
    while dirname(path) is not path:
        path = dirname(path)
        folders.append(path)

    # build and execute SQL query
    subs = ','.join('%s' for _ in folders)
    query = """UPDATE Layout SET size = size + (%s) WHERE root_id = %s AND path IN ({})""".format(subs)
    args = [delta,root_id]
    args.extend(folders)
    cur.execute(query, tuple(args))

# assumes the user exists in our database
def update_path(user_id, path, metadata):
    try:
        con = connect()
        cur = con.cursor()

        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        root_id, = cur.fetchone()

        # add entries for nonexistent parent folders
        parent_id, parent_depth = add_parent_folders(cur, dirname(path), root_id)

        # grab file_id for the file specified at given path (file_id will be None if it doesn't exist)
        cur.execute("""SELECT id, dir
                       FROM Layout
                       WHERE root_id = %s AND path = %s""", (root_id, path))

        file_result = cur.fetchone()

        if metadata['is_dir']:
            if file_result is not None:
                file_id, is_dir = file_result

                if is_dir:
                    # just update metadata (in our case, just name) and don't touch children
                    cur.execute("""UPDATE Layout
                                   SET path = %s
                                   WHERE id = %s""", (path, file_id))
                else:
                    # delete file and replace with folder
                    delete_path_h(cur, root_id, path)
                    # default size to 0
                    cur.execute("""INSERT INTO Layout(path, path_depth, root_id, parent_id, dir, size)
                                   VALUES(%s,%s,%s,%s,%s,%s)""", (path, parent_depth + 1, root_id, parent_id, True, 0))
            # no file at path, just add folder
            else:
                cur.execute("""INSERT INTO Layout(path, path_depth, root_id, parent_id, dir, size)
                               VALUES(%s,%s,%s,%s,%s,%s)""", (path, parent_depth + 1, root_id, parent_id, True, 0))
        else:
            # delete whatever we have at path with file
            if file_result is not None:
                delete_path_h(cur, root_id, path)

            cur.execute("""INSERT INTO Layout(path, path_depth, root_id, parent_id, dir, size)
                           VALUES(%s,%s,%s,%s,%s,%s)""", (path, parent_depth + 1, root_id, parent_id, False, metadata['bytes']))

            # update the sizes for all parent folders recursively up to the root
            adjust_parent_folder_size(cur, dirname(path), metadata['bytes'], root_id)

        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()


# deletes path for a given root_id (assumes we're passing in a DB cursor)l
def delete_path_h(cur, root_id, path):
    cur.execute("""SELECT dir, size FROM Layout
                   WHERE root_id = %s AND path = %s""", (root_id, path))
    size_result = cur.fetchone()

    # decrement size of parent folders
    if size_result:
        is_dir, deleted_size = size_result
        adjust_parent_folder_size(cur, dirname(path), -deleted_size, root_id)

        # delete file/folder
        cur.execute("""DELETE FROM Layout WHERE root_id = %s AND path = %s""", (root_id, path))

        # delete all children of folder
        if is_dir:
            cur.execute("""DELETE FROM Layout WHERE root_id = %s AND path LIKE %s""", (root_id, path + "/%"))

# deletes the file or folder (and all children) at the given path
# assumes the user exists in our database
def delete_path(user_id, path):
    try:
        con = connect()
        cur = con.cursor()

        # get root_id from User table and delete all corresponding entries in the File and Layout table
        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        root_id, = cur.fetchone()

        delete_path_h(cur, root_id, path)

        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# clears the filetree for a given user
# assumes the user exists in our database
def clear(user_id):
    try:
        con = connect()
        cur = con.cursor()

        # get root_id from User table and delete all corresponding entries in the File and Layout table
        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        root_id, = cur.fetchone()
        cur.execute("""DELETE FROM Layout WHERE root_id = %s AND id <> %s""", (root_id, root_id))
        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# stores the filetree for a given user
def store(user_id, file_tree, cursor):
    try:
        con = connect()
        cur = con.cursor()

        # write to layout and file table
        root_id = store_tree(cur, file_tree)

        # write to user table
        cur.execute("""INSERT INTO Users(user_id, root_id, delta_cursor) VALUES(%s,%s,%s)""", (user_id, root_id, cursor))

        con.commit()

    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# overwrites the filetree for a given user (which must exist)
def overwrite(user_id, file_tree, cursor):
    try:
        con = connect()
        cur = con.cursor()

        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        
        # assumes we can always unwrap the result; IE: there must exist an entry with the given user_id
        root_id, = cur.fetchone()

        # delete everything
        cur.execute("""DELETE FROM Layout WHERE root_id = %s""", (root_id,))

        # write new tree to Layout table and update the new root_id
        root_id = store_tree(cur, file_tree)

        # write to user table
        cur.execute("""UPDATE Users SET root_id = %s, delta_cursor = %s WHERE user_id = %s""", (root_id, cursor, user_id))

        con.commit()

    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()


# stores the root of the tree into the layout and file table (if applicable), and returns
# the id of the root entry in the layout table
# then calls store_tree_r for all children (if applicable) of the root
def store_tree(cur, root):
    # 1. inserts the root dir into the file table; this is guaranteed NOT to exist by definition (IE: each user should have
    #    a unique root directory)
    # 2. inserts the root dir into the layout table (and update root_id column after insertion)
    # 3. recursively inserts all children of the root into the file and layout table
    cur.execute("""INSERT INTO Layout(path, path_depth, dir, size)
                   VALUES(%s,%s,%s,%s)""", (root['path'], 0, root['is_dir'], root['size']))
    root_id = cur.lastrowid

    cur.execute("""UPDATE Layout SET root_id = %s WHERE id = %s""", (root_id, root_id))

    if 'children' in root:
        for child in root['children']:
            store_tree_r(cur, child, root_id, 0, root_id)
    return root_id

# recursively stores the filetree into the  file table
# TODO be mindful of preexisting folders (EG: a shared folder)
# right now all shared folders are blissfully duplicated
def store_tree_r(cur, node, parent_id, parent_depth, root_id):
    is_dir = node['is_dir']

    file_id = cur.lastrowid
    cur.execute("""INSERT INTO Layout(root_id, parent_id, path, path_depth, dir, size)
                   VALUES(%s,%s,%s,%s,%s,%s)""", (root_id, parent_id, node['path'], parent_depth + 1, node['is_dir'], node['size']))

    new_parent_id = cur.lastrowid 

    if is_dir and 'children' in node:
        for child in node['children']:
            store_tree_r(cur, child, new_parent_id, parent_depth + 1, root_id)
