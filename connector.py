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
def read(user_id):
    try:
        con = connect()
        cur = con.cursor()
        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        result = cur.fetchone()
        if not result:
            return None
        else:
            root_id, = result
            cur.execute("""SELECT * FROM Layout JOIN Files ON Layout.file_id = Files.id
                           WHERE Layout.root_id = %s""", (root_id,))
            return treeify(cur.fetchall())
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# treeifies the db cache
def treeify(rows):
    return treeify_h(rows, {})

# two passes: builds disconnected nodes first, then constructs the
# tree structure given the disconnected nodes
def treeify_h(rows, tab):
    # build nodes first
    for row in rows:
        id, root_id, parent_id, path, file_id, _, is_dir, name, size = row
        node = { 'name': name
               , 'is_dir': is_dir
               , 'path': path
               , 'size': size }
        if is_dir:
            node['children'] = []
        tab[id] = node
    # now build hiearchical tree structure
    for row in rows:
        id, _, parent_id, _, _, _, _, _, _ = row
        if parent_id is not None:
            tab[parent_id]['children'].append(tab[id])
    # return the root
    _, root_id, _, _, _, _, _, _, _ = rows[0]
    return tab[root_id]

# basically mkdir -p
def add_parent_folders(cur, path, root_id):
    if dirname(path) is not path: # while we haven't reached the root (/)
        cur.execute("""SELECT id FROM Layout WHERE path = %s""", (path,))
        result = cur.fetchone()
        if result is None:
            # add all parents before me first
            parent_id = add_parent_folders(cur, dirname(path), root_id)
            cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (True, basename(path), 0))
            file_id = cur.lastrowid
            cur.execute("""INSERT INTO Layout(path, root_id, parent_id, file_id) VALUES(%s,%s,%s,%s)""", (path, root_id, parent_id, file_id))
            root_id = cur.lastrowid
        else:
            file_id, = result
            return file_id
    else:
        return root_id

# increments the parent folder size by some given number
# assumes entries for all parent folders in path exist (IE:
# we called add_parent_folders for path)
def increment_parent_folder_size(cur, path, addition):
    cur.execute("""UPDATE Files
                   INNER JOIN Layout ON Layout.file_id = Files.id\
                    SET Files.size = Files.size + %s\
                   WHERE Layout.path = %s""", (addition, path))
    if dirname(path) is not path: # while we haven't reached the root (/)
        increment_parent_folder_size(cur, dirname(path), addition)

# assumes the user exists in our database
def update_path(user_id, path, metadata):
    try:
        con = connect()
        cur = con.cursor()

        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        root_id, = cur.fetchone()

        # add entries for nonexistent parent folders
        parent_id = add_parent_folders(cur, dirname(path), root_id)

        # grab file_id for the file specified at given path (file_id will be None if it doesn't exist)
        cur.execute("""SELECT file_id FROM Layout WHERE path = %s""", (path,))
        file_id_result = cur.fetchone()

        if metadata['is_dir']:
            # new entry is a folder, create it
            if file_id_result is not None:
                file_id, = file_id_result
                cur.execute("""UPDATE Files SET dir = %s WHERE id = %s""", (True, file_id))
            else:
                # does not exist, add folder to layout and file tables
                cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (True, basename(path), metadata['bytes']))
                file_id = cur.lastrowid
                cur.execute("""INSERT INTO Layout(path, root_id, parent_id, file_id) VALUES(%s,%s,%s,%s)""", (path, root_id, parent_id, file_id))
        else:
            # new entry is a file, replace local state at path with file
            # then update the sizes for all parents recursively up to the root
            if file_id_result is not None:
                delete_path(user_id, path)

            cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (False, basename(path), metadata['bytes']))
            file_id = cur.lastrowid
            cur.execute("""INSERT INTO Layout(path, root_id, parent_id, file_id) VALUES(%s,%s,%s,%s)""", (path, root_id, parent_id, file_id))

            increment_parent_folder_size(cur, dirname(path), metadata['bytes'])

        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# deletes the file or folder (and all children) at the given path
# assumes the user exists in our database
def delete_path(user_id, path):
    try:
        con = connect()
        cur = con.cursor()

        # get root_id from User table and delete all corresponding entries in the File and Layout table
        cur.execute("""SELECT root_id FROM Users WHERE user_id = %s""", (user_id,))
        root_id, = cur.fetchone()
        cur.execute("""DELETE Layout.*, Files.* FROM Layout INNER JOIN Files ON Layout.file_id = Files.id\
                       WHERE Layout.path LIKE %s""", (path + "%",))
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
        cur.execute("""DELETE Layout.*, Files.* FROM Layout INNER JOIN Files ON Layout.file_id = Files.id\
                       WHERE Layout.root_id = %s AND Layout.id <> %s""", (root_id, root_id))
        con.commit()
    except mdb.Error, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        con.rollback()
        sys.exit(1)
    finally:
        if con:
            con.close()

# stores the filetree for a given user
def store(user_id, file_tree):
    try:
        con = connect()
        cur = con.cursor()

        # write to layout and file table
        root_id = store_tree(cur, file_tree)

        # write to user table
        cur.execute("""INSERT INTO Users(user_id, root_id) VALUES(%s,%s)""", (user_id, root_id))

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
    cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (root['is_dir'], root['name'], root['size']))
    file_id = cur.lastrowid

    cur.execute("""INSERT INTO Layout(path, file_id) VALUES(%s,%s)""", (root['path'], file_id))
    root_id = cur.lastrowid

    cur.execute("""UPDATE Layout SET root_id = %s WHERE id = %s""", (root_id, root_id))

    if 'children' in root:
        for child in root['children']:
            store_tree_r(cur, child, root_id, root_id)
    return root_id

# recursively stores the filetree into the  file table
# TODO be mindful of preexisting folders (EG: a shared folder)
# right now all shared folders are blissfully duplicated

def store_tree_r(cur, node, parent_id, root_id):
    is_dir = node['is_dir']
    if is_dir:
        cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (node['is_dir'], node['name'], node['size']))
    else:
        cur.execute("""INSERT INTO Files(dir, name, size) VALUES(%s,%s,%s)""", (False, node['name'], node['size']))

    file_id = cur.lastrowid
    cur.execute("""INSERT INTO Layout(root_id, parent_id, path, file_id) VALUES(%s,%s,%s,%s)""", (root_id, parent_id, node['path'], file_id))

    new_parent_id = cur.lastrowid 

    if is_dir and 'children' in node:
        for child in node['children']:
            store_tree_r(cur, child, new_parent_id, root_id)
