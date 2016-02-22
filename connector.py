import MySQLdb as mdb
from secrets import *

from pudb import set_trace

def update(user_id, filetree):
    return null

def read(user_id):
    return null

def store(user_id, file_tree):
    set_trace()
    try:
        con = mdb.connect( 'localhost'
                         , MYSQL_USERNAME
                         , MYSQL_PASSWORD
                         , MYSQL_DBNAME )

        cur = con.cursor()

        # write to layout and file table
        root_id = store_tree(cur, file_tree)

        # write to user table
        cur.execute("""INSERT INTO Users(user_id, root_id, delta_cursor) VALUES(%s,%s,%s)""", (user_id, root_id, "NULL"))

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
    cur.execute("""INSERT INTO Files(hash, name, size) VALUES(%s,%s,%s)""", (root['hash'], root['name'], root['size']))
    file_id = cur.lastrowid

    cur.execute("""INSERT INTO Layout(path, file_id) VALUES(%s,%s)""", (root['path'], file_id))
    root_id = cur.lastrowid

    cur.execute("""UPDATE Layout SET root_id = %s WHERE id = %s""", (root_id, root_id))

    for child in root['children']:
        store_tree_r(cur, child, root_id, root_id)
    return root_id

# recursively stores the filetree into the  file table
# TODO be mindful of preexisting folders (EG: a shared folder)

        # cur.execute("""SELECT id FROM Files WHERE hash = %s""", (node['hash']))
        # existing_id = cur.fetchone()
        # if not existing_id:

def store_tree_r(cur, node, parent_id, root_id):
    is_dir = node['hash'] is not None
    if is_dir:
        cur.execute("""INSERT INTO Files(hash, name, size) VALUES(%s,%s,%s)""", (node['hash'], node['name'], node['size']))
    else:
        cur.execute("""INSERT INTO Files(hash, name, size) VALUES(%s,%s,%s)""", ("NULL", node['name'], node['size']))

    file_id = cur.lastrowid
    cur.execute("""INSERT INTO Layout(root_id, path, file_id) VALUES(%s,%s,%s)""", (root_id, node['path'], file_id))

    if is_dir and 'children' in node:
        for child in node['children']:
            store_tree_r(cur, child, file_id, root_id)
