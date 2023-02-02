import os
from py_scripts import functions as f

conn_edu, cursor_edu = f.connect_to_db('edu',
                                       os.getenv('HOST'),
                                       os.getenv('USER_EDU'),
                                       os.getenv('PASSWORD_EDU'),
                                       os.getenv('PORT')
                                       )

f.create_tables(cursor_edu, conn_edu)