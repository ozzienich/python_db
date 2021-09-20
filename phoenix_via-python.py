'''
depedency packages:
==============================================
phoenixdb                          1.1.0

'''
import phoenixdb
import phoenixdb.cursor
import sys
database_url = 'http://10.xxx.xxx.xx:8765/'

print ("CREATING PQS CONNECTION")
conn = phoenixdb.connect(database_url, autocommit=True, auth="SPNEGO")
cursor = conn.cursor()
cursor.execute("select count (*) from XXX.XXXX")
result =  cursor.fetchall()
print (result)

