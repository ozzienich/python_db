'''
depedency package:

thrift                             0.11.0
thrift-sasl                        0.4.3
thriftpy2                          0.4.14
impyla                             0.18a1

'''

from impala.dbapi import connect
conn = connect(host='10.xx.xx.xx', port=21050,database='nama_database') 
cursor = conn.cursor()
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

print("show list TABEL")
for t in tables:
    print(t[0])
