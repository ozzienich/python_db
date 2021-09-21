"""
SYNC IMPALA DC TO DRC
-------------------------
Topologi :
KUDU_DC  ----->  TRANSFORMASI  ---> KUDU_DRC

TODO :
- ORDER BY clause
- LIMIT clause
- OFFSET clause


Copyright (c) 2021, OZ
"""
import sys,time
from impala.dbapi import connect
# -------------------------------------------------------------------------------------#    
## PARAMETER
nama_database = 'default'
nama_table = '1_test'
sort_by = 'id'
#limit = '10'
# -------------------------------------------------------------------------------------#    
conn_dc = connect(host="10.xx.xxx.xx", database=nama_database, port=21050, auth_mechanism='GSSAPI', timeout=100000, use_ssl=False, ca_cert=None, ldap_user=None, ldap_password=None,  kerberos_service_name='impala',krb_host='impala.XXXs.Xo.id')

conn_drc = connect(host="10.xx.xxx.xx", database=nama_database, port=21050, auth_mechanism='GSSAPI', timeout=100000, use_ssl=False, ca_cert=None, ldap_user=None, ldap_password=None,  kerberos_service_name='impala',krb_host='drX.XXXXs.Xo.id')

cur_dc = conn_dc.cursor()
cur_drc = conn_drc.cursor()

def total_dc():
    cur_dc.execute("SELECT COUNT(*) FROM " + nama_table)
    for r in cur_dc.fetchall():
        return (r[0])
# -------------------------------------------------------------------------------------#    
def total_drc():
    cur_drc.execute("SELECT COUNT(*) FROM " + nama_table)
    for r in cur_drc.fetchall():
        return (r[0])
# -------------------------------------------------------------------------------------#    
def start_sync():
    try:
        jum_dc = total_dc()
        print("TOTAL DATA DI DC: "+ str(jum_dc))
        jum_drc = total_drc()
        print("TOTAL DATA DI DRC: "+ str(jum_drc))
     

        limit = jum_dc - jum_drc + 1
    
        if jum_dc==jum_drc:
            print ("[INFO] SUDAH SAMA")
            return
        
    except Exception as e:
        print(e)
        sys.exit()
    
    try:
        mulai_dari = (jum_drc - 1)
        print("START KIRIM OFFSET: "+ str(mulai_dari))
        print("==============================================================")      
#        que =  "SELECT * FROM " + nama_table + " ORDER BY " +  sort_by  + " LIMIT 10 OFFSET " + str(mulai_dari)
        que =  "SELECT * FROM " + nama_table + " ORDER BY " +  sort_by  + " ASC LIMIT " + str(limit) + " OFFSET " + str(mulai_dari)
#        que =  "SELECT * FROM " + nama_table + " ORDER BY " +  sort_by  + " ASC LIMIT 10 ")
#        que =  "SELECT * FROM " + nama_table + " LIMIT 10 OFFSET " + str(mulai_dari)
        print (que)
        print("==============================================================")

        cur_dc.execute(que)
        for isi in cur_dc.fetchall():
            
            print(isi)
            field0 = isi[0]
            field1 = isi[1]
            field2 = isi[2]
            field3 = isi[3]
            in_statement = ("INSERT INTO " + nama_table + " VALUES ("+ str(field0)+ ",'" + str(field1)+"'," + str(field2) + ",'" + str(field3) + "')") 
            
            print (in_statement)
            cur_drc.execute(in_statement)
            print ("DONE \n")
            
        
    except Exception as e:
        print(e)
        #sys.exit()
        return

# -------------------------------------------------------------------------------------#    
# START SYNC        
if __name__ == '__main__':
    while True:
        try:
            start_sync()
            print("ISTIRAHAT BOSSS.. \n")
            time.sleep(10)

        except KeyboardInterrupt:
            print >> sys.stderr, ('\n[+] KELAR BOOOOSSSSSS!!!.\n')
            sys.exit(0)
