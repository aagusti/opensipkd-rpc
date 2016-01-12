import sys
import requests
import json
from tools import json_rpc_header
from config import (
    username,
    pass_encrypted,)


url = 'http://127.0.0.1:6543/ws/pbb'

argv = sys.argv
if argv[1:]:
    username = argv[1]
    pass_encrypted = argv[2]
if argv[3:]:
    url = argv[3]

def get_dict(method,params):
    return dict(jsonrpc = '2.0',
                method = method,
                params = params,
                id = 1)

data1 = dict(
        kode    = '611010000300600430',
        tahun  = '1998',)
data2 = dict(
        kode    = '611010000300600430',
        )
data3 = dict(
        kode    = '6110100003',
        tahun  = '1998'
        )
        
#Rekap Desa By Kecamatan    
data4 = dict(
        kode    = '6110100', 
        tahun  = '1998'
        )
         
#Rekap Kecamatan    
data5 = dict(
        tahun  = '1998'
        )

row_dicted = [] #[data1]
row_dicted.append(data2)

headers = json_rpc_header(username, pass_encrypted)
params = dict(data=row_dicted)

#data = get_dict('get_sppt', params)
data = get_dict('get_info_op', params)

jsondata = json.dumps(data, ensure_ascii=False)
print('Send to {url}'.format(url=url))
print(jsondata)          
rows = requests.post(url, data=jsondata, headers=headers)
print('Result:')
print(json.loads(rows.text))
