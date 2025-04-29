import sys
import logging
import traceback
from types import (
    StringType,
    UnicodeType)
from StringIO import StringIO
from . import( auth_from_rpc, LIMIT, CODE_OK, CODE_NOT_FOUND,
    CODE_DATA_INVALID, CODE_INVALID_LOGIN, CODE_NETWORK_ERROR,
    MSG_OK, MSG_DATA_INVALID,
    )
from pyramid_rpc.jsonrpc import jsonrpc_method, JsonRpcError
from pyramid.view import (view_config,)
from datetime import datetime
import re
from ...models import pbb_DBSession
from ...models.pbb import (
    Sppt,
    DatObjekPajak,
    DatSubjekPajak,
    DatOpBumi,
    DatOpBangunan,
    DatFasilitasBangunan as DatFasilitas
    )
import pprint
# from ..tools import nop_to_id
from ...tools import date_from_str, FixLength
# from ...pbb.tools import nop_to_id

KD_LISTRIK = '44'
KD_AC_SPLIT = '17'
KD_AC_WINDOW = '16'

NOP = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),
    ('kd_kelurahan', 3, 'N'),
    ('kd_blok', 3, 'N'),
    ('no_urut', 4, 'N'),
    ('kd_jns_op', 1, 'N'),]


def invalid_data(msg):
    return dict(code = CODE_DATA_INVALID, message = msg)

    
@jsonrpc_method(method='set_dop', endpoint='ws_pbb')
def set_dop(request, data):
    # Digunakan untuk menerima data spop dan lspop
    resp, user = auth_from_rpc(request)
    if resp['code'] != 0:
        return resp
    conn = pbb_DBSession.connection().connection
    curs = conn.cursor()
    # pbb_DBSession.configure(autoflush=False, autocommit=False)


    if 1==1:
        ret_data =[]
        # pbb_DBSession.begin()
        conn.begin()
        for row in data:
            a = row['pbb_nop']
            pkey = FixLength(NOP)
            pkey.set_raw(a)
            # 36.75.730.003.019-1410.0
            splitDot = a.split('.')
            splitStrip = splitDot[4].split('-')

            s_id = pbb_DBSession.query(DatObjekPajak.subjek_pajak_id).filter(
                                                DatObjekPajak.kd_propinsi == splitDot[0],
                                                DatObjekPajak.kd_dati2 == splitDot[1],
                                                DatObjekPajak.kd_kecamatan == splitDot[2],
                                                DatObjekPajak.kd_kelurahan == splitDot[3],
                                                DatObjekPajak.kd_blok == splitStrip[0],
                                                DatObjekPajak.no_urut == splitStrip[1],
                                                DatObjekPajak.kd_jns_op == splitDot[5],
                                            ).scalar()

            update_op_sql = """update dat_objek_pajak set 
            jalan_op = '{}',
            blok_kav_no_op = '{}',
            rw_op = '{}',
            rt_op = '{}',
            total_luas_bumi = {},
            njop_bumi = {},
            total_luas_bng = {},
            njop_bng = {}
            where dat_objek_pajak.kd_propinsi = '{}'
            and dat_objek_pajak.kd_dati2 = '{}'
            and dat_objek_pajak.kd_kecamatan = '{}'
            and dat_objek_pajak.kd_kelurahan = '{}'
            and dat_objek_pajak.kd_blok = '{}'
            and dat_objek_pajak.no_urut = '{}'
            and dat_objek_pajak.kd_jns_op = '{}'""".\
            format(
                    row['op_alamat'],
                    row['op_blok_kav'],
                    row['op_rw'],
                    row['op_rt'],
                    row['bumi_luas'],
                    row['bumi_njop'],
                    row['bng_luas'],
                    row['bng_njop'],

                    splitDot[0].strip(),
                    splitDot[1].strip(),
                    splitDot[2].strip(),
                    splitDot[3].strip(),
                    splitStrip[0].strip(),
                    splitStrip[1].strip(),
                    splitDot[5].strip(),
                )
            # update_op_sqls.append(update_op_sql)
            try:
                pbb_DBSession.execute(update_op_sql)
            except Exception as e:
                conn.rollback()
                return dict(code = 500, message = 'Gagal proses data. : ' + str(e), params = '')
            

            update_sp_sql = """update dat_subjek_pajak set 
            nm_wp = '{}',
            npwp = '{}',
            jalan_wp = '{}',
            blok_kav_no_wp = '{}',
            kelurahan_wp = '{}',
            rw_wp = '{}',
            rt_wp = '{}',
            kota_wp = '{}'
            where dat_subjek_pajak.subjek_pajak_id = '{}'""".\
            format(
                    # 'yasusdir',
                    row['wp_nama'],
                    row['wp_npwp'].replace('-', '').replace('.', '').strip(),
                    row['wp_alamat'],
                    row['wp_blok_kav'],
                    row['wp_kelurahan'],
                    row['wp_rw'],
                    row['wp_rt'],
                    row['wp_kota'],

                    s_id.strip()
                )
            # update_sp_sqls.append(update_sp_sql)
            try:
                pbb_DBSession.execute(update_sp_sql)
            except Exception as e:
                conn.rollback()
                return dict(code = 500, message = 'Gagal proses data.', params = '')
            
        # pbb_DBSession.flush()
        conn.commit()
        # pbb_DBSession.configure(autoflush=True, autocommit=True)
        # update_sqls = ''.join(update_sp_sqls)
        # update_sqls = update_sqls + ''.join(update_op_sqls)

        # curs.execute(update_sqls)    
        # pbb_DBSession.execute(update_sqls)
    params = dict(data = ret_data)
    return dict(code = CODE_OK, message = MSG_OK, params = params)

