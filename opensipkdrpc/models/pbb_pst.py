import sys
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    Text,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    String,
    SmallInteger,
    types,
    func,
    ForeignKeyConstraint,
    literal_column,
    )
from sqlalchemy.orm import aliased

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref
    )
import re
from ..tools import as_timezone, FixLength

from ..models import CommonModel, pbb_Base, pbb_DBSession
#from pbb_ref_wilayah import Kelurahan, Kecamatan, Dati2, KELURAHAN, KECAMATAN

class Seksi(pbb_Base, CommonModel):
    __tablename__  = 'ref_seksi'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
                      
class PstJenis(pbb_Base, CommonModel):
    __tablename__  = 'ref_jns_pelayanan'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
                      
class PstBerkasKirim(pbb_Base, CommonModel):
    __tablename__  = 'berkas_kirim'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}

class PstBerkasTerima(pbb_Base, CommonModel):
    __tablename__  = 'berkas_terima'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
                      
class PstPermohonan(pbb_Base, CommonModel):
    __tablename__  = 'pst_permohonan'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls).\
                             filter(cls.kd_kanwil==r['kd_kanwil'],
                                       cls.kd_kantor==r['kd_kantor'],
                                       cls.thn_pelayanan==r['thn_pelayanan'], 
                                       cls.bundel_pelayanan==r['bundel_pelayanan'],
                                       cls.no_urut_pelayanan==r['no_urut_pelayanan'],).\
                             first()
    
class PstLampiran(pbb_Base, CommonModel):
    __tablename__  = 'pst_lampiran'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls).\
                             filter(cls.kd_kanwil==r['kd_kanwil'],
                                       cls.kd_kantor==r['kd_kantor'],
                                       cls.thn_pelayanan==r['thn_pelayanan'], 
                                       cls.bundel_pelayanan==r['bundel_pelayanan'],
                                       cls.no_urut_pelayanan==r['no_urut_pelayanan'],).\
                             first()
    
class PstDetail(pbb_Base, CommonModel):
    __tablename__  = 'pst_detail'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls).\
                             filter(cls.kd_kanwil            ==r['kd_kanwil'],
                                    cls.kd_kanwil            ==r['kd_kanwil'], 
                                    cls.kd_kantor            ==r['kd_kantor'], 
                                    cls.thn_pelayanan        ==r['thn_pelayanan'], 
                                    cls.bundel_pelayanan     ==r['bundel_pelayanan'], 
                                    cls.no_urut_pelayanan    ==r['no_urut_pelayanan'], 
                                    cls.kd_propinsi_pemohon  ==r['kd_propinsi_pemohon'], 
                                    cls.kd_dati2_pemohon     ==r['kd_dati2_pemohon'], 
                                    cls.kd_kecamatan_pemohon ==r['kd_kecamatan_pemohon'], 
                                    cls.kd_kelurahan_pemohon ==r['kd_kelurahan_pemohon'], 
                                    cls.kd_blok_pemohon      ==r['kd_blok_pemohon'], 
                                    cls.no_urut_pemohon      ==r['no_urut_pemohon'], 
                                    cls.kd_jns_op_pemohon    ==r['kd_jns_op_pemohon'], ).\
                             first()
    @classmethod
    def get_position(cls, r):
        return pbb_DBSession.query(cls.kd_kanwil, cls.kd_kantor, cls.thn_pelayanan, 
                    cls.bundel_pelayanan, cls.no_urut_pelayanan, 
                    cls.kd_propinsi_pemohon, cls.kd_dati2_pemohon, cls.kd_kecamatan_pemohon, 
                    cls.kd_kelurahan_pemohon, cls.kd_blok_pemohon, cls.no_urut_pemohon, 
                    cls.kd_jns_op_pemohon, cls.kd_jns_pelayanan, cls.thn_pajak_permohonan, 
                    cls.nama_penerima, cls.catatan_penyerahan, cls.status_selesai, 
                    cls.tgl_selesai, cls.kd_seksi_berkas, cls.tgl_penyerahan, cls.nip_penyerah,
                    Seksi.kd_seksi, Seksi.nm_seksi).\
                             filter(cls.kd_kanwil            ==r['kd_kanwil'],
                                    cls.kd_kanwil            ==r['kd_kanwil'], 
                                    cls.kd_kantor            ==r['kd_kantor'], 
                                    cls.thn_pelayanan        ==r['thn_pelayanan'], 
                                    cls.bundel_pelayanan     ==r['bundel_pelayanan'], 
                                    cls.no_urut_pelayanan    ==r['no_urut_pelayanan'], 
                                    cls.kd_propinsi_pemohon  ==r['kd_propinsi_pemohon'], 
                                    cls.kd_dati2_pemohon     ==r['kd_dati2_pemohon'], 
                                    cls.kd_kecamatan_pemohon ==r['kd_kecamatan_pemohon'], 
                                    cls.kd_kelurahan_pemohon ==r['kd_kelurahan_pemohon'], 
                                    cls.kd_blok_pemohon      ==r['kd_blok_pemohon'], 
                                    cls.no_urut_pemohon      ==r['no_urut_pemohon'], 
                                    cls.kd_jns_op_pemohon    ==r['kd_jns_op_pemohon'], 
                                    cls.kd_seksi_berkas      == Seksi.kd_seksi)

                                    
    @classmethod
    def get_tracking(cls, r):
        SeksiAlias = aliased(Seksi, name='seksi_alias')
        return pbb_DBSession.query(cls.kd_kanwil, cls.kd_kantor, cls.thn_pelayanan, 
                    cls.bundel_pelayanan, cls.no_urut_pelayanan, 
                    cls.kd_propinsi_pemohon, cls.kd_dati2_pemohon, cls.kd_kecamatan_pemohon, 
                    cls.kd_kelurahan_pemohon, cls.kd_blok_pemohon, cls.no_urut_pemohon, 
                    cls.kd_jns_op_pemohon,
                    PstBerkasKirim.kd_seksi,
                    PstBerkasKirim.thn_agenda_kirim,
                    PstBerkasKirim.no_agenda_kirim,
                    PstBerkasKirim.tgl_kirim,
                    PstBerkasTerima.kd_seksi_terima,
                    PstBerkasTerima.tgl_terima,
                    Seksi.nm_seksi.label('pengirim'),
                    SeksiAlias.nm_seksi.label('penerima')).\
                             filter(cls.kd_kanwil            ==r['kd_kanwil'],
                                    cls.kd_kanwil            ==r['kd_kanwil'], 
                                    cls.kd_kantor            ==r['kd_kantor'], 
                                    cls.thn_pelayanan        ==r['thn_pelayanan'], 
                                    cls.bundel_pelayanan     ==r['bundel_pelayanan'], 
                                    cls.no_urut_pelayanan    ==r['no_urut_pelayanan'], 
                                    cls.kd_propinsi_pemohon  ==r['kd_propinsi_pemohon'], 
                                    cls.kd_dati2_pemohon     ==r['kd_dati2_pemohon'], 
                                    cls.kd_kecamatan_pemohon ==r['kd_kecamatan_pemohon'], 
                                    cls.kd_kelurahan_pemohon ==r['kd_kelurahan_pemohon'], 
                                    cls.kd_blok_pemohon      ==r['kd_blok_pemohon'], 
                                    cls.no_urut_pemohon      ==r['no_urut_pemohon'], 
                                    cls.kd_jns_op_pemohon    ==r['kd_jns_op_pemohon'],
                                    
                                    cls.kd_kanwil            ==PstBerkasKirim.kd_kanwil, 
                                    cls.kd_kantor            ==PstBerkasKirim.kd_kantor, 
                                    cls.thn_pelayanan        ==PstBerkasKirim.thn_pelayanan, 
                                    cls.bundel_pelayanan     ==PstBerkasKirim.bundel_pelayanan, 
                                    cls.no_urut_pelayanan    ==PstBerkasKirim.no_urut_pelayanan, 
                                    cls.kd_propinsi_pemohon  ==PstBerkasKirim.kd_propinsi_pemohon, 
                                    cls.kd_dati2_pemohon     ==PstBerkasKirim.kd_dati2_pemohon, 
                                    cls.kd_kecamatan_pemohon ==PstBerkasKirim.kd_kecamatan_pemohon, 
                                    cls.kd_kelurahan_pemohon ==PstBerkasKirim.kd_kelurahan_pemohon, 
                                    cls.kd_blok_pemohon      ==PstBerkasKirim.kd_blok_pemohon, 
                                    cls.no_urut_pemohon      ==PstBerkasKirim.no_urut_pemohon, 
                                    cls.kd_jns_op_pemohon    ==PstBerkasKirim.kd_jns_op_pemohon,
                                    PstBerkasKirim.kd_seksi  ==Seksi.kd_seksi,
                                    PstBerkasKirim.kd_kanwil            ==PstBerkasTerima.kd_kanwil, 
                                    PstBerkasKirim.kd_kantor            ==PstBerkasTerima.kd_kantor, 
                                    PstBerkasKirim.thn_pelayanan        ==PstBerkasTerima.thn_pelayanan, 
                                    PstBerkasKirim.bundel_pelayanan     ==PstBerkasTerima.bundel_pelayanan, 
                                    PstBerkasKirim.no_urut_pelayanan    ==PstBerkasTerima.no_urut_pelayanan, 
                                    PstBerkasKirim.kd_propinsi_pemohon  ==PstBerkasTerima.kd_propinsi_pemohon, 
                                    PstBerkasKirim.kd_dati2_pemohon     ==PstBerkasTerima.kd_dati2_pemohon, 
                                    PstBerkasKirim.kd_kecamatan_pemohon ==PstBerkasTerima.kd_kecamatan_pemohon, 
                                    PstBerkasKirim.kd_kelurahan_pemohon ==PstBerkasTerima.kd_kelurahan_pemohon, 
                                    PstBerkasKirim.kd_blok_pemohon      ==PstBerkasTerima.kd_blok_pemohon, 
                                    PstBerkasKirim.no_urut_pemohon      ==PstBerkasTerima.no_urut_pemohon, 
                                    PstBerkasKirim.kd_jns_op_pemohon    ==PstBerkasTerima.kd_jns_op_pemohon,
                                    PstBerkasKirim.kd_seksi             ==PstBerkasTerima.kd_seksi        ,
                                    PstBerkasKirim.thn_agenda_kirim     ==PstBerkasTerima.thn_agenda_kirim,
                                    PstBerkasKirim.no_agenda_kirim      ==PstBerkasTerima.no_agenda_kirim ,
                                    
                                    PstBerkasTerima.kd_seksi_terima      ==SeksiAlias.kd_seksi,
                                    )
                                    
class PstDataOpBaru(pbb_Base, CommonModel):
    __tablename__  = 'pst_data_op_baru'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    
    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls).\
                             filter(cls.kd_kanwil            ==r['kd_kanwil'],
                                    cls.kd_kanwil            ==r['kd_kanwil'], 
                                    cls.kd_kantor            ==r['kd_kantor'], 
                                    cls.thn_pelayanan        ==r['thn_pelayanan'], 
                                    cls.bundel_pelayanan     ==r['bundel_pelayanan'], 
                                    cls.no_urut_pelayanan    ==r['no_urut_pelayanan'], 
                                    cls.kd_propinsi_pemohon  ==r['kd_propinsi_pemohon'],
                                    cls.kd_dati2_pemohon     ==r['kd_dati2_pemohon'], 
                                    cls.kd_kecamatan_pemohon ==r['kd_kecamatan_pemohon'], 
                                    cls.kd_kelurahan_pemohon ==r['kd_kelurahan_pemohon'], 
                                    cls.kd_blok_pemohon      ==r['kd_blok_pemohon'], 
                                    cls.no_urut_pemohon      ==r['no_urut_pemohon'], 
                                    cls.kd_jns_op_pemohon    ==r['kd_jns_op_pemohon'], ).\
                             first()
                             
class PstPengurangan(pbb_Base, CommonModel):
    __tablename__  = 'pst_permohonan_pengurangan'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls).\
                             filter(cls.kd_kanwil            ==r['kd_kanwil'],
                                    cls.kd_kanwil            ==r['kd_kanwil'], 
                                    cls.kd_kantor            ==r['kd_kantor'], 
                                    cls.thn_pelayanan        ==r['thn_pelayanan'], 
                                    cls.bundel_pelayanan     ==r['bundel_pelayanan'], 
                                    cls.no_urut_pelayanan    ==r['no_urut_pelayanan'], 
                                    cls.kd_propinsi_pemohon  ==r['kd_propinsi_pemohon'],
                                    cls.kd_dati2_pemohon     ==r['kd_dati2_pemohon'], 
                                    cls.kd_kecamatan_pemohon ==r['kd_kecamatan_pemohon'], 
                                    cls.kd_kelurahan_pemohon ==r['kd_kelurahan_pemohon'], 
                                    cls.kd_blok_pemohon      ==r['kd_blok_pemohon'], 
                                    cls.no_urut_pemohon      ==r['no_urut_pemohon'], 
                                    cls.kd_jns_op_pemohon    ==r['kd_jns_op_pemohon'], ).\
                             first()
                             
class MaxUrutPstOl(pbb_Base, CommonModel):
    __tablename__  = 'max_urut_pst_ol'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)          
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4))           
    bundel_pelayanan = Column(String(4))
    no_urut_pelayanan = Column(String(3))

    @classmethod
    def query_data(cls):
        return pbb_DBSession.query(cls)
    
    @classmethod
    def get_nopel(cls, request):
        settings = request.registry.settings
        
        thn_pelayanan = datetime.now().strftime('%Y')
        row = pbb_DBSession.query(cls).first()
        if not row:
            row = cls()
            row.kd_kanwil = settings['pbb_kd_kanwil']
            row.kd_kantor = settings['pbb_kd_kantor']
            row.thn_pelayanan = thn_pelayanan
            row.bundel_pelayanan = '9000'
            row.no_urut_pelayanan = '000'
            
        if row.thn_pelayanan!=thn_pelayanan:
            row.thn_pelayanan = thn_pelayanan
            row.bundel_pelayanan = '9000'
            row.no_urut_pelayanan = '000'
            
        bundel_pelayanan = int(row.bundel_pelayanan)
        no_urut_pelayanan = int(row.no_urut_pelayanan)
        if no_urut_pelayanan == 999:
            bundel_pelayanan +=1
            no_urut_pelayanan = 1
        else:    
            no_urut_pelayanan += 1
            
        row.thn_pelayanan = thn_pelayanan
        row.bundel_pelayanan = str(bundel_pelayanan).zfill(4)
        row.no_urut_pelayanan = str(no_urut_pelayanan).zfill(3)
        pbb_DBSession.add(row)
        pbb_DBSession.flush()
        return (row.kd_kanwil, row.kd_kantor, row.thn_pelayanan, row.bundel_pelayanan, row.no_urut_pelayanan)
        
