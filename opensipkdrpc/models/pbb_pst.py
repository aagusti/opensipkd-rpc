import sys
from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    Float,
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


# from pbb_ref_wilayah import Kelurahan, Kecamatan, Dati2, KELURAHAN, KECAMATAN

class Seksi(pbb_Base, CommonModel):
    __tablename__ = 'ref_seksi'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_seksi = Column(String(2), primary_key=True)
    nm_seksi = Column(String(75), nullable=False)
    no_srt_seksi = Column(String(2), nullable=False)
    kode_surat_1 = Column(String(5), nullable=False)
    kode_surat_2 = Column(String(5), nullable=False)


class PstJenis(pbb_Base, CommonModel):
    __tablename__ = 'ref_jns_pelayanan'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_jns_pelayanan = Column(String(2), primary_key=True)
    nm_jenis_pelayanan = Column(String(50), nullable=False)


class PstBerkasKirim(pbb_Base, CommonModel):
    __tablename__ = 'berkas_kirim'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    kd_propinsi_pemohon = Column(String(2), primary_key=True)
    kd_dati2_pemohon = Column(String(2), primary_key=True)
    kd_kecamatan_pemohon = Column(String(3), primary_key=True)
    kd_kelurahan_pemohon = Column(String(3), primary_key=True)
    kd_blok_pemohon = Column(String(3), primary_key=True)
    no_urut_pemohon = Column(String(4), primary_key=True)
    kd_jns_op_pemohon = Column(String(1), primary_key=True)
    kd_seksi = Column(String(2), primary_key=True)
    thn_agenda_kirim = Column(String(4), primary_key=True)
    no_agenda_kirim = Column(String(30), primary_key=True)
    tgl_kirim = Column(DateTime, nullable=False)
    nip_pengirim_berkas = Column(String(18), nullable=False)


class PstBerkasTerima(pbb_Base, CommonModel):
    __tablename__ = 'berkas_terima'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    kd_propinsi_pemohon = Column(String(2), primary_key=True)
    kd_dati2_pemohon = Column(String(2), primary_key=True)
    kd_kecamatan_pemohon = Column(String(3), primary_key=True)
    kd_kelurahan_pemohon = Column(String(3), primary_key=True)
    kd_blok_pemohon = Column(String(3), primary_key=True)
    no_urut_pemohon = Column(String(4), primary_key=True)
    kd_jns_op_pemohon = Column(String(1), primary_key=True)
    kd_seksi = Column(String(2), primary_key=True)
    thn_agenda_kirim = Column(String(4), primary_key=True)
    no_agenda_kirim = Column(String(30), primary_key=True)
    kd_seksi_terima = Column(String(2), nullable=False)
    tgl_terima = Column(DateTime)
    nip_penerima_berkas = Column(String(18))


class PstPermohonan(pbb_Base, CommonModel):
    __tablename__ = 'pst_permohonan'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    no_srt_permohonan = Column(String(30))
    tgl_surat_permohonan = Column(DateTime)
    nama_pemohon = Column(String(30))
    alamat_pemohon = Column(String(40))
    keterangan_pst = Column(String(75))
    catatan_pst = Column(String(75))
    status_kolektif = Column(String(1), nullable=False)
    tgl_terima_dokumen_wp = Column(DateTime, nullable=False)
    tgl_perkiraan_selesai = Column(DateTime, nullable=False)
    nip_penerima = Column(String(18), nullable=False)
    no_hp_pemohon = Column(String(15))
    email_pemohon = Column(String(256))

    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'], ). \
            first()


class PstLampiran(pbb_Base, CommonModel):
    __tablename__ = 'pst_lampiran'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    l_permohonan = Column(Float)
    l_surat_kuasa = Column(Float)
    l_ktp_wp = Column(Float)
    l_sertifikat_tanah = Column(Float)
    l_sppt = Column(Float)
    l_imb = Column(Float)
    l_akte_jual_beli = Column(Float)
    l_sk_pensiun = Column(Float)
    l_sppt_stts = Column(Float)
    l_stts = Column(Float)
    l_sk_pengurangan = Column(Float)
    l_sk_keberatan = Column(Float)
    l_skkp_pbb = Column(Float)
    l_spmkp_pbb = Column(Float)
    l_lain_lain = Column(Float)

    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'], ). \
            first()


class PstDetail(pbb_Base, CommonModel):
    __tablename__ = 'pst_detail'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    kd_propinsi_pemohon = Column(String(2), primary_key=True)
    kd_dati2_pemohon = Column(String(2), primary_key=True)
    kd_kecamatan_pemohon = Column(String(3), primary_key=True)
    kd_kelurahan_pemohon = Column(String(3), primary_key=True)
    kd_blok_pemohon = Column(String(3), primary_key=True)
    no_urut_pemohon = Column(String(4), primary_key=True)
    kd_jns_op_pemohon = Column(String(1), primary_key=True)
    kd_jns_pelayanan = Column(String(2))
    thn_pajak_permohonan = Column(String(4))
    nama_penerima = Column(String(30))
    catatan_penyerahan = Column(String(75))
    status_selesai = Column(Float, nullable=False)
    tgl_selesai = Column(DateTime, nullable=False)
    kd_seksi_berkas = Column(String(2), nullable=False)
    tgl_penyerahan = Column(DateTime)
    nip_penyerah = Column(String(18))
    jns_mutasi = Column(String(1))

    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'],
                   cls.kd_propinsi_pemohon == r['kd_propinsi_pemohon'],
                   cls.kd_dati2_pemohon == r['kd_dati2_pemohon'],
                   cls.kd_kecamatan_pemohon == r['kd_kecamatan_pemohon'],
                   cls.kd_kelurahan_pemohon == r['kd_kelurahan_pemohon'],
                   cls.kd_blok_pemohon == r['kd_blok_pemohon'],
                   cls.no_urut_pemohon == r['no_urut_pemohon'],
                   cls.kd_jns_op_pemohon == r['kd_jns_op_pemohon'], ). \
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
                                   Seksi.kd_seksi, Seksi.nm_seksi). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'],
                   cls.kd_propinsi_pemohon == r['kd_propinsi_pemohon'],
                   cls.kd_dati2_pemohon == r['kd_dati2_pemohon'],
                   cls.kd_kecamatan_pemohon == r['kd_kecamatan_pemohon'],
                   cls.kd_kelurahan_pemohon == r['kd_kelurahan_pemohon'],
                   cls.kd_blok_pemohon == r['kd_blok_pemohon'],
                   cls.no_urut_pemohon == r['no_urut_pemohon'],
                   cls.kd_jns_op_pemohon == r['kd_jns_op_pemohon'],
                   cls.kd_seksi_berkas == Seksi.kd_seksi)

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
                                   SeksiAlias.nm_seksi.label('penerima')). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'],
                   cls.kd_propinsi_pemohon == r['kd_propinsi_pemohon'],
                   cls.kd_dati2_pemohon == r['kd_dati2_pemohon'],
                   cls.kd_kecamatan_pemohon == r['kd_kecamatan_pemohon'],
                   cls.kd_kelurahan_pemohon == r['kd_kelurahan_pemohon'],
                   cls.kd_blok_pemohon == r['kd_blok_pemohon'],
                   cls.no_urut_pemohon == r['no_urut_pemohon'],
                   cls.kd_jns_op_pemohon == r['kd_jns_op_pemohon'],

                   cls.kd_kanwil == PstBerkasKirim.kd_kanwil,
                   cls.kd_kantor == PstBerkasKirim.kd_kantor,
                   cls.thn_pelayanan == PstBerkasKirim.thn_pelayanan,
                   cls.bundel_pelayanan == PstBerkasKirim.bundel_pelayanan,
                   cls.no_urut_pelayanan == PstBerkasKirim.no_urut_pelayanan,
                   cls.kd_propinsi_pemohon == PstBerkasKirim.kd_propinsi_pemohon,
                   cls.kd_dati2_pemohon == PstBerkasKirim.kd_dati2_pemohon,
                   cls.kd_kecamatan_pemohon == PstBerkasKirim.kd_kecamatan_pemohon,
                   cls.kd_kelurahan_pemohon == PstBerkasKirim.kd_kelurahan_pemohon,
                   cls.kd_blok_pemohon == PstBerkasKirim.kd_blok_pemohon,
                   cls.no_urut_pemohon == PstBerkasKirim.no_urut_pemohon,
                   cls.kd_jns_op_pemohon == PstBerkasKirim.kd_jns_op_pemohon,
                   PstBerkasKirim.kd_seksi == Seksi.kd_seksi,
                   PstBerkasKirim.kd_kanwil == PstBerkasTerima.kd_kanwil,
                   PstBerkasKirim.kd_kantor == PstBerkasTerima.kd_kantor,
                   PstBerkasKirim.thn_pelayanan == PstBerkasTerima.thn_pelayanan,
                   PstBerkasKirim.bundel_pelayanan == PstBerkasTerima.bundel_pelayanan,
                   PstBerkasKirim.no_urut_pelayanan == PstBerkasTerima.no_urut_pelayanan,
                   PstBerkasKirim.kd_propinsi_pemohon == PstBerkasTerima.kd_propinsi_pemohon,
                   PstBerkasKirim.kd_dati2_pemohon == PstBerkasTerima.kd_dati2_pemohon,
                   PstBerkasKirim.kd_kecamatan_pemohon == PstBerkasTerima.kd_kecamatan_pemohon,
                   PstBerkasKirim.kd_kelurahan_pemohon == PstBerkasTerima.kd_kelurahan_pemohon,
                   PstBerkasKirim.kd_blok_pemohon == PstBerkasTerima.kd_blok_pemohon,
                   PstBerkasKirim.no_urut_pemohon == PstBerkasTerima.no_urut_pemohon,
                   PstBerkasKirim.kd_jns_op_pemohon == PstBerkasTerima.kd_jns_op_pemohon,
                   PstBerkasKirim.kd_seksi == PstBerkasTerima.kd_seksi,
                   PstBerkasKirim.thn_agenda_kirim == PstBerkasTerima.thn_agenda_kirim,
                   PstBerkasKirim.no_agenda_kirim == PstBerkasTerima.no_agenda_kirim,

                   PstBerkasTerima.kd_seksi_terima == SeksiAlias.kd_seksi,
                   )


class PstDataOpBaru(pbb_Base, CommonModel):
    __tablename__ = 'pst_data_op_baru'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    kd_propinsi_pemohon = Column(String(2), primary_key=True)
    kd_dati2_pemohon = Column(String(2), primary_key=True)
    kd_kecamatan_pemohon = Column(String(3), primary_key=True)
    kd_kelurahan_pemohon = Column(String(3), primary_key=True)
    kd_blok_pemohon = Column(String(3), primary_key=True)
    no_urut_pemohon = Column(String(4), primary_key=True)
    kd_jns_op_pemohon = Column(String(1), primary_key=True)
    nama_wp_baru = Column(String(30), nullable=False)
    letak_op_baru = Column(String(35), nullable=False)

    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'],
                   cls.kd_propinsi_pemohon == r['kd_propinsi_pemohon'],
                   cls.kd_dati2_pemohon == r['kd_dati2_pemohon'],
                   cls.kd_kecamatan_pemohon == r['kd_kecamatan_pemohon'],
                   cls.kd_kelurahan_pemohon == r['kd_kelurahan_pemohon'],
                   cls.kd_blok_pemohon == r['kd_blok_pemohon'],
                   cls.no_urut_pemohon == r['no_urut_pemohon'],
                   cls.kd_jns_op_pemohon == r['kd_jns_op_pemohon'], ). \
            first()


class PstPengurangan(pbb_Base, CommonModel):
    __tablename__ = 'pst_permohonan_pengurangan'
    __table_args__ = {'extend_existing': True,
                      'schema': pbb_Base.pbb_schema}
    kd_kanwil = Column(String(2), primary_key=True)
    kd_kantor = Column(String(2), primary_key=True)
    thn_pelayanan = Column(String(4), primary_key=True)
    bundel_pelayanan = Column(String(4), primary_key=True)
    no_urut_pelayanan = Column(String(3), primary_key=True)
    kd_propinsi_pemohon = Column(String(2), primary_key=True)
    kd_dati2_pemohon = Column(String(2), primary_key=True)
    kd_kecamatan_pemohon = Column(String(3), primary_key=True)
    kd_kelurahan_pemohon = Column(String(3), primary_key=True)
    kd_blok_pemohon = Column(String(3), primary_key=True)
    no_urut_pemohon = Column(String(4), primary_key=True)
    kd_jns_op_pemohon = Column(String(1), primary_key=True)
    jns_pengurangan = Column(String(1), nullable=False)
    pct_permohonan_pengurangan = Column(Float, nullable=False)

    @classmethod
    def get_by_nopel(cls, r):
        return pbb_DBSession.query(cls). \
            filter(cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kanwil == r['kd_kanwil'],
                   cls.kd_kantor == r['kd_kantor'],
                   cls.thn_pelayanan == r['thn_pelayanan'],
                   cls.bundel_pelayanan == r['bundel_pelayanan'],
                   cls.no_urut_pelayanan == r['no_urut_pelayanan'],
                   cls.kd_propinsi_pemohon == r['kd_propinsi_pemohon'],
                   cls.kd_dati2_pemohon == r['kd_dati2_pemohon'],
                   cls.kd_kecamatan_pemohon == r['kd_kecamatan_pemohon'],
                   cls.kd_kelurahan_pemohon == r['kd_kelurahan_pemohon'],
                   cls.kd_blok_pemohon == r['kd_blok_pemohon'],
                   cls.no_urut_pemohon == r['no_urut_pemohon'],
                   cls.kd_jns_op_pemohon == r['kd_jns_op_pemohon'], ). \
            first()


class MaxUrutPstOl(pbb_Base, CommonModel):
    __tablename__ = 'max_urut_pst_ol'
    __table_args__ = {'extend_existing': True,
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

        if row.thn_pelayanan != thn_pelayanan:
            row.thn_pelayanan = thn_pelayanan
            row.bundel_pelayanan = '9000'
            row.no_urut_pelayanan = '000'

        bundel_pelayanan = int(row.bundel_pelayanan)
        no_urut_pelayanan = int(row.no_urut_pelayanan)
        if no_urut_pelayanan == 999:
            bundel_pelayanan += 1
            no_urut_pelayanan = 1
        else:
            no_urut_pelayanan += 1

        row.thn_pelayanan = thn_pelayanan
        row.bundel_pelayanan = str(bundel_pelayanan).zfill(4)
        row.no_urut_pelayanan = str(no_urut_pelayanan).zfill(3)
        pbb_DBSession.add(row)
        pbb_DBSession.flush()
        return (row.kd_kanwil, row.kd_kantor, row.thn_pelayanan, row.bundel_pelayanan, row.no_urut_pelayanan)
