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
    )

from sqlalchemy.orm.exc import NoResultFound

from sqlalchemy.orm import (
    scoped_session,
    sessionmaker,
    relationship,
    backref
    )
import re
from ..tools import (
    as_timezone,
    FixLength,
    )

from ..models import CommonModel, pbb_Base, pbb_DBSession

KECAMATAN = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),]
    
KELURAHAN = [
    ('kd_propinsi', 2, 'N'),
    ('kd_dati2', 2, 'N'),
    ('kd_kecamatan', 3, 'N'),
    ('kd_kelurahan', 3, 'N'),]
    

class Propinsi(pbb_Base, CommonModel):
    __tablename__  = 'ref_propinsi'
    __table_args__ = {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema}

class Dati2(pbb_Base, CommonModel):
    __tablename__  = 'ref_dati2'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi'], 
                                            ['ref_propinsi.kd_propinsi']),
                     {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema})
                     
class Kecamatan(pbb_Base, CommonModel):
    __tablename__  = 'ref_kecamatan'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2'], 
                                            ['ref_dati2.kd_propinsi', 'ref_dati2.kd_dati2']),
                     {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema})

class Kelurahan(pbb_Base, CommonModel):
    __tablename__  = 'ref_kelurahan'
    __table_args__ = (ForeignKeyConstraint(['kd_propinsi','kd_dati2','kd_kecamatan'], 
                                            ['ref_kecamatan.kd_propinsi', 'ref_kecamatan.kd_dati2',
                                             'ref_kecamatan.kd_kecamatan']),
                     {'extend_existing':True, 'autoload':True,
                      'schema': pbb_Base.pbb_schema})
