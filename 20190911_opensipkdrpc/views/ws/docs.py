import os
import uuid
#from ..tools import row2dict, xls_reader
from datetime import datetime
from sqlalchemy import not_, func
from pyramid.view import (
    view_config,
    )
from pyramid.httpexceptions import (
    HTTPFound,
    )
import colander
from deform import (
    Form,
    widget,
    ValidationFailure,
    )
#from ..models import (
#    DBSession,
#    Group
#    )

########
# List #
########
#@view_config(route_name='pbb-rpc-doc', renderer='templates/pbb-rpc-doc.pt',
#             permission='view')
#def view_list(request):
#    return dict(a={})

