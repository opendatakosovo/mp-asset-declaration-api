from flask import Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo


class MP(View):

    methods = ['GET']

    def dispatch_request(self, mp_name_slug):
        ''' Get the asset declaration for a given MP.
        :param mp_name_slug: slug value of the MP's name.
        '''
        query = {'mp.slug': mp_name_slug}

        declarations = mongo.db.mpassetdeclarations.find(query).sort([
            ("year", flask_pymongo.DESCENDING),
            ("mp.slug", flask_pymongo.ASCENDING)
        ])

        # Build response object
        resp = Response(
            response=json_util.dumps(declarations), mimetype='application/json')

        return resp
