from flask import Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo


class AllMedian(View):

    methods = ['GET']

    def dispatch_request(self):
        ''' Get medians of all asset declarations for all parties
        '''

        medians = mongo.db.mpassetdeclarationmedians \
            .find({}) \
            .sort([
                ("year", flask_pymongo.DESCENDING),
                ("party.slug", flask_pymongo.ASCENDING)
            ])

        # Build response object
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

        return resp
