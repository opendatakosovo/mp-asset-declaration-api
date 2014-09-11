from flask import Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo


class YearMedian(View):

    methods = ['GET']

    def dispatch_request(self, year):
        ''' Get medians of all asset declarations of a given Year.
        :param year: the slug representation of the Year.
        '''

        query = {'year': year}

        medians = mongo.db.mpassetdeclarationmedians \
            .find(query) \
            .sort([
                ("party.slug", flask_pymongo.ASCENDING)
            ])

        # Build response object
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

        return resp
