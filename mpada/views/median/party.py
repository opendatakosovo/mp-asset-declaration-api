from flask import Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo


class PartyMedian(View):

    methods = ['GET']

    def dispatch_request(self, party_slug):
        ''' Get medians of all asset declarations of a given Party.
        :param party_slug: the slug representation of the Party.
        '''

        query = {'party.slug': party_slug}

        medians = mongo.db.mpassetdeclarationmedians \
            .find(query) \
            .sort([
                ("year", flask_pymongo.DESCENDING)
            ])

        # Build response object
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

        return resp
