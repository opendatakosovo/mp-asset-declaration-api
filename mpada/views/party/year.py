from flask import Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo


class PartyYear(View):

    methods = ['GET']

    def dispatch_request(self, year, party_slug):
        ''' Get the asset declaration for a given MP.
        :param year: year of the asset declaration.
        :param party_slug: slug value of the Party.
        '''
        query = {'party.slug': party_slug, 'year': year}

        declaration = mongo.db.mpassetdeclarations.find(query).sort([
            ("mp.slug", flask_pymongo.ASCENDING)
        ])

        # Build response object
        resp = Response(
            response=json_util.dumps(declaration), mimetype='application/json')

        return resp
