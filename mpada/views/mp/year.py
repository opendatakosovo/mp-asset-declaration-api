from flask import Response
from flask.views import View
from bson import json_util

from mpada import mongo


class MPYear(View):

    methods = ['GET']

    def dispatch_request(self, year, party_slug, mp_slug):
        ''' Get the asset declaration for a given MP.
        :param year: year of the asset declaration.
        :param party_slug: slug value of the Party's name.
        :param mp_slug: slug value of the MP's name.
        '''
        query = {
            'year': year,
            'party.slug': party_slug,
            'mp.slug': mp_slug
        }

        declaration = mongo.db.mpassetdeclarations.find_one(query)

        # Build response object
        resp = Response(
            response=json_util.dumps(declaration), mimetype='application/json')

        return resp
