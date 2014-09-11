from flask import Response
from flask.views import View
from bson import json_util

from mpada import mongo


class PartyYearMedian(View):

    methods = ['GET']

    def dispatch_request(self, year, party_slug):
        ''' Get medians of all asset declarations of a given Party and year.
        :param year: the year of the declaration.
        :param party_slug: the slug representation of the Party.
        '''

        query = {
            'year': year,
            'party.slug': party_slug
        }

        medians = mongo.db.mpassetdeclarationmedians.find(query)

        # Build response object
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

        return resp
