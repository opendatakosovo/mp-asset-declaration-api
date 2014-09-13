from flask import Response
from flask.views import View
from bson import json_util

from mpada import mongo, utils


class Party(View):

    methods = ['GET']

    def dispatch_request(self, party_slug):
        ''' Get the asset declarations for members of a given Party.
        :param party_slug: slug value of the Party.
        '''
        query = {'party.slug': party_slug}
        sort = utils.get_sort('year desc,mp.slug asc')

        declarations = mongo.db.mpassetdeclarations.find(query).sort(sort)

        # Build response object
        resp = Response(
            response=json_util.dumps(declarations), mimetype='application/json')

        return resp
