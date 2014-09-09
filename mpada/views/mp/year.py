from flask import Response
from flask.views import View
from bson import json_util

from mpada import mongo


class MPYear(View):

    methods = ['GET']

    def dispatch_request(self, year, mp_name_slug):
        ''' Get the asset declaration for a given MP.
        :param year: year of the asset declaration.
        :param mp_name_slug: slug value of the MP's name.
        '''
        query = {'mp.slug': mp_name_slug, 'year': year}

        declaration = mongo.db.mpassetdeclarations.find_one(query)

        # Build response object
        resp = Response(
            response=json_util.dumps(declaration), mimetype='application/json')

        return resp
