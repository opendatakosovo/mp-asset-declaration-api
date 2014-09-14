from flask import Response
from bson import json_util


from mpada import mongo
from declaration import DeclarationSumAggregate


class MPSumAggregate(DeclarationSumAggregate):

    def dispatch_request(self, party_slug, mp_slug):
        ''' Get the asset declaration of an MP for the given Party and MP slugs.
        :param party_slug: slug value of the Party.
        :param party_slug: slug value of the MP.
        '''

        # Match.
        match = self.get_match(party_slug, mp_slug)

        # Group.
        group = self.get_group()

        # Sort.
        sort = self.get_sort()

        # Projection.
        project = self.get_projection()

        # Execute aggregate query.
        declarations = mongo.db.mpassetdeclarations.aggregate([
            match,
            group,
            sort,
            project
        ])

        # Build response object.
        resp = Response(
            response=json_util.dumps(declarations), mimetype='application/json')

        # Return response.
        return resp

    def get_match(self, party_slug, mp_slug):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slug: name slug of a party.
        :param mp_name_slug: name slug of an MP.
        '''

        match = {"$match": {
            "party.slug": party_slug,
            'mp.slug': mp_slug
        }}

        return match
