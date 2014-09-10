from flask import Response
from bson import json_util

from mpada import mongo
from declaration import DeclarationAggregate


class PartyAggregate(DeclarationAggregate):

    def dispatch_request(self, party_slug):
        ''' Get the asset declaration of a Party for the given Party slug.
        :param party_slug: slug value of the Party.
        '''

        # Match.
        match = self.get_match(party_slug)

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

    def get_match(self, party_slug):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slug: name slug of a party.
        '''
        match = {"$match": {
            "party.slug": party_slug
        }}

        return match
