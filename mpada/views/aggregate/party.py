from flask import Response
from bson import json_util

from mpada import mongo
from declaration import DeclarationAggregate


class PartyAggregate(DeclarationAggregate):

    def dispatch_request(self, party_slug):
        ''' Get the asset declaration of a Party for the given Party slug.
        :param party_slug: slug value of the Party.
        '''

        # Query.
        query = self.get_query(party_slug)

        # Group.
        group = self.get_group()

        # Sort.
        sort = self.get_sort()

        # Projection.
        project = self.get_projection()

        # Execute query.
        declarations = mongo.db.mpassetdeclarations.aggregate([
            query,
            group,
            sort,
            project
        ])

        # Build response object.
        resp = Response(
            response=json_util.dumps(declarations), mimetype='application/json')

        # Return response.
        return resp

    def get_query(self, party_slug):
        '''Build and return the query object to be used in aggregation pipeline.
        :param party_slug: name slug of a party.
        '''
        query = {"$match": {
            "party.slug": party_slug
        }}

        return query
