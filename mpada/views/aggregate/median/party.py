from flask import Response
from bson import json_util

from mpada import mongo
from median import MedianAggregate


class PartyMedian(MedianAggregate):

    methods = ['GET']

    def dispatch_request(self, party_slug):
        ''' Get medians of all asset declarations of a given Party.
        :param party_slug: the slug representation of the Party.
        '''
        # Match
        match = self.get_match(party_slug)

        # Group.
        group = self.get_group()

        # Sort.
        sort = self.get_sort()

        # Projection.
        project = self.get_projection()

        # Execute aggregate query.
        result_doc = mongo.db.mpassetdeclarations.aggregate([
            match,
            group,
            sort,
            project
        ])

        # Get aggregate declarations
        declarations = result_doc['result']

        # Build medians doc
        medians = self.build_median_documents(declarations)

        # Build response object.
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

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
