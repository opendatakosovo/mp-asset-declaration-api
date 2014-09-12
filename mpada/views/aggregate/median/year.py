from flask import Response
from bson import json_util

from mpada import mongo
from median import MedianAggregate


class YearMedian(MedianAggregate):

    methods = ['GET']

    def dispatch_request(self, year):
        ''' Get medians of all asset declarations of a given Party and year.
        :param year: the year of the declaration.
        '''

        # Match
        match = self.get_match(year)

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

    def get_match(self, year):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slug: name slug of a party.
        '''
        match = {"$match": {
            'year': year
        }}

        return match
