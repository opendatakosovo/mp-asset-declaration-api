from flask import Response, request
from bson import json_util

from mpada import mongo
from median import MedianAggregate


class AllMedian(MedianAggregate):

    methods = ['GET']

    def dispatch_request(self):
        ''' Get the median asset declaration for all parties.
        '''
        aggregate_pipeline = []

        parties_string = request.args.get('parties', '')
        if parties_string != '':
            party_slugs = parties_string.split(',')
            match = self.get_match(party_slugs)
            aggregate_pipeline.append(match)

        # Group.
        group = self.get_group()
        aggregate_pipeline.append(group)

        # Sort.
        sort = self.get_sort()
        aggregate_pipeline.append(sort)

        # Projection.
        project = self.get_projection()
        aggregate_pipeline.append(project)

        # Execute aggregate query.
        result_doc = mongo.db.mpassetdeclarations.aggregate(aggregate_pipeline)

        # Get aggregate declarations
        declarations = result_doc['result']

        # Build medians doc
        medians = self.build_median_documents(declarations)

        # Build response object.
        resp = Response(
            response=json_util.dumps(medians), mimetype='application/json')

        # Return response.
        return resp
