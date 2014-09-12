from flask import Response
from bson import json_util

from mpada import mongo
from median import MedianAggregate


class AllMedian(MedianAggregate):

    methods = ['GET']

    def dispatch_request(self):
        ''' Get the median asset declaration for all parties.
        '''

        # Group.
        group = self.get_group()

        # Sort.
        sort = self.get_sort()

        # Projection.
        project = self.get_projection()

        # Execute aggregate query.
        result_doc = mongo.db.mpassetdeclarations.aggregate([
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
