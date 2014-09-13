from flask.views import View
from flask import Response
from bson import json_util
from bson.son import SON

from mpada import mongo

import flask_pymongo


class DeclararedPartyMPsAggregate(View):

    methods = ['GET']

    def dispatch_request(self):
        ''' Get the asset declaration of a Party for the given Party slug.
        '''

        # Define the aggregate pipeline operations
        group_add_mps_to_set = self.get_group_add_mps_to_set()

        unwind_unsorted_mp_set = {"$unwind": "$mps"}

        sort_mps = {
            "$sort": {
                "mps": flask_pymongo.ASCENDING
            }
        }

        group_push_ordered_mps_to_array = self.get_group_push_ordered_mps_to_array()

        # Sort.
        sort_by_party = {
            '$sort': SON([
                ('_id.party.slug', flask_pymongo.ASCENDING)
            ])
        }

        # Projection.
        project = self.get_projection()

        # Execute aggregate query.
        sorted_mps_grouped_in_parties = mongo.db.mpassetdeclarations.aggregate([
            group_add_mps_to_set,
            unwind_unsorted_mp_set,
            sort_mps,
            group_push_ordered_mps_to_array,
            sort_by_party,
            project
        ])

        result = sorted_mps_grouped_in_parties['result']

        # Build response object.
        resp = Response(
            response=json_util.dumps(result), mimetype='application/json')

        # Return response.
        return resp


    def get_group_add_mps_to_set(self):
        ''' Build and return the group object to be used in aggregation pipeline.
        '''
        group = {
            '$group': {
                '_id': {
                    'party': {
                        'name': '$party.name',
                        'acronym': '$party.acronym',
                        'slug': '$party.slug'
                    }
                },
                'mps': {
                    '$addToSet': '$mp'
                }
            }
        }

        return group

    def get_group_push_ordered_mps_to_array(self):
        ''' Build and return the group object to be used in aggregation pipeline.
        '''
        group = {
            '$group': {
                '_id': {
                    'party': {
                        'name': '$_id.party.name',
                        'acronym': '$_id.party.acronym',
                        'slug': '$_id.party.slug'
                    }
                },
                'mps': {
                    '$push': '$mps'
                }
            }
        }

        return group

    def get_projection(self):
        ''' Get the projection object to be used in the aggregation pipeline.
        '''
        project = {
            '$project': {
                '_id': 0,  # hide _id field
                'party': {
                    'name': '$_id.party.name',
                    'acronym': '$_id.party.acronym',
                    'slug': '$_id.party.slug'
                },
                'mps': '$mps'
            }
        }

        return project
