from flask.views import View
from flask import Response
from bson import json_util
from bson.son import SON
from sets import Set

from mpada import mongo

import flask_pymongo


class YearsPartiesDeclaredAggregate(View):

    methods = ['GET']

    def dispatch_request(self):
        ''' Get the asset declaration of a Party for the given Party slug.
        :param party_slug: slug value of the Party.
        '''

        # Group.
        group = self.get_group()

        # Sort.
        sort = self.get_sort()

        # Projection.
        project = self.get_projection()

        # Execute aggregate query.
        declarations = mongo.db.mpassetdeclarations.aggregate([
            group,
            sort,
            project
        ])

        parties = []
        years = Set()

        for declaration in declarations['result']:
            parties.append(declaration['party']['name'])
            for year in declaration['years']:
                years.add(year)

        resp_json = {
            'declared': {
                'parties': parties,
                'years': years,
            },
            'yearsDeclaredPerParties': declarations['result']
        }

        # Build response object.
        resp = Response(
            response=json_util.dumps(resp_json), mimetype='application/json')

        # Return response.
        return resp

    def get_group(self):
        ''' Build and return the group object to be used in aggregation pipeline.
        '''
        group = {
            '$group': {
                '_id': {
                    'party': {
                        'acronym': '$party.acronym',
                        'name': '$party.name',
                        'slug': '$party.slug'
                    }
                },
                'years': {
                    '$addToSet': '$year'
                }
            }
        }

        return group

    def get_sort(self):
        ''' Build and return the sort object to be used in aggregation pipeline.
        '''
        sort = {
            '$sort': SON([
                ('_id.party.slug', flask_pymongo.ASCENDING)
            ])
        }

        return sort

    def get_projection(self):
        ''' Get the projection object to be used in the aggregation pipeline.
        '''
        project = {
            '$project': {
                '_id': 0,  # hide _id field
                'party': {
                    'acronym': '$_id.party.acronym',
                    'name': '$_id.party.name',
                    'slug': '$_id.party.slug'
                },
                'years': '$years'
            }
        }

        return project
