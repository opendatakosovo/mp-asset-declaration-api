from flask.views import View, request
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
        declarations = mongo.db.mpassetdeclarations.aggregate(aggregate_pipeline)

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

    def get_match(self, party_slugs):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slugs: list of party slugs we want to process
        '''
        match = {"$match": {
            "party.slug": {"$in": party_slugs}
        }}

        return match

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
