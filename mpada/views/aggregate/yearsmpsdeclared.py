from flask.views import View
from flask import Response
from bson import json_util
from bson.son import SON
from sets import Set

from mpada import mongo

import flask_pymongo


class YearsMPsDeclaredAggregate(View):

    methods = ['GET']

    def dispatch_request(self, party_slug, mp_slug=None):
        ''' Get the asset declaration of a Party for the given Party slug.
        :param party_slug: slug value of the Party.
        '''

        # Match
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

        mps = []
        years = Set()
        resp_json = ()

        for declaration in declarations['result']:
            mps.append(declaration['mp']['name'])
            for year in declaration['years']:
                years.add(year)

        if not mp_slug:
            resp_json = {
                'declared': {
                    'mps': mps,
                    'years': years,
                },
                'yearsDeclaredPerMPs': declarations['result']
            }
        else:
            # If we are only retrieving declaration years for one PM
            # then there is no need to list MPs, just grab first list object
            # and return that.
            resp_json = declarations['result'][0]

        # Build response object.
        resp = Response(
            response=json_util.dumps(resp_json), mimetype='application/json')

        # Return response.
        return resp

    def get_match(self, party_slug, mp_slug):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slug: name slug of a party.
        '''

        match = {}

        if not mp_slug:
            match = {"$match": {
                "party.slug": party_slug
            }}
        else:
            match = {"$match": {
                "party.slug": party_slug,
                "mp.slug": mp_slug
            }}

        return match

    def get_group(self):
        ''' Build and return the group object to be used in aggregation pipeline.
        '''
        group = {
            '$group': {
                '_id': {
                    'mp': {
                        'name': '$mp.name',
                        'slug': '$mp.slug'
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
                ('_id.mp.slug', flask_pymongo.ASCENDING)
            ])
        }

        return sort

    def get_projection(self):
        ''' Get the projection object to be used in the aggregation pipeline.
        '''
        project = {
            '$project': {
                '_id': 0,  # hide _id field
                'mp': {
                    'name': '$_id.mp.name',
                    'slug': '$_id.mp.slug'
                },
                'years': '$years'
            }
        }

        return project
