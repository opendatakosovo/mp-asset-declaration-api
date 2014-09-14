from flask import Response
from flask.views import View

from bson import json_util
from bson.son import SON

import flask_pymongo

from mpada import mongo


class AllSumAggregate(View):

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

        results = declarations['result']

        # Build response object.
        resp = Response(
            response=json_util.dumps(results), mimetype='application/json')

        # Return response.
        return resp

    methods = ['GET']

    def get_sort(self):
        ''' Build and return the sort object to be used in aggregation pipeline.
        '''
        sort = {
            '$sort': SON([
                ('_id.party.slug', flask_pymongo.ASCENDING),
                ('_id.year', flask_pymongo.DESCENDING)
            ])
        }

        return sort

    def get_group(self):
        ''' Build and return the group object to be used in aggregation pipeline.
        '''
        group = {
            '$group': {
                '_id': {
                    'party': '$party',
                    'year': '$year'
                },

                # Total real estate assets.
                'realEstateIndividual': {
                    '$sum': '$realEstate.individual'
                },
                'realEstateJoint': {
                    '$sum': '$realEstate.joint'
                },
                'realEstateTotal': {
                    '$sum': '$realEstate.total'
                },

                # Total movable assets.
                'movableIndividual': {
                    '$sum': '$movable.individual'
                },
                'movableJoint': {
                    '$sum': '$movable.joint'
                },
                'movableTotal': {
                    '$sum': '$movable.total'
                },

                # Total individual assets.
                'sharesIndividual': {
                    '$sum': '$shares.individual'
                },
                'sharesJoint': {
                    '$sum': '$shares.joint'
                },
                'sharesTotal': {
                    '$sum': '$shares.total'
                },

                # Total bonds assets.
                'bondsIndividual': {
                    '$sum': '$bonds.individual'
                },
                'bondsJoint': {
                    '$sum': '$bonds.joint'
                },
                'bondsTotal': {
                    '$sum': '$bonds.total'
                },

                # Total cash assets.
                'cashIndividual': {
                    '$sum': '$cash.individual'
                },
                'cashJoint': {
                    '$sum': '$cash.joint'
                },
                'cashTotal': {
                    '$sum': '$cash.total'
                },

                # Total debs or outstanding assets.
                'debtsOrOutstandingIndividual': {
                    '$sum': '$debtsOrOutstanding.individual'
                },
                'debtsOrOutstandingJoint': {
                    '$sum': '$debtsOrOutstanding.joint'
                },
                'debtsOrOutstandingTotal': {
                    '$sum': '$debtsOrOutstanding.total'
                },

                # Total regular annual salary assets.
                'annualSalaryRegularIndividual': {
                    '$sum': '$annualSalary.regular.individual'
                },
                'annualSalaryRegularJoint': {
                    '$sum': '$annualSalary.regular.joint'
                },
                'annualSalaryRegularTotal': {
                    '$sum': '$annualSalary.regular.total'
                },

                # Total honorarium annual salary assets.
                'annualSalaryHonorariumsIndividual': {
                    '$sum': '$annualSalary.honorariums.individual'
                },
                'annualSalaryHonorariumsJoint': {
                    '$sum': '$annualSalary.honorariums.joint'
                },
                'annualSalaryHonorariumsTotal': {
                    '$sum': '$annualSalary.honorariums.total'
                },

                # Totals
                'totalIndividual': {
                    '$sum': '$totals.individual'
                },
                'totalJoint': {
                    '$sum': '$totals.joint'
                },
                'total': {
                    '$sum': '$totals.total'
                }
            }
        }

        return group

    def get_projection(self):
        ''' Get the projection object to be used in aggregation pipeline.
        '''
        project = {
            '$project': {
                '_id': 0,  # hide _id field
                'party': '$_id.party',
                'year': '$_id.year',

                'realEstate': {
                    'individual': '$realEstateIndividual',
                    'joint': '$realEstateJoint',
                    'total': '$realEstateTotal'
                },
                'movable': {
                    'individual': '$movableIndividual',
                    'joint': '$movableJoint',
                    'total': '$movableTotal'
                },
                'shares': {
                    'individual': '$sharesIndividual',
                    'joint': '$sharesJoint',
                    'total': '$sharesTotal'
                },
                'bonds': {
                    'individual': '$bondsIndividual',
                    'joint': '$bondsJoint',
                    'total': '$bondsTotal'
                },
                'cash': {
                    'individual': '$cashIndividual',
                    'joint': '$cashJoint',
                    'total': '$cashTotal'
                },
                'debtsOrOutstanding': {
                    'individual': '$debtsOrOutstandingIndividual',
                    'joint': '$debtsOrOutstandingJoint',
                    'total': '$debtsOrOutstandingTotal'
                },
                'annualSalary': {
                    'regular': {
                        'individual': '$annualSalaryRegularIndividual',
                        'joint': '$annualSalaryRegularJoint',
                        'total': '$annualSalaryRegularTotal',
                    },
                    'honorariums': {
                        'individual': '$annualSalaryHonorariumsIndividual',
                        'joint': '$annualSalaryHonorariumsJoint',
                        'total': '$annualSalaryHonorariumsTotal',
                    },
                    'totals': {
                        'individual': {'$add': [
                            '$annualSalaryRegularIndividual',
                            '$annualSalaryHonorariumsIndividual'
                        ]},
                        'joint': {'$add': [
                            '$annualSalaryRegularJoint',
                            '$annualSalaryHonorariumsJoint'
                        ]},
                        'total': {'$add': [
                            '$annualSalaryRegularTotal',
                            '$annualSalaryHonorariumsTotal'
                        ]},
                    }
                },
                'totals': {
                    'individual': '$totalIndividual',
                    'joint': '$totalJoint',
                    'total': '$total'
                }
            }
        }

        return project
