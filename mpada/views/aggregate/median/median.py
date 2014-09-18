from flask.views import View
import flask_pymongo

from bson.son import SON


class MedianAggregate(View):

    methods = ['GET']

    def get_match(self, party_slugs):
        '''Build and return the match object to be used in aggregation pipeline.
        :param party_slugs: list of party slugs we want to process
        '''
        match = {"$match": {
            "party.slug": {"$in": party_slugs}
        }}


        return match

    def build_median_documents(self, declarations):
        ''' Build the documents containing the median assets for each asset source.
        :param declarations: the aggregation response listing all assets grouped by Party.
        '''

        # The array that will contain our median asset declaration results.
        medians = []

        for declaration in declarations:

            median = {
                "year": declaration['year'],
                "party": {
                    "name": declaration['party']['name'],
                    "acronym": declaration['party']['acronym'],
                    "slug": declaration['party']['slug']
                },
                "realEstate": {
                    "individual": self.median(declaration['realEstate']['individual']),
                    "joint": self.median(declaration['realEstate']['joint']),
                    "total": self.median(declaration['realEstate']['total'])
                },
                "movable": {
                    "individual": self.median(declaration['movable']['individual']),
                    "joint": self.median(declaration['movable']['joint']),
                    "total": self.median(declaration['movable']['total'])
                },
                "shares": {
                    "individual": self.median(declaration['shares']['individual']),
                    "joint": self.median(declaration['shares']['joint']),
                    "total": self.median(declaration['shares']['total'])
                },
                "bonds": {
                    "individual": self.median(declaration['bonds']['individual']),
                    "joint": self.median(declaration['bonds']['joint']),
                    "total": self.median(declaration['bonds']['total'])
                },
                "cash": {
                    "individual": self.median(declaration['cash']['individual']),
                    "joint": self.median(declaration['cash']['joint']),
                    "total": self.median(declaration['cash']['total'])
                },
                "debtsOrOutstanding": {
                    "individual": self.median(declaration['debtsOrOutstanding']['individual']),
                    "joint": self.median(declaration['debtsOrOutstanding']['joint']),
                    "total": self.median(declaration['debtsOrOutstanding']['total'])
                },
                "annualSalary": {
                    "regular": {
                        "individual": self.median(declaration['annualSalary']['regular']['individual']),
                        "joint": self.median(declaration['annualSalary']['regular']['joint']),
                        "total": self.median(declaration['annualSalary']['regular']['total'])
                    },
                    "honorariums": {
                        "individual": self.median(declaration['annualSalary']['honorariums']['individual']),
                        "joint": self.median(declaration['annualSalary']['honorariums']['joint']),
                        "total": self.median(declaration['annualSalary']['honorariums']['total'])
                    },
                    'totals': {
                        "individual": self.median(declaration['annualSalary']['totals']['individual']),
                        "joint": self.median(declaration['annualSalary']['totals']['joint']),
                        "total": self.median(declaration['annualSalary']['totals']['total'])
                    }
                },
                "totals": {
                    "individual": self.median(declaration['totals']['individual']),
                    "joint": self.median(declaration['totals']['joint']),
                    "total": self.median(declaration['totals']['total'])
                }
            }

            medians.append(median)

        return medians

    def median(self, numbers):
        ''' Get the median value from a list of numbers.
        param numbers: the list of numbers.
        '''

        # First let's get rid of the zeros and sort.
        numbers = filter(lambda number: number != 0, numbers)
        numbers.sort()

        # Different way to get Median depending on whether the list size
        # is even or odd.
        list_length = len(numbers)
        if list_length > 0:
            if list_length % 2 == 0:
                return float((numbers[list_length / 2] + numbers[list_length / 2 - 1])) / 2

            else:
                return numbers[list_length / 2]
        else:
            return 0

    def get_group(self):
        ''' Build and return the aggregation pipeline's group object.
        '''
        group = {
            '$group': {
                '_id': {
                    'year': '$year',
                    'party': {
                        'acronym': '$party.acronym',
                        'name': '$party.name',
                        'slug': '$party.slug'
                    }
                },

                # Total real estate assets.
                'realEstateIndividual': {
                    '$addToSet': '$realEstate.individual'
                },
                'realEstateJoint': {
                    '$addToSet': '$realEstate.joint'
                },
                'realEstateTotal': {
                    '$addToSet': '$realEstate.total'
                },

                # Total movable assets.
                'movableIndividual': {
                    '$addToSet': '$movable.individual'
                },
                'movableJoint': {
                    '$addToSet': '$movable.joint'
                },
                'movableTotal': {
                    '$addToSet': '$movable.total'
                },

                # Total individual assets.
                'sharesIndividual': {
                    '$addToSet': '$shares.individual'
                },
                'sharesJoint': {
                    '$addToSet': '$shares.joint'
                },
                'sharesTotal': {
                    '$addToSet': '$shares.total'
                },

                # Total bonds assets.
                'bondsIndividual': {
                    '$addToSet': '$bonds.individual'
                },
                'bondsJoint': {
                    '$addToSet': '$bonds.joint'
                },
                'bondsTotal': {
                    '$addToSet': '$bonds.total'
                },

                # Total cash assets.
                'cashIndividual': {
                    '$addToSet': '$cash.individual'
                },
                'cashJoint': {
                    '$addToSet': '$cash.joint'
                },
                'cashTotal': {
                    '$addToSet': '$cash.total'
                },

                # Total debs or outstanding assets.
                'debtsOrOutstandingIndividual': {
                    '$addToSet': '$debtsOrOutstanding.individual'
                },
                'debtsOrOutstandingJoint': {
                    '$addToSet': '$debtsOrOutstanding.joint'
                },
                'debtsOrOutstandingTotal': {
                    '$addToSet': '$debtsOrOutstanding.total'
                },

                # Total regular annual salary assets.
                'annualSalaryRegularIndividual': {
                    '$addToSet': '$annualSalary.regular.individual'
                },
                'annualSalaryRegularJoint': {
                    '$addToSet': '$annualSalary.regular.joint'
                },
                'annualSalaryRegularTotal': {
                    '$addToSet': '$annualSalary.regular.total'
                },

                # Total honorarium annual salary assets.
                'annualSalaryHonorariumsIndividual': {
                    '$addToSet': '$annualSalary.honorariums.individual'
                },
                'annualSalaryHonorariumsJoint': {
                    '$addToSet': '$annualSalary.honorariums.joint'
                },
                'annualSalaryHonorariumsTotal': {
                    '$addToSet': '$annualSalary.honorariums.total'
                },

                # Total annual salary assets.
                'totalAnnualSalaryIndividual': {
                    '$addToSet': '$annualSalary.totals.individual'
                },
                'totalAnnualSalaryJoint': {
                    '$addToSet': '$annualSalary.totals.joint'
                },
                'totalAnnualSalary': {
                    '$addToSet': '$annualSalary.totals.total'
                },

                # Totals
                'totalIndividual': {
                    '$addToSet': '$totals.individual'
                },
                'totalJoint': {
                    '$addToSet': '$totals.joint'
                },
                'total': {
                    '$addToSet': '$totals.total'
                }
            }
        }

        return group

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

    def get_projection(self):
        ''' Get the projection object to be used in the aggregation pipeline.
        '''
        project = {
            '$project': {
                '_id': 0,  # hide _id field
                'year': '$_id.year',
                'party': {
                    'acronym': '$_id.party.acronym',
                    'name': '$_id.party.name',
                    'slug': '$_id.party.slug'
                },
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
                        'individual': '$totalAnnualSalaryIndividual',
                        'joint': '$totalAnnualSalaryJoint',
                        'total': '$totalAnnualSalary',
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
