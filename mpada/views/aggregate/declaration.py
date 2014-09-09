from flask import Flask, Response
from flask.views import View
import flask_pymongo

from bson import json_util
from bson.son import SON

from mpada import mongo

class DeclarationAggregate(View):

	methods = ['GET']

	def get_sort(self):
		''' Build and return the sort object to be used in aggregation pipeline.
		'''
		sort = { 
			'$sort': SON([
				('_id', flask_pymongo.DESCENDING)
			])
		}

		return sort

	def get_group(self):
		''' Build and return the group object to be used in aggregation pipeline.
		'''
		group =	{
			'$group':{
				'_id':'$year',

				# Total real estate assets.
				'realEstateIndividual':{
					'$sum':'$realEstate.individual'
				},
				'realEstateJoint':{
					'$sum':'$realEstate.joint'
				},
				'realEstateJoint':{
					'$sum':'$realEstate.total'
				},

				# Total movable assets.
				'movableIndividual':{
					'$sum':'$movable.individual'
				},
				'movableJoint':{
					'$sum':'$movable.joint'
				},
				'movableTotal':{
					'$sum':'$movable.total'
				},

				# Total individual assets.
				'sharesIndividual':{
					'$sum':'$shares.individual'
				},
				'sharesJoint':{
					'$sum':'$shares.joint'
				},
				'sharesTotal':{
					'$sum':'$shares.total'
				},

				# Total bonds assets.
				'bondsIndividual':{
					'$sum':'$bonds.individual'
				},
				'bondsJoint':{
					'$sum':'$bonds.joint'
				},
				'bondsTotal':{
					'$sum':'$bonds.total'
				},

				# Total cash assets.
				'cashIndividual':{
					'$sum':'$cash.individual'
				},
				'cashJoint':{
					'$sum':'$cash.joint'
				},
				'cashTotal':{
					'$sum':'$cash.total'
				},

				# Total debs or outstanding assets.
				'debtsOrOutstandingIndividual':{
					'$sum':'$debtsOrOutstanding.individual'
				},
				'debtsOrOutstandingJoint':{
					'$sum':'$debtsOrOutstanding.joint'
				},
				'debtsOrOutstandingTotal':{
					'$sum':'$debtsOrOutstanding.total'
				},

				# Total regular annual salary assets.
				'annualSalaryRegularIndividual':{
					'$sum':'$annualSalary.regular.individual'
				},
				'annualSalaryRegularJoint':{
					'$sum':'$annualSalary.regular.joint'
				},
				'annualSalaryRegularTotal':{
					'$sum':'$annualSalary.regular.total'
				},

				# Total honorarium annual salary assets.
				'annualSalaryHonorariumsIndividual':{
					'$sum':'$annualSalary.honorariums.individual'
				},
				'annualSalaryHonorariumsJoint':{
					'$sum':'$annualSalary.honorariums.joint'
				},
				'annualSalaryHonorariumsTotal':{
					'$sum':'$annualSalary.honorariums.total'
				},

				# Totals
				'totalIndividual':{
					'$sum':'$totals.individual'
				},
				'totalJoint':{
					'$sum':'$totals.joint'
				},
				'total':{
					'$sum':'$totals.total'
				}
			}
		}

		return group


	def get_projection(self):
		''' Get the projection object to be used in aggregation pipeline.
		'''
		project = {
			'$project':{
				'_id': 0,
				'year': '$_id',
				'realEstate':{
					'individual': '$realEstateIndividual',
					'joint': '$realEstateJoint',
					'total': '$realEstateTotal'
				},
				'movable':{
					'individual': '$movableIndividual',
					'joint': '$movableJoint',
					'total': '$movableTotal'
				},
				'shares':{
					'individual': '$sharesIndividual',
					'joint': '$sharesJoint',
					'total': '$sharesTotal'
				},
				'bonds':{
					'individual': '$bondsIndividual',
					'joint': '$bondsJoint',
					'total': '$bondsTotal'
				},
				'cash':{
					'individual': '$cashIndividual',
					'joint': '$cashJoint',
					'total': '$cashTotal'
				},
				'debtsOrOutstanding':{
					'individual': '$debtsOrOutstandingIndividual',
					'joint': '$debtsOrOutstandingJoint',
					'total': '$debtsOrOutstandingTotal'
				},
				'annualSalary':{
					'regular':{
						'individual': '$annualSalaryRegularIndividual',
						'joint': '$annualSalaryRegularJoint',
						'total': '$annualSalaryRegularTotal',
					},
					'honorariums':{
						'individual': '$annualSalaryHonorariumsIndividual',
						'joint': '$annualSalaryHonorariumsJoint',
						'total': '$annualSalaryHonorariumsTotal',
					},
					'totals':{
						'individual': '$totalIndividual',
						'joint': '$totalJoint',
						'total': '$total'
					}
				}
			}
		}

		return project
