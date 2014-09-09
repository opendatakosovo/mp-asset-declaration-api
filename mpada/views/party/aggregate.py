from flask import Flask, Response
from flask.views import View
import flask_pymongo
from bson import json_util
from bson.son import SON

from mpada import mongo

class PartyAggregate(View):

	methods = ['GET']

	def dispatch_request(self, party_slug):
		''' Get the asset declarations for members of a given Party.
		:param party_slug: slug value of the Party.
		'''

		# Query.
		query = { "$match": {
			"party.slug": party_slug
			}
		} 

		# Group.
		group = {
			'$group':{
				'_id':'$year',

				# Total real estate assets.
				'realEstateIndividual':{
					'$sum':'$realEstate.individual'
				},
				'realEstateJoint':{
					'$sum':'$realEstate.joint'
				},

				# Total movable assets.
				'movableIndividual':{
					'$sum':'$movable.individual'
				},
				'movableJoint':{
					'$sum':'$movable.joint'
				},

				# Total individual assets.
				'sharesIndividual':{
					'$sum':'$shares.individual'
				},
				'sharesJoint':{
					'$sum':'$shares.joint'
				},

				# Total bonds assets.
				'bondsIndividual':{
					'$sum':'$bonds.individual'
				},
				'bondsJoint':{
					'$sum':'$bonds.joint'
				},

				# Total cash assets.
				'cashIndividual':{
					'$sum':'$cash.individual'
				},
				'cashJoint':{
					'$sum':'$cash.joint'
				},

				# Total debs or outstanding assets.
				'debtsOrOutstandingIndividual':{
					'$sum':'$debtsOrOutstanding.individual'
				},
				'debtsOrOutstandingJoint':{
					'$sum':'$debtsOrOutstanding.joint'
				},

				# Total regular annual salary assets.
				'annualSalaryRegularIndividual':{
					'$sum':'$annualSalary.regular.individual'
				},
				'annualSalaryRegularJoint':{
					'$sum':'$annualSalary.regular.joint'
				},

				# Total honorarium annual salary assets.
				'annualSalaryHonorariumsIndividual':{
					'$sum':'$annualSalary.honorariums.individual'
				},
				'annualSalaryHonorariumsJoint':{
					'$sum':'$annualSalary.honorariums.joint'
				},
			}
		}

		# Sort.
		sort = { 
			'$sort': SON([
				('_id', flask_pymongo.DESCENDING)
			])
		}

		# Projection.
		project = {
			'$project':{
				'_id': 0,
				'realEstateIndividual': 0,
				'realEstateJoint': 0,
				'year': '$_id',
				'realEstate':{
					'individual': '$realEstateIndividual',
					'joint': '$realEstateJoint'
				},
				'movable':{
					'individual': '$movableIndividual',
					'joint': '$movableJoint'
				}		
			}
		}

		# Execute query.
		declarations = mongo.db.mpassetdeclarations.aggregate([
			query,
			group,
			sort
		])

		# Build response object.			
		resp = Response(response=json_util.dumps(declarations), mimetype='application/json')

		# Return response.
		return resp
