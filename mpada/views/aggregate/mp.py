from flask import Flask, Response
from flask.views import View
import flask_pymongo
from bson import json_util
from bson.son import SON

from mpada import mongo
from declaration import DeclarationAggregate

class MPAggregate(DeclarationAggregate):

	def dispatch_request(self, party_slug, mp_name_slug):
		''' Get the asset declaration of an MP for the given Party and MP slugs.
		:param party_slug: slug value of the Party.
		:param party_slug: slug value of the MP.
		'''

		# Query.
		query = self.get_query(party_slug, mp_name_slug)

		# Group.
		group = self.get_group()

		# Sort.
		sort = self.get_sort()

		# Projection.
		project = self.get_projection()

		# Execute query.
		declarations = mongo.db.mpassetdeclarations.aggregate([
			query,
			group,
			sort,
			project
		])

		# Build response object.			
		resp = Response(response=json_util.dumps(declarations), mimetype='application/json')

		# Return response.
		return resp


	def get_query(self, party_slug, mp_name_slug):
		''' Build and return the query object to be used in aggregation pipeline.
		:param party_slug: name slug of a party.
		:param mp_name_slug: name slug of an MP.
		'''
		query = { "$match": {
			"party.slug": party_slug,
			'mp.slug': mp_name_slug
			}
		} 

		return query