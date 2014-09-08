from flask import Flask, Response
from flask.views import View
import flask_pymongo
from bson import json_util

from mpada import mongo

class Party(View):

	methods = ['GET']

	def dispatch_request(self, party_slug):
		''' Get the asset declarations for members of a given Party.
		:param party_slug: slug value of the Party.
		'''
		query = {'party.slug': party_slug}

		declarations = mongo.db.mpassetdeclarations.find(query).sort([
			("year", flask_pymongo.DESCENDING),
			("mp.slug", flask_pymongo.ASCENDING)
		])

		# Build response object				
		resp = Response(response=json_util.dumps(declarations), mimetype='application/json')

		return resp
