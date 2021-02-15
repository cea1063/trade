from flask_restful import Resource

class Root(Resource):
	def get(self):
		return 'GCP Flask'
