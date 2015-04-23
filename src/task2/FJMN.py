class mapper:
	def setup():
		store top-neighourhoods in top-nei-map

	def map(String blank, String country/locality/count):
		emit(country, 
			locality:count top-nei-map.
							getMaxNeiCountPair(locality))

class reducer:
	def reducer(Text country, 
		Iterable<Text> locality:count neighb:count):
		locality_neighb_list = country
		for each values str
			init_str += str
		emit(country, locality_neighb_list)


		