class mapper:
	def map(String blank, 
		String locality/neighbourhood/count):
	emit(locality, neighbourhood/count)

	class reducer:
		def reducer(Text locality, 
			Iterable<Text> values):
		pair neighbourhood-count = 
		max(values.pair by count)
		emit(locality, neighbourhood/count)
