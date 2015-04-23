class mapper:
	def map(String blank, String country/locality/count):
		emit(count, country/locality)

class reducer:
	def reducer(IntWritable count, 
		Iterable<IntWritable> country/locality):
		for each values str
			if locations.get(str.country) < 10
				increment locations.get(str.country)
				emit(str.country/str.locality, count)