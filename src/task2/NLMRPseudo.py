class mapper:
	def setup():
		load places.txt file into lookup-map

	def map(String placeId, String record):
		get data from lookup-map with placeId
		if record is a neighbourhood
			emit(locality/neighbourhood/user,1)

class cominber as IntSumReducer

class reducer:
	hashtable locations
	def reducer(String locality/neighbourhood/user, Iterable<IntWritable> values):
		table.put(locality-neighbourhood, current-value+1)

	def cleanup():
		for entry in locations
			emit(entry.key, entry.value)

