class mapper:
	def setup():
		load places.txt file into lookup-map

	def map(String placeId, String record):
		get data from lookup-map with placeId
		if record is a neighbhourhood or locality
			emit(country/locality/user, 1)

class cominber as IntSumReducer

class reducer:
	hashtable table
	def reducer(String country/locality/user, Iterable<IntWritable> values):
		table.put(country-locality, current-value+1)

	def cleanup():
		for entry in country-locality
			emit(entry.key, entry.value)

