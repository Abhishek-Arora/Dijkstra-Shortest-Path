from pyspark import SparkConf, SparkContext
import sys, operator
import json
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def min_distance((source1, dist1), (source2, dist2)):
	if dist1 <= dist2:
		return (source1, dist1)
	
	else: 
		return (source2, dist2)

def main():

	inputs = sys.argv[1]
	output = sys.argv[2]
	sourceNode = int(sys.argv[3])
	destinationNode = int(sys.argv[4])
	
	conf = SparkConf().setAppName('Shortest Path')
	sc = SparkContext(conf=conf)
	
	inputForTheProgram = sc.textFile(inputs)
	splittedInput = inputForTheProgram.map(lambda input : input.split(':')).map(lambda (node, edges) : (int(node), map(int, edges.split())))
	edges = splittedInput.flatMapValues(lambda value : value)
	#print edges.collect()
	
	knownPath = sc.parallelize([(1, ('',0))])
	
	for i in range(6):
	
		joinEdgesWithKnownPath = edges.join(knownPath.filter(lambda (node,(source, dist)) : dist==i))
		intermediatePath = joinEdgesWithKnownPath.map(lambda (node, (dest, (source, dist))) : (dest, (node, dist+1)))
		knownPath = knownPath.union(intermediatePath).reduceByKey(min_distance)
		print knownPath.collect()	
		knownPath.saveAsTextFile(output + '/iter-' + str(i))
		
	finalPath = [int(destinationNode)]
		
	while destinationNode!= sourceNode and destinationNode != '':
		print destinationNode
		lookUpValue = knownPath.lookup(destinationNode)
		print lookUpValue
		
		
		if lookUpValue[0][0] =='':
			break
		
		else:
			destinationNode = lookUpValue[0][0]
			finalPath.append(destinationNode)
		
	
	print finalPath
	finalPath = finalPath[::-1]
	finalPath = sc.parallelize(finalPath)
	finalPath.saveAsTextFile(output + '/path')
	
if __name__ == "__main__":
	main()
		
	
	