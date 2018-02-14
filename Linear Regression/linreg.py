# Author: Rekhansh Panchal
# Cloud Computing for Data Analysis (ITCS 6190)
# Assignment: Implementation of Linear Regression on PySpark.

# Standalone Python/Spark program to perform linear regression.
# Performs linear regression by computing the summation form of the
# closed form expression for the ordinary least squares estimate of beta.
# 
# Takes the yx file as input, where on each line y is the first element 
# and the remaining elements constitute the x.
#
# Usage: spark-submit linreg.py <inputdatafile> <outputfile>
# Example usage: spark-submit linreg.py yxlin.csv yxlin.out


import sys
import numpy as np

from pyspark import SparkContext

# The program should be run with three parameters. If all Re are not available, one should exit.
# Parameters: 1.Program Name 2.Input Data File 3.Output File

if __name__ == "__main__":
  if len(sys.argv) !=3:
    print >>"Usage: linreg <datafile> <outputfile>"
    exit(-1)

  sc = SparkContext(appName="LinearRegression")

  # Input yx file has y_i as the first element of each line 
  # And the remaining elements constitute x_i
  yxinputFile = sc.textFile(sys.argv[1])

  yxlines = yxinputFile.map(lambda line: line.split(','))
  yxfirstline = yxlines.first()
  yxlength = len(yxfirstline)
  print("Number of splits for " + str(sys.argv[1]) + " is : " + str(yxlength))

  # Function to map values of A and b for each line.
  def mapAb(line):

  	# Get values of y for the line.
    y = np.matrix([float(line[0])])
    
    # Creating x as a combination of 1 and current x value.
    x = np.matrix([1.0]+[float(val) for val in line[1:]])

    return [('A', x.T * x),('b', x.T * y)]
	
  def calcBeta(ab):

    A = ab[0][1]  
    b = ab[1][1]

	# Returning beta values.
    return A.I * b

  # Map the function into a flat map.
  AinverseB_Mapper = yxlines.flatMap(mapAb)

  # Reduce the function by both the keys and add values.
  AinverseB_Reducer = AinverseB_Mapper.reduceByKey(lambda out, vals: out + vals)
  
  # Fetch the values using collect function.
  AinverseB_Values = AinverseB_Reducer.collect()
  
  # Calculate beta for given input.
  beta = calcBeta(AinverseB_Values)
	
  # Printing the beta values.
  print "Beta values are: "
  for coeff in beta:
	print coeff

  # Save the beta values in HDFS output file.
  result = sc.parallelize(beta)
  out_path = sys.argv[2]
  result.coalesce(1).saveAsTextFile(out_path)
  sc.stop()
