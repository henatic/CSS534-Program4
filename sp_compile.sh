#!/bin/sh

javac -Xlint -cp \
jars/spark-core_2.13-4.0.0.jar:\
jars/spark-common-utils_2.13-4.0.0.jar:\
jars/spark-sql_2.13-4.0.0.jar:\
jars/scala-library-2.13.16.jar:\
google-collections-1.0.jar:.\
 ShortestPath.java Data.java
jar -cvf ShortestPath.jar ShortestPath.class Data.class
