#!/bin/sh

javac -cp jars/spark-core_2.13-4.0.0.jar:\
jars/spark-common-utils_2.13-4.0.0.jar:\
jars/spark-sql-api_2.13-4.0.0.jar:\
jars/scala-library-2.13.16.jar:\
google-collections-1.0.jar\
 $1.java
jar -cvf $1.jar $1*.class
