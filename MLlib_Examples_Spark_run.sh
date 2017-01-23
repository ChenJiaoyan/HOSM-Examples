#! /bin/sh
git pull
mvn clean
mvn compile
mvn assembly:assembly -DdescriptorId=jar-with-dependencies

spark-submit --class org.heigit.hosm.example.HOSM_MLlib_Examples --master local[1] target/jiaoyan-example-1.0-SNAPSHOT-jar-with-dependencies.jar
