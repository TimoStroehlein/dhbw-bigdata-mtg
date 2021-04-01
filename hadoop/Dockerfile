FROM marcelmittelstaedt/spark_base:latest

#USER hadoop
WORKDIR /home/hadoop

RUN wget http://www.congiu.net/hive-json-serde/1.3.8/hdp23/json-serde-1.3.8-jar-with-dependencies.jar
RUN wget http://www.congiu.net/hive-json-serde/1.3.8/hdp23/json-udf-1.3.8-jar-with-dependencies.jar
RUN mv json-serde-1.3.8-jar-with-dependencies.jar hive/lib
RUN mv json-udf-1.3.8-jar-with-dependencies.jar hive/lib
