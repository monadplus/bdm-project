.PHONY: compile run
all: compile run

compile:
	sbt package

run:
	spark-submit --class bdm.Main --master local[4] --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 target/scala-2.12/bdm_2.12-0.0.1.jar
