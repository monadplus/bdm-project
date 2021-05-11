.PHONY: run

all: run

run:
	python3 main.py

spark:
	spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 --master local[4] main.py
