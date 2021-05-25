.PHONY: compile run
all: compile run

compile:
	sbt stage

run:
	spark-submit --class bdm.Main --master local[4]
