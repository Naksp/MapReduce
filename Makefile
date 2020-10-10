CC = g++
CXXFLAGS = -Wall -Werror -pthread -o wordcount

all: mapReduce.cpp MapReduce.hpp word_count.cpp
	$(CC) *.cpp $(CXXFLAGS)

debug: mapReduce.cpp MapReduce.hpp word_count.cpp
	$(CC) *.cpp -g $(CXXFLAGS)

clean:
	rm -rf wordcount
	rm -rf *.out