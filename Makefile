CC = g++
CXXFLAGS = -Wall -Werror -pthread -o wordcount

all: mapReduce.cpp MapReduce.hpp word_count.cpp
	$(CC) *.cpp -g $(CXXFLAGS)

debug: mapReduce.cpp MapReduce.hpp word_count.cpp
	$(CC) *.cpp -g $(CXXFLAGS)

test: all
	./wordcount test.txt test2.txt test3.txt

clean:
	rm -rf wordcount
	rm -rf *.out