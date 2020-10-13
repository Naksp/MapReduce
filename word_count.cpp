#include <assert.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>

#include "MapReduce.hpp"

std::unique_ptr<MapReduce> map_reduce;

void Map(const std::string &file_name)
{
    std::ifstream fp;
    fp.open(file_name);
    assert(fp);

    std::string line;
    while(std::getline(fp, line))
    {
        std::string token;
        std::istringstream iss(line);
        while (!iss.eof())
        {
            iss >> token;
            //std::cout << token << std::endl;
            map_reduce->MR_Emit(token, "1");
        }
    }
    fp.close();
}

void Reduce(const std::string &key, MapReduce::Getter get_next, int partition_number)
{
    int count = 0;
    std::string value;
    while (!(value = get_next(key, partition_number)).empty())
    {
        count++;
    }
    std::cout << key << " " << count << std::endl;
}

int main(int argc, char* argv[])
{
    if (argc > 1)
    {
        map_reduce = std::make_unique<MapReduce>(argc, argv, Map, 10, Reduce, 10, MapReduce::MR_DefaultHashPartition);
        map_reduce->MR_Run();
        
    }
    else
    {
        std::cout << "Usage: ./word_count [FILE NAME]" << std::endl;
    }

    return 0;
}