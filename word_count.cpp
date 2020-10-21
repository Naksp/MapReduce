#include <assert.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string.h>
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
            map_reduce->MR_Emit(token, "1");
        }
    }
    fp.close();
}

void Reduce(const std::string &key, MapReduce::InIter it, MapReduce::InIter end)
{
    int count = 0;
    std::string value;

    for (;it != end; it++)
    {
        count += std::stoi(it->second);
    }

    std::cout << key << " " << count << std::endl;
}

int main(int argc, char* argv[])
{
    try {
        if (argc > 1)
        {
            uint num_mappers = 10;
            uint num_reducers = 10;
            uint file_start_index = 1;

            for (int i = 0; i < argc; i++)
            {
                if (strcmp(argv[i], "-m") == 0)
                {
                    num_mappers = std::stoi(argv[i+1]);
                    if (num_mappers <= 0)
                    {
                        throw std::invalid_argument("");
                    }
                    file_start_index = i + 2;
                    i++;
                    continue;
                }
                if (strcmp(argv[i], "-r") == 0)
                {
                    num_reducers = std::stoi(argv[i+1]);
                    if (num_reducers <= 0)
                    {
                        throw std::invalid_argument("");
                    }
                    file_start_index = i + 2;
                    i++;
                    continue;
                }
            }
            //char* files[argc-file_start_index];
            std::vector<char*> files(argv + file_start_index, argv + argc);
            //std::copy(argv + file_start_index, argv + argc, files);
            map_reduce = std::make_unique<MapReduce>(files, Map, num_mappers, Reduce, num_reducers, MapReduce::MR_DefaultHashPartition);
            map_reduce->MR_Run();
            
        }
        else
        {
            std::cerr << "Usage: ./word_count [-m][num mappers][-r][num reducers][FILE NAME]" << std::endl;
        }
    }
    catch (const std::invalid_argument& ia)
    {
        std::cerr << "Number of mappers and reducers must be an integer greater than 0";
    }

    return 0;
}