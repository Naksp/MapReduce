#include <assert.h>
#include <iostream>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "MapReduce.hpp"

MapReduce::MapReduce(Mapper map_function, uint num_mappers,
                     Reducer reduce_function, uint num_reducers,
                     Partitioner partition_function) :
                     map_function(map_function), num_mappers(num_mappers),
                     reduce_function(reduce_function), num_reducers(num_reducers),
                     partition_function(partition_function)
{

}

MapReduce::~MapReduce()
{
}

void map_thread_start(void* arg)
{
    
}

void MapReduce::MR_Emit(const std::string &key, const std::string &value)
{
    std::cout << key << " " << value << std::endl;
}

unsigned long MapReduce::MR_DefaultHashPartition(const std::string &key, int num_partitions)
{
    std::hash<std::string> hasher;
    auto hashed = hasher(key);
    return hashed % num_partitions;
}

void MapReduce::MR_Run(int argc, char *argv[],
                       Mapper map, uint num_map,
                       Reducer reduce, uint num_red,
                       Partitioner partition)
{

    // Initialize partitions
    partitions = std::make_shared<std::vector<std::string>>();
}