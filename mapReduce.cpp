#include <assert.h>
#include <iostream>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

#include "MapReduce.hpp"

typedef struct
{
    std::string &key;
    std::string &value;
} KVpair;

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
                       Mapper map_function, uint num_mappers,
                       Reducer reduce_function, uint num_reducers,
                       Partitioner partition_function)
{

    // Initialize partitions
    auto partitions = std::make_shared<std::vector<std::string>>();
}