#include <algorithm>
#include <assert.h>
#include <iostream>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "MapReduce.hpp"

MapReduce::MapReduce(int argc, char *argv[], 
                     Mapper map_function, uint num_mappers,
                     Reducer reduce_function, uint num_reducers,
                     Partitioner partition_function) :
                     argc(argc), argv(argv),
                     map_function(map_function), num_mappers(num_mappers),
                     reduce_function(reduce_function), num_reducers(num_reducers),
                     partition_function(partition_function)
{
    curr_file = 1;
}

MapReduce::~MapReduce()
{
}

void MapReduce::partition_add(uint partition_num, KVpair pair)
{
    partition_lock.lock();
    auto it = partitions->begin() + partition_num;
    partitions->insert(it, pair);
    partition_lock.unlock();
}

std::string MapReduce::get_next_file_name()
{
    std::string file;
    map_lock.lock();
    if (curr_file == argc)
    {
        file = "";
    }
    else
    {
        file = argv[curr_file];
        curr_file++;
    }
    map_lock.unlock();
    
    return file;
}

void* MapReduce::map_thread_start(uint num)
{
    std::string file_name;
    while (!(file_name = get_next_file_name()).empty())
    {
        map_function(file_name);
    }

    return NULL;
}

void MapReduce::MR_Emit(const std::string &key, const std::string &value)
{
    //std::cout << key << " " << value << std::endl;
    uint partition_num = partition_function(key, num_reducers);
    //KVpair pair(key, value);
    partition_add(partition_num, std::make_pair(key, value));
}

unsigned long MapReduce::MR_DefaultHashPartition(const std::string &key, int num_partitions)
{
    std::hash<std::string> hasher;
    auto hashed = hasher(key);
    return hashed % num_partitions;
}

void MapReduce::MR_Run()
{

    // Initialize partitions
    partitions = std::make_shared<std::vector<KVpair>>();
    partitions->reserve(num_mappers);

    std::thread threads[num_mappers];

    // Start mappers
    for (uint i = 0; i < num_mappers; i++)
    {
        threads[i] = std::thread(&MapReduce::map_thread_start, this, i+1);
    }
    for (auto& th : threads)
    {
        th.join();
    }

    // Sort partitions
    std::sort (partitions->begin(), partitions->end());

}
