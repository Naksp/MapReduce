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
    partitions->at(partition_num).push_back(pair);
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

void* MapReduce::reduce_thread_start(uint partition_num)
{
    if (partitions->at(partition_num).empty())
    {
        return NULL;
    }

    thread_local InIter key_begin = partitions->at(partition_num).begin();
    thread_local InIter key_end = key_begin;

    thread_local std::vector<KVpair>::iterator it = partitions->at(partition_num).begin();
    thread_local std::string key = "";

    for (; true; it++)
    {
        // Set key if it's first in partition then iterate
        if (partitions->at(partition_num).begin() == it)
        {
            key = it->first;
            continue;
        }
        // Iterate until next key is found
        if (key == it->first)
        {
            continue;
        }
        else
        {
            key_end = it;
        }
        // Call reduce_function for range of key
        reduce_lock.lock();
        reduce_function(key, key_begin, key_end, partition_num);
        reduce_lock.unlock();
        // Set up for next key
        if (it == partitions->at(partition_num).end())
        {
            break;
        }
        key_begin = it;
        key = it->first;
    }
    return NULL;
}

void MapReduce::MR_Emit(const std::string &key, const std::string &value)
{
    uint partition_num = partition_function(key, num_reducers);
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
    partitions = std::make_shared<std::vector<std::vector<KVpair>>>();
    for (uint i = 0; i < num_reducers; i++)
    {
        partitions->push_back(std::vector<KVpair>());
    }


    // Start mappers
    std::thread map_threads[num_mappers];
    for (uint i = 0; i < num_mappers; i++)
    {
        map_threads[i] = std::thread(&MapReduce::map_thread_start, this, i+1);
    }
    for (auto& th : map_threads)
    {
        th.join();
    }

    // Sort partitions
    for (auto& partition : *partitions)
    {
        std::sort (partition.begin(), partition.end());
    }

    int num = 0;
    for (auto& p1 : *partitions)
    {
        std::cout << "Partition " << num << ":" << std::endl;
        for (auto& p2 : p1)
        {
            std::cout << "\t" << p2.first << ", " << p2.second << std::endl;
        }
        num++;
    }
    

    // Start reducers
    std::thread reduce_threads[num_reducers];
    for (uint i = 0; i < num_reducers; i++)
    {
        reduce_threads[i] = std::thread(&MapReduce::reduce_thread_start, this, i);
    }
    for (auto& th : reduce_threads)
    {
        th.join();
    }
}
