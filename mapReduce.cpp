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

MapReduce::MapReduce(std::vector<char*> files, 
                     Mapper map_function, uint num_mappers,
                     Reducer reduce_function, uint num_reducers,
                     Partitioner partition_function) :
                     file_names(files),
                     map_function(map_function), num_mappers(num_mappers),
                     reduce_function(reduce_function), num_reducers(num_reducers),
                     partition_function(partition_function)
{
    curr_file = 0;
}

MapReduce::~MapReduce()
{
}

void MapReduce::partition_add(uint partition_num, KVpair pair)
{
    std::lock_guard<std::mutex> lk(partition_lock);
    partitions->at(partition_num).push_back(pair);
}

std::string MapReduce::get_next_file_name()
{
    std::string file;
    std::lock_guard<std::mutex> lk(map_lock);
    if (curr_file == file_names.size())
    {
        file = "";
    }
    else
    {
        file = file_names.at(curr_file);
        curr_file++;
    }
    
    return file;
}

void* MapReduce::map_thread_start()
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
        if (it != partitions->at(partition_num).end() && key == it->first)
        {
            continue;
        }
        else
        {
            key_end = it;
        }
        // Call reduce_function for range of key
        call_reduce_function(key, key_begin, key_end);
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

void MapReduce::call_reduce_function(std::string key, InIter begin , InIter end)
{
        std::lock_guard<std::mutex> lk(reduce_lock);
        reduce_function(key, begin, end);
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
    partitions = std::make_unique<std::vector<std::vector<KVpair>>>();
    for (uint i = 0; i < num_reducers; i++)
    {
        partitions->push_back(std::vector<KVpair>());
    }


    // Start mappers
    std::thread map_threads[num_mappers];
    for (uint i = 0; i < num_mappers; i++)
    {
        map_threads[i] = std::thread(&MapReduce::map_thread_start, this);
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
