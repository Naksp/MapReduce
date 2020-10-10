#ifndef MAP_REDUCE_HPP
#define MAP_REDUCE_HPP

namespace MapReduce
{
    typedef std::string (*Getter)(const std::string &key, int partition_number);
    typedef void (*Mapper)(const std::string &file_name);
    typedef void (*Reducer)(const std::string &key, Getter get_func, int partition_number);
    typedef unsigned long (*Partitioner)(const std::string &key, int num_partitions);

    void MR_Emit(const std::string &key, const std::string &value);

    unsigned long MR_DefaultHashPartition(const std::string &key, int num_partitions);

    void MR_Run(int argc, char *argv[],
                Mapper map, uint num_mappers,
                Reducer reduce, uint num_reducers,
                Partitioner partition);
};

#endif // MAP_REDUCE_HPP