#ifndef MAP_REDUCE_HPP
#define MAP_REDUCE_HPP

#include <memory>
#include <vector>

class MapReduce
{
    public:
        typedef std::string (*Getter)(const std::string &key, int partition_number);
        typedef void (*Mapper)(const std::string &file_name);
        typedef void (*Reducer)(const std::string &key, Getter get_func, int partition_number);
        typedef unsigned long (*Partitioner)(const std::string &key, int num_partitions);

        typedef struct { std::string &key, &value; } KVpair;

        /**
         * MapReduce Constructor
         */
        MapReduce(Mapper map_function, uint num_mappers,
                            Reducer reduce_function, uint num_reducers,
                            Partitioner partition_function);
        /** 
         * MapReduce Destructor
         */
        ~MapReduce();


        void MR_Emit(const std::string &key, const std::string &value);

        static unsigned long MR_DefaultHashPartition(const std::string &key, int num_partitions);

        void MR_Run(int argc, char *argv[],
                    Mapper map, uint num_mappers,
                    Reducer reduce, uint num_reducers,
                    Partitioner partition);

    private:
        Mapper map_function;
        uint num_mappers;
        Reducer reduce_function;
        uint num_reducers;
        Partitioner partition_function;

        std::shared_ptr<std::vector<std::string>> partitions;

};

#endif // MAP_REDUCE_HPP