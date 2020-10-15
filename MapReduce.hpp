#ifndef MAP_REDUCE_HPP
#define MAP_REDUCE_HPP

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

class MapReduce
{
    public:
        using KVpair = std::pair<std::string /*key*/, std::string /*value*/>;

        using InIter = std::vector<KVpair>::const_iterator;

        typedef void (*Mapper)(const std::string &file_name);
        typedef void (*Reducer)(const std::string &key, const InIter begin, const InIter end);
        typedef unsigned long (*Partitioner)(const std::string &key, int num_partitions);


        /**
         * MapReduce Constructor
         * 
         * @param argc  argc from caller
         * @param argv  argv from caller
         */
        MapReduce(int argc, char *argv[], Mapper map_function, uint num_mappers,
                  Reducer reduce_function, uint num_reducers,
                  Partitioner partition_function);
        /** 
         * MapReduce Destructor
         */
        ~MapReduce();

        void MR_Emit(const std::string &key, const std::string &value);

        static unsigned long MR_DefaultHashPartition(const std::string &key, int num_partitions);

        void MR_Run();
    
    private:
        /**
         * Gets name of next file to be read from argv
         * @return name of next file to read
         */
        std::string get_next_file_name();

        /**
         * Starting function for mapper threads
         */
        void* map_thread_start(uint num);

        /**
         * Starting function for reducer threads. 
         * Calls the reduce function given from caller.
         * 
         * @param parition_num Partition for reducer to work on
         */
        void* reduce_thread_start(uint partition_num);


        /**
         * Thread-safe insert to partition vector
         * @param partition_num Partition number given from partition_function
         * @param pair KVpair to add to partition
         */
        void partition_add(uint partition_num, const KVpair pair);


    private:
        uint argc; // argc from caller
        char **argv; // argv from caller. argv[1-n] are file names.
        Mapper map_function;
        uint num_mappers;   // Number of mapper threads to create
        Reducer reduce_function;
        uint num_reducers;  // Number of reducer threads to create
        Partitioner partition_function;

        uint curr_file;

        std::mutex partition_lock, map_lock, reduce_lock;
        std::shared_ptr<std::vector<std::vector<KVpair>>> partitions;

};

#endif // MAP_REDUCE_HPP