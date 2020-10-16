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

        /**
         * \typedef Mapper
         * \brief Pointer to caller map function.
         */
        typedef void (*Mapper)(const std::string &file_name);

        /**
         * \typedef Reducer
         * \brief Pointer to caller reduce function.
         */
        typedef void (*Reducer)(const std::string &key, const InIter begin, const InIter end);

        /**
         * \typedef Partitioner
         * \brief Pointer to partitioning function.
         * 
         *  If no partitioning function is implemented by the caller, MR_DefaultHashPartition() should be used
         * \see MR_DefaultHashPartition
         */
        typedef unsigned long (*Partitioner)(const std::string &key, int num_partitions);


        /**
         * MapReduce Constructor
         * 
         * \param argc  argc from caller
         * \param argv  argv from caller
         * \param map_function  Mapper function to be called by mappper threads
         * \param num_mappers   number of mapper threads to create
         * \param reduce_function   Reduce function to be called by reduce threads
         * \param num_reducers      number of reducer functions to create
         * \param partition_function    Partitioner function to be called
         * 
         */
        MapReduce(int argc, char *argv[], Mapper map_function, uint num_mappers,
                  Reducer reduce_function, uint num_reducers,
                  Partitioner partition_function);
        /** 
         * MapReduce Destructor
         */
        ~MapReduce();

        /**
         * \brief Emit function to be called from caller Reduce function.
         * Stores key and value into KVpair and adds to partition
         * 
         * \param key Key to be added
         * \param value Value to be added
         */
        void MR_Emit(const std::string &key, const std::string &value);

        /**
         * \brief Default partitioning function for MapReduce.
         * Determines partition number with std::hash
         * 
         * \param key Key to be added to parition
         * \param num_partitions Total number of partitions in data partitions vector
         */
        static unsigned long MR_DefaultHashPartition(const std::string &key, int num_partitions);

        /**
         * \brief Starts the MapReduce process.
         * Runs mapper threads, sorts the partitions, then runs reducer threads.
         */
        void MR_Run();
    
    private:
        /**
         * Gets name of next file to be read from argv
         * \return name of next file to read
         */
        std::string get_next_file_name();

        /**
         * \brief Starting function for mapper threads.
         * Calls the reduce function given by caller
         */
        void* map_thread_start();

        /**
         * \brief Starting function for reducer threads. 
         * Calls the reduce function given by caller.
         * 
         * \param parition_num Partition for reducer to work on
         */
        void* reduce_thread_start(uint partition_num);


        /**
         * Thread-safe insert to partition vector
         * \param partition_num Partition number given from partition_function
         * \param pair KVpair to add to partition
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

        /**
         * Locks
         */
        std::mutex partition_lock, map_lock, reduce_lock;

        /**
         * Main data structure
         */
        std::shared_ptr<std::vector<std::vector<KVpair>>> partitions;

};

#endif // MAP_REDUCE_HPP