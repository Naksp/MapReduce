#ifndef MAP_REDUCE_HPP
#define MAP_REDUCE_HPP

typedef std::string (*Getter)(const std::string &key, int partition_number);

void MR_Emit(const std::string &key, const std::string &value);

#endif // MAP_REDUCE_HPP