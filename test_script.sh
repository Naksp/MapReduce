#!/bin/bash
TIMEFORMAT=%3R;
FILE="file10m.in file100m.in"
THREADS="5 10 50 100 200"

echo "Starting Test Script";
echo "Running test with 1 thread...";
base_time=$( { time ./wordcount -m 1 -r 1 $FILE 1>&3 2>&4; } 2>&1 )
echo "$base_time"s;
echo "";

for i in $THREADS
do
    echo "Running test with $i threads...";
    thread_time=$( { time ./wordcount -m $i -r $i $FILE 1>&3 2>&4; } 2>&1 )
    echo "$thread_time"s;
    PCT=$(echo "scale=3; $thread_time / $base_time" | bc);
    PCT=$(echo "scale=2; (1 - $PCT) * 100" | bc);
    echo "$PCT"% Improvement;
    echo "";
done