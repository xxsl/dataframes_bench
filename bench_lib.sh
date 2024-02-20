#!/bin/sh -e

lib_name="$1"
output_dir="$lib_name.result"

mkdir -p "$output_dir"

python3 bench.py $lib_name t1.csv "$output_dir/t1.json"
python3 bench.py $lib_name t2.csv "$output_dir/t2.json"
python3 bench.py $lib_name t3.csv "$output_dir/t3.json"
python3 bench.py $lib_name t4.csv "$output_dir/t4.json"
python3 bench.py $lib_name t5.csv "$output_dir/t5.json"
