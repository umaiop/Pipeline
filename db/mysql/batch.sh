#!/bin/bash

./export_to_csv.sh --s "2013-09-30"

for i in {1..11}
do
	./export_to_csv.sh --s "2013-10-${i}"
done
