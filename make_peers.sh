#!/bin/bash

for i in {1..6}
do
    echo "Creating dir $i and copying files"
    mkdir -p peers$i
    cp *.py peer$i
done

