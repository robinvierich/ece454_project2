#!/bin/bash

for i in {1..6}
do
    echo "Removing dfs files and DB data in $i"
    #rm "peer$i/test.db"
    rm -f peer$i/*.db
    rm -f peer$i/dfs/*
done
