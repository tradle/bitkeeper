#!/bin/bash
mkdir -p bitkeeper
mkdir -p bitkeeper/conf
wget https://github.com/urbien/bitkeeper/raw/master/build/bitkeeper.jar -P bitkeeper
wget https://github.com/urbien/bitkeeper/raw/master/conf/config.json -P bitkeeper/conf