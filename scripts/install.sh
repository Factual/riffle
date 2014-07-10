#!/bin/sh

if [ -z $1 ]
then
    echo "Must provide directory where 'riffle' script gets copied to."
else
    mkdir -p ~/.riffle
    lein do clean, install, uberjar
    cp target/*standalone.jar ~/.riffle/riffle.jar
    cd riffle-hadoop
    lein do clean, uberjar
    cp target/*standalone.jar ~/.riffle/riffle-hadoop.jar
    cd ..
    cp scripts/riffle $1
fi
