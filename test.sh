#!/bin/bash

set -x

##
# This is a helper script for development, do not use for CI or anything
##
QDB_API_VERSION="3.9.0"

echo "Rebuilding JNI..."
rm -rf jni \
    && mkdir jni \
    && cd ../qdb-api-jni/ \
    && mvn compile \
    && rm -rf build \
    && mkdir build  \
    && cd build \
    && cmake -G Ninja .. \
    && cmake --build . \
    && cd .. \
    && mvn install \
    && cp target/jni* ../qdb-api-java/jni/ \
    && cd ../qdb-api-java

echo "Installing JNI"
mvn install:install-file -f pom-jni.xml
mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64

echo "Running tests"
mvn \
    -Dqdbd.port=2836 \
    -Dqdbd.secure.port=2838 \
    test

#    '-Dtest=WriterTest*' \
