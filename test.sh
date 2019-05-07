#!/bin/bash
##
# This is a helper script for development, do not use for CI or anything
##
QDB_API_VERSION="3.4.0-SNAPSHOT"

echo "Rebuilding JNI..."
rm -rf jni \
    && mkdir jni \
    && cd ../qdb-api-jni/build/ \
    && rm -rf ./* \
    && cmake -DQDB_API_VERSION=${QDB_API_VERSION} .. \
    && make -j32 \
    && cp ./jni* ../../qdb-api-java/jni/ \
    && cd ../../qdb-api-java

echo "Installing JNI"
mvn install:install-file -f pom-jni.xml -Dqdb.api.version="${QDB_API_VERSION}"
mvn install:install-file -f pom-jni-arch.xml -Dqdb.api.version="${QDB_API_VERSION}"  -Darch=linux-x86_64

echo "Running tests"
mvn \
    -Dqdbd.port=28360 \
    -Dqdbd.secure.port=28361 \
    -Dqdb.api.version="${QDB_API_VERSION}" \
    '-Dtest=WriterTest#canAsyncInsertDoubleRow*' \
    test
