#!/bin/bash
##
# This is a helper script for development, do not use for CI or anything
##

echo "Rebuilding JNI..."
cd ../qdb-api-jni/build/ && rm -rf ./* && cmake .. && make -j4 && cp ./jni* ../../qdb-api-java/qdb && cp ./libqdb_api_jni.so ../../qdb-api-java/qdb && cd ../../qdb-api-java

echo "Running tests"
./gradlew test -Pqdbd.port=2836 -Pqdbd.secure.port=2837 --info
