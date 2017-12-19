Quasardb Java API
=================

Java API for [quasardb](https://www.quasardb.net/).

See documentation at [doc.quasardb.net](https://doc.quasardb.net/latest/api/java.html)

### Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
2. [quasardb JNI API](https://download.quasardb.net/quasardb/)
3. Gradle
4. JDK 6 or above

### Build instructions:

Step 1, unzip JNI package:

- extract quasardb daemon archive to `qdb/`
- copy net.quasardb.qdb.jni jar files to `qdb/`

Step 2, build:

    gradle build -Pqdbd.path=$PWD/qdb/qdbd

or, on Windows:

    gradle build -Pqdbd.path=%CD%/qdb/qdbd.exe

### Troubleshooting

If you encounter strange build or test errors, try to clean the directory:

    gradle clean -Pqdbd.path=

You may verify as well that the given paths are absolute.
On Windows, do not use symbolic links for `qdb_api.dll`, otherwise all the tests will fail.
