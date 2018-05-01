Quasardb Java API
=================

Java API for [quasardb](https://www.quasardb.net/).

See documentation at [doc.quasardb.net](https://doc.quasardb.net/master/api/java.html)

### Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
2. [quasardb JNI API](https://download.quasardb.net/quasardb/)
3. Gradle
4. JDK 6 or above

### Build instructions:

Step 1, unzip JNI package:

- extract quasardb daemon archive to `qdb/`
- copy net.quasardb.qdb.jni jar files to `qdb/`

Step 2, launch qdbd:

If you're planning to run the tests, you need to launch two qdbd processes,
one secure and one normal instance:

 - qdbd -a 127.0.0.1:2836 --security=false --root qdb/insecure
 - qdbd -a 127.0.0.1:2837 --cluster-private-file=$PWD/cluster-secret-key.txt --user-list $PWD/users.txt --root qdb/secure

Step 3, build:

    gradle build -Pqdbd.port=2836 -Pqdbd.secure.port=2837

or, on Windows:

    gradle build -Pqdbd.path=%CD%/qdb/qdbd.exe

### Troubleshooting

If you encounter strange build or test errors, try to clean the directory:

    gradle clean -Pqdbd.path=

You may verify as well that the given paths are absolute.
On Windows, do not use symbolic links for `qdb_api.dll`, otherwise all the tests will fail.
