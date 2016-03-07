Quasardb Java API
=================

Java API for [quasardb](http://www.quasardb.net/).

### Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
2. [quasardb JNI API](https://download.quasardb.net/quasardb/)
3. Gradle
4. JDK 6 or above

### Build instructions:

Step 1, unzip JNI package:

- extract quasardb daemon archive to `qdb/`
- extract quasardb JNI API archive to `qdb/`

Step 2, build:

    gradle build -Pqdbd.path=$PWD/qdb/qdbd

or, on Windows:

    gradle build -Pqdbd.path=%CD%/qdb/qdbd.exe

### Troubleshooting

If you encounter strange build or test errors, try to clean the directory:

    gradle clean -Pqdbd.path=

You may verify as well that the given paths are absolute.
On Windows, do not use symbolic links for `qdb_api.dll`, otherwise all the tests will fail.

### Contact information

> **quasardb**
>
> 24, rue Feydeau
> 75002 Paris, France
>
> +33 (0)1 55 34 92 30
>
> +33 (0)1 55 34 92 39
