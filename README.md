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

- content of `lib/` should be copied to `/usr/local/lib/`
- content of `jni/` must be copied to `src/main/java/net/quasardb/qdb/jni/`

Step 2, build:

    mvn clean install

### Contact information

> **quasardb**
> 
> 24, rue Feydeau
> 75002 Paris, France
> 
> +33 (0)1 55 34 92 30
>
> +33 (0)1 55 34 92 39
