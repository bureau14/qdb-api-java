Quasardb Java API
=================

Java API for [quasardb](https://www.quasardb.net/).

See documentation at [doc.quasardb.net](https://doc.quasardb.net/master/api/java.html)

## Requirements

1. [quasardb daemon](https://download.quasardb.net/quasardb/)
2. JDK 8 or higher

## Adding QuasarDB to your build

QuasarDB's Maven group ID is `net.quasardb` and the artifact ID is `qdb`.

### Maven

To add a dependency on the QuasarDB Java API using Maven, use the following:

```xml
<dependency>
  <groupId>net.quasardb</groupId>
  <artifactId>qdb</artifactId>
  <version>3.5.0-rc2</version>
</dependency>
```

### Gradle

To add a dependency using Gradle:

```
dependencies {
  compile "net.quasardb.qdb:3.5.0-rc2"
}
```

### Snapshot releases

We continuously release snapshot releases on Sonatype's OSS repository. To gain access to it, please add Sonatype's snapshot repository to your build profile. For Maven, this can be achieved by adding the following profile to your `settings.xml`:

```xml
<profile>
  <id>allow-snapshots</id>
    <activation><activeByDefault>true</activeByDefault></activation>
    <repositories>
    <repository>
      <id>snapshots-repo</id>
      <url>https://oss.sonatype.org/content/repositories/snapshots</url>
      <releases><enabled>false</enabled></releases>
      <snapshots><enabled>true</enabled></snapshots>
    </repository>
  </repositories>
</profile>
```

Please adjust your artifact version accordingly by appending the appropriate `-SNAPSHOT` qualifier as documented at [the official Maven documentation](https://docs.oracle.com/middleware/1212/core/MAVEN/maven_version.htm#MAVEN401).

### Documentation

For more information on how to use this API, please refer to the [official documentation](http://doc.quasardb.net/master/api/java.html) at doc.quasardb.net.
