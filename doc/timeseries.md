
# Timeseries API

This document will show several examples on how to interact with timeseries using the QuasarDB Java API. For the complete documentation, please refer to the [JavaDoc](https://doc.quasardb.net/java/)

## Batch inserting data

QuasarDB timeseries supports batch inserts to load data as fast as possible. It works by maintaining a local cache of the data to be inserted, and periodically flushing this cache.

### Acquiring a table reference

Assuming you have a timeseries table called 'my_series', the following shows how to acquire a reference to that table:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.getTimeSeries("my_series");
QdbTimeSeriesTable table = series.table();

...

// Do not forget to flush after you are done.
table.flush();
```

### Flushing

The table object will be your main entry point to other timeseries operations. It keeps a local cache of the objects to be written, and needs to be periodically flushed using the `flush()` member function. To make this easier for you, we provide built-in support for auto-flushing as follows:


```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("my_series");
QdbTimeSeriesTable table = series.autoFlushTable();

...
// Table is automatically flushed every 50000 rows by default
...

// Do not forget to flush after you are done.
table.flush();```

### Inserting rows data

Use the `append` member function to append new rows to the local table cache. Assuming you have a table with a double and a blob column, the following example shows how to insert a new row:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("my_series");
QdbTimeSeriesTable table = series.autoFlushTable();

Double myDouble = ...;
ByteBuffer myBlob = ...;

QdbTimeSeriesValue[] values = {
  QdbTimeSeriesValue.createDouble(myDouble),
  QdbTimeSeriesValue.createBlob(myBlob) 
  };

table.append(new QdbTimeSeriesRow (LocalDateTime.now(), values));
table.flush();
```

### Safe blobs

In  order to maximize performance, QuasarDB tries to keep memory copying to a minimum and `createBlob` does not copy blob data before putting it into the local cache using `append`. This means that when short-lived objects that get garbage collectred before they are added to the local cache will lead to errors. As a solution to this, we provide a SafeBlob class that copies the data into a new, temporary ByteBuffer automatically:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("my_series");
QdbTimeSeriesTable table = series.autoFlushTable();

QdbTimeSeriesValue[] values = {
  QdbTimeSeriesValue.createDouble(1.23),
  QdbTimeSeriesValue.createSafeBlob(ByteBuffer.allocateDirect(...)) 
  };

table.append(new QdbTimeSeriesRow (LocalDateTime.now(), values));
table.flush();```

While SafeBlob should be avoided for performance reasons, the code above is safe to use, even when the short-lived ByteBuffer gets garbage collected.

### Closing 

As a timeseries table keeps a local cache of all data that needs to be loaded, it can consume a large amount of memory. You are strongly encouraged to use the `close` function to flush the table and release any acquired memory:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("my_series");
QdbTimeSeriesTable table = series.autoFlushTable();

// load data

table.close(); // automatically flushes as well
```

## Querying double data

The `getDoubles` function is available on a `QdbTimeSeries` object which can be used to retrieve double data as a `QdbDoubleColumnCollection`. The following example shows how to query data from the past hour:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("doubles_test");

QdbTimeRangeCollection timeRanges = new QdbTimeRangeCollection();
timeRanges.add(new QdbTimeRange(new QdbTimespec(Timestamp.from(Instant.now())),
                                new QdbTimespec(Timestamp.from(Instant.now().minusSeconds(3600)))));

QdbDoubleColumnCollection results = series.getDoubles("value", timeRanges);
```

## Aggregating double data

The `doubleAggregate` function is available on a `QdbTimeSeries` object which can be used to aggregate double data as a `QdbDoubleAggregationCollection`. The following example shows how to count the amount of double entries in the past hour:

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("doubles_test");

QdbDoubleAggregationCollection aggregations = new QdbDoubleAggregationCollection();
aggregations.add(new QdbDoubleAggregation(QdbAggregation.Type.COUNT,
                                          new QdbTimeRange(new QdbTimespec(Timestamp.from(Instant.now())),
                                                           new QdbTimespec(Timestamp.from(Instant.now().minusSeconds(3600))))));

QdbDoubleAggregationCollection results = series.doubleAggregate("value", aggregations);
assert(results.size() == 1);

System.out.println("count: " + results.get(0).getCount());
```
