
# Timeseries API

This document will show several examples on how to interact with timeseries using the QuasarDB Java API. For the sake of clarity it will show the usage of double data types only; the blob interface mirros the double interface exactly, and you should use the respective blob classes you find in the [JavaDoc](https://doc.quasardb.net/java/)

Given a QuasarDB timeseries table `doubles_test` that looks as follows

| timestamp           | value     |
| ------------------- | --------- |
| 2017-10-01 12:09:03 | 1.2345678 |
| 2017-10-01 12:09:04 | 8.7654321 |
| 2017-10-01 12:09:05 | 5.6789012 |
| 2017-10-01 12:09:06 | 2.1098765 |

## Inserting double data

The `insertDoubles` function is available on a `QdbTimeSeries` object which can be used to insert new double data into a timeseries. The following example shows how 

```java
String qdbUri = "qdb://127.0.0.1:2838";
QdbCluster cluster = new QdbCluster(qdbUri);
QdbTimeSeries series = cluster.gettimeSeries("doubles_test");

QdbDoubleColumnCollection data = new QdbDoubleColumnCollection("value");
data.add(new QdbDoubleColumnValue(1.2345678));
data.add(new QdbDoubleColumnValue(8.7654321));
data.add(new QdbDoubleColumnValue(5.6789012));
data.add(new QdbDoubleColumnValue(2.1098765));

series.insertDoubles(data);
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
