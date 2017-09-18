import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import net.quasardb.qdb.*;
import org.junit.*;
import org.hamcrest.Matcher;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class QdbTimeSeriesBlobTest {
    @Test
    public void canSerialize_andDeserialize() throws Exception {
        ByteBuffer data = Helpers.createSampleData();
        QdbBlobColumnValue vBefore = new QdbBlobColumnValue(data);

        byte[] serialized = Helpers.serialize(vBefore);
        QdbBlobColumnValue vAfter = (QdbBlobColumnValue)Helpers.deserialize(serialized, vBefore.getClass());

        assertEquals(vBefore, vAfter);
    }

    @Test
    public void doesNotThrow_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Blob (alias)));

        QdbBlobColumnCollection data = Helpers.createBlobColumnCollection(alias);

        series.insertBlobs(data);
    }

    @Test
    public void canGetResults_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Blob (alias)));

        QdbBlobColumnCollection data = Helpers.createBlobColumnCollection(alias);

        QdbTimeRange dataRange = data.range();
        series.insertBlobs(data);

        QdbTimeRangeCollection ranges = new QdbTimeRangeCollection();
        ranges.add(new QdbTimeRange(dataRange.getBegin(),
                                    new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1))));

        QdbBlobColumnCollection results = series.getBlobs(alias, ranges);

        for(QdbBlobColumnValue value : data) {
            assertThat(results, hasItem(value));
        }
    }

    @Test
    public void canAgregateCount_afterInsert() throws Exception {
        String alias = Helpers.createUniqueAlias();
        QdbTimeSeries series =
            Helpers.createTimeSeries(Arrays.asList(new QdbColumnDefinition.Blob (alias)));

        QdbBlobColumnCollection data = new QdbBlobColumnCollection(alias);
        data.add(new QdbBlobColumnValue(Helpers.createSampleData()));
        data.add(new QdbBlobColumnValue(Helpers.createSampleData()));

        QdbTimeRange dataRange = data.range();
        series.insertBlobs(data);

        QdbBlobAggregationCollection aggregations = new QdbBlobAggregationCollection();
        aggregations.add(new QdbBlobAggregation(QdbAggregation.Type.COUNT,
                                                  new QdbTimeRange(dataRange.getBegin(),
                                                                   new QdbTimespec(dataRange.getEnd().getValue().plusNanos(1)))));

        QdbBlobAggregationCollection result = series.blobAggregate(alias, aggregations);
        assertEquals(result.size(), aggregations.size());
        assertEquals(2, result.get(0).getCount());
    }
}
