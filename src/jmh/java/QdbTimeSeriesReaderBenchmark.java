package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Reader;
import net.quasardb.qdb.ts.TimeRange;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
public class QdbTimeSeriesReaderBenchmark {

    @State(Scope.Thread)
    public static class ReadableTable {
        TimeRange[] ranges;
        String tableName;

        @Setup(Level.Trial)
        public void setup(Table table) throws Exception {
            System.out.println("Determining time ranges..");
            this.ranges = new TimeRange[]{ Helpers.rangeFromRows(table.rows) };

            System.out.println("Seeding table.....");
            this.tableName = Helpers.seedTable(table.cols, table.rows).getName();

            System.out.println("Got tablename = " + this.tableName);
        }
    }

    @State(Scope.Thread)
    public static class Reader {
        net.quasardb.qdb.ts.Reader reader;

        @Setup(Level.Iteration)
        public void setup(ReadableTable table) throws Exception {
            this.reader = Helpers.getTimeSeries(table.tableName).tableReader(table.ranges);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws Exception {
            this.reader.close();
        }
    }

    @Benchmark
    public Row readRow(Reader r) throws Exception {
        assert(r.reader.hasNext());
        return r.reader.next();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(QdbTimeSeriesReaderBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

        Helpers.createCluster();

        new Runner(opt).run();
    }

}
