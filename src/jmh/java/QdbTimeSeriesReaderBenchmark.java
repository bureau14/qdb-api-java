package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class QdbTimeSeriesReaderBenchmark {

    @State(Scope.Thread)
    public static class Table {
        @Param({"1", "10", "25", "100"})
        public int colCount;

        QdbColumnDefinition[] cols;
        QdbTimeSeriesRow[] rows;
        QdbTimeRange[] ranges ;
        String tableName;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            System.out.println("Generating columns..");
            this.cols = Helpers.generateTableColumns(colCount);

            System.out.println("Generating rows..");
            this.rows = Helpers.generateTableRows(cols, 10000000 / colCount);

            System.out.println("Determining time ranges..");
            this.ranges = new QdbTimeRange[]{ Helpers.rangeFromRows(this.rows) };

            System.out.println("Seeding table.....");
            this.tableName = Helpers.seedTable(this.cols, this.rows).getName();

            System.out.println("Got tablename = " + this.tableName);
        }
    }

    @State(Scope.Thread)
    public static class Reader {
        QdbTimeSeriesReader reader;

        @Setup(Level.Iteration)
        public void setup(Table table) throws Exception {
            System.out.println("Setting up Reader from table " + table.tableName + " and timeseries");
            this.reader = Helpers.getTimeSeries(table.tableName).tableReader(table.ranges);
        }

        @TearDown(Level.Iteration)
        public void tearDown() throws Exception {
            this.reader.close();
        }
    }

    @Benchmark
    public QdbTimeSeriesRow readRow(Reader r) throws Exception {
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
