package net.quasardb.qdb;

import java.time.Instant;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import net.quasardb.qdb.*;

@BenchmarkMode(Mode.AverageTime)
@Fork(1)
@State(Scope.Thread)
public class QdbTimeSeriesReaderBenchmark {

    @Param({"1", "10", "100"})
    public int colCount;

    QdbTimeSeries series;
    QdbTimeRange[] ranges;

    @Setup
    public void setup() throws Exception {
        QdbColumnDefinition[] cols = Helpers.generateTableColumns(colCount);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 1000000);

        QdbTimeRange[] ranges  = { Helpers.rangeFromRows(rows) };

        this.ranges = ranges;
        this.series = Helpers.seedTable(cols, rows);
    }

    private void run(int rowCount) {
        QdbTimeSeriesReader reader = this.series.tableReader(ranges);

        while (reader.hasNext()) {
            reader.next();
        }
    }

    @Benchmark
    @OperationsPerInvocation(100)
    public void readRows_100() {
        this.run(100);
    }

    @Benchmark
    @OperationsPerInvocation(1000)
    public void readRows_1000() {
        this.run(1000);
    }

    @Benchmark
    @OperationsPerInvocation(10000)
    public void readRows_10000() {
        this.run(10000);
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void readRows_100000() {
        this.run(100000);
    }

    @Benchmark
    @OperationsPerInvocation(1000000)
    public void readRows_1000000() {
        this.run(1000000);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(QdbTimeSeriesReaderBenchmark.class.getSimpleName())
            .warmupIterations(5)
            .measurementIterations(5)
            .forks(1)
            .build();

        new Runner(opt).run();
    }

}
