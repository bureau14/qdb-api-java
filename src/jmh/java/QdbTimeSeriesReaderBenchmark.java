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

    @Param({"100", "10000", "1000000"})
    public int rowCount;

    QdbTimeSeries series;
    QdbTimeRange[] ranges;

    @Setup
    public void setup() throws Exception {
        QdbColumnDefinition[] cols = Helpers.generateTableColumns(colCount);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, rowCount);

        this.series = Helpers.seedTable(cols, rows);
        QdbTimeRange[] ranges  = { Helpers.rangeFromRows(rows) };
        this.ranges = ranges;
    }

    @Benchmark
    public void readRows() {
        QdbTimeSeriesReader reader = this.series.tableReader(ranges);

        while (reader.hasNext()) {
            reader.next();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QdbTimeSeriesReaderBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
