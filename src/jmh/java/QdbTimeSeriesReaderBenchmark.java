package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Thread)
public class QdbTimeSeriesReaderBenchmark {

    @Param({"1", "10", "25", "100"})
    public int colCount;

    QdbTimeSeriesReader reader;

    @Setup(Level.Iteration)
    public void setup() throws Exception {
        QdbColumnDefinition[] cols = Helpers.generateTableColumns(colCount);
        QdbTimeSeriesRow[] rows = Helpers.generateTableRows(cols, 1000000 / colCount);

        QdbTimeRange[] ranges  = { Helpers.rangeFromRows(rows) };

        QdbTimeSeries series = Helpers.seedTable(cols, rows);
        this.reader = series.tableReader(ranges);
    }

    @Benchmark
    public QdbTimeSeriesRow readRow() {
        assert(this.reader.hasNext());
        return this.reader.next();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(QdbTimeSeriesReaderBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .shouldDoGC(true)
            .build();

        Helpers.createCluster();

        new Runner(opt).run();
    }

}
