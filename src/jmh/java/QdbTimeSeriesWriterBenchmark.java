package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.Row;
import net.quasardb.qdb.ts.Writer;
import net.quasardb.qdb.ts.AutoFlushWriter;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class QdbTimeSeriesWriterBenchmark {

    @State(Scope.Thread)
    public static class TimeSeries {
        QdbTimeSeries series;
        Iterator<Row> iterator;

        @Setup(Level.Iteration)
        public void setup(Table table) throws Exception {
            this.series = Helpers.createTimeSeries(table.cols);
            this.iterator = Arrays.stream(table.rows).iterator();
        }
    }

    @State(Scope.Thread)
    public static class Writer {
        @Param({"1", "100", "10000", "100000"})
        public int flushThreshold;

        net.quasardb.qdb.ts.Writer writer;

        @Setup(Level.Iteration)
        public void setup(TimeSeries ts) throws Exception {
            this.writer = ts.series.autoFlushTableWriter(this.flushThreshold);
        }

        @TearDown(Level.Iteration)
        public void tearDown(TimeSeries ts) throws Exception {
            this.writer.close();
        }
    }

    @Benchmark
    public void write(TimeSeries ts, Writer w) throws IOException {
        assert(ts.iterator.hasNext());
        w.writer.append(ts.iterator.next());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(QdbTimeSeriesWriterBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

        Helpers.createCluster();

        new Runner(opt).run();
    }

}
