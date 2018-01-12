package net.quasardb.qdb;

import java.time.Instant;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import net.quasardb.qdb.*;

@State(Scope.Thread)
public class QdbTimespecBenchmark {

    public QdbClock clock;

    @Setup
    public void setup() throws Exception {
        this.clock = new QdbClock();
    }

    @Benchmark
    public QdbTimespec jni() {
        return QdbTimespec.now();
    }

    @Benchmark
    public QdbTimespec clock() {
        return QdbTimespec.now(this.clock);
    }

    @Benchmark
    public QdbTimespec instant() {
        return new QdbTimespec(Instant.now());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QdbTimespecBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
