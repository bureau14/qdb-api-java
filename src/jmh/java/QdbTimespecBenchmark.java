package net.quasardb.qdb;

import java.time.Instant;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import net.quasardb.qdb.*;

public class QdbTimespecBenchmark {

    @Benchmark
    public void qdbTimespec() {
        QdbTimespec.now();
    }

    @Benchmark
    public void instant() {
        Instant.now();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QdbTimespecBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
