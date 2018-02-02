package net.quasardb.qdb;

import java.time.Instant;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.NanoClock;
import net.quasardb.qdb.ts.Timespec;

@State(Scope.Thread)
public class QdbTimespecBenchmark {

    public NanoClock clock;

    @Setup
    public void setup() throws Exception {
        this.clock = new NanoClock();
    }

    @Benchmark
    public Timespec jni() {
        return Timespec.now();
    }

    @Benchmark
    public Timespec clock() {
        return Timespec.now(this.clock);
    }

    @Benchmark
    public Timespec instant() {
        return new Timespec(Instant.now());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(QdbTimespecBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

}
