package net.quasardb.qdb;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.*;
import java.util.concurrent.TimeUnit;

import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.Result;
import net.quasardb.qdb.ts.Query;
import net.quasardb.qdb.ts.QueryBuilder;
import net.quasardb.qdb.ts.TimeRange;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
public class QdbTimeSeriesQueryBenchmark {

    @State(Scope.Thread)
    public static class QueryableTable {
        Query query;

        @Setup(Level.Trial)
        public void setup(Table table) throws Exception {
            System.out.println("Seeding table.....");
            String tableName = Helpers.seedTable(table.cols, table.rows).getName();

            System.out.println("Seeding table.....");
            this.query = new QueryBuilder()
                .add("select")
                .add(table.cols[0].getName())
                .add("from")
                .add(tableName)
                .in(Helpers.rangeFromRows(table.rows))
                .asQuery();
        }
    }

    @Benchmark
    public Result execute(QueryableTable q) throws Exception {
        return q.query.execute(Helpers.getSession());
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(QdbTimeSeriesQueryBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

        Helpers.createCluster();

        new Runner(opt).run();
    }

}
