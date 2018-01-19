package net.quasardb.qdb;

import java.util.*;

import net.quasardb.qdb.ts.TimeRange;
import net.quasardb.qdb.jni.qdb_ts_aggregation_type;

public class QdbAggregation <T extends QdbColumnValue> {
    protected Type type;
    protected TimeRange range;
    protected long count;
    protected T result;

    public enum Type {
        FIRST(qdb_ts_aggregation_type.qdb_agg_first),
        LAST(qdb_ts_aggregation_type.qdb_agg_last),
        MIN(qdb_ts_aggregation_type.qdb_agg_min),
        MAX(qdb_ts_aggregation_type.qdb_agg_max),
        ARITHMETIC_MEAN(qdb_ts_aggregation_type.qdb_agg_arithmetic_mean),
        HARMONIC_MEAN(qdb_ts_aggregation_type.qdb_agg_harmonic_mean),
        GEOMETRIC_MEAN(qdb_ts_aggregation_type.qdb_agg_geometric_mean),
        QUADRATIC_MEAN(qdb_ts_aggregation_type.qdb_agg_quadratic_mean),
        COUNT(qdb_ts_aggregation_type.qdb_agg_count),
        SUM(qdb_ts_aggregation_type.qdb_agg_sum),
        SUM_OF_SQUARES(qdb_ts_aggregation_type.qdb_agg_sum_of_squares),
        SPREAD(qdb_ts_aggregation_type.qdb_agg_spread),
        SAMPLE_VARIANCE(qdb_ts_aggregation_type.qdb_agg_sample_variance),
        SAMPLE_STDDEV(qdb_ts_aggregation_type.qdb_agg_sample_stddev),
        POPULATION_VARIANCE(qdb_ts_aggregation_type.qdb_agg_population_variance),
        POPULATION_STDDEV(qdb_ts_aggregation_type.qdb_agg_population_stddev),
        ABS_MIN(qdb_ts_aggregation_type.qdb_agg_abs_min),
        ABS_MAX(qdb_ts_aggregation_type.qdb_agg_abs_max),
        PRODUCT(qdb_ts_aggregation_type.qdb_agg_product),
        SKEWNESS(qdb_ts_aggregation_type.qdb_agg_skewness),
        KURTOSIS(qdb_ts_aggregation_type.qdb_agg_kurtosis);

        protected final int value;
        Type(int type) {
            this.value = type;
        }
    }

    public QdbAggregation (Type type, TimeRange range, long count, T value) {
        this.type = type;
        this.range = range;
        this.count = count;
        this.result = value;
    }

    public String toString() {
        return "QdbAggregation (type: " + this.type + ", range: " + this.range.toString() + ", count: " + this.count + ", result: " + this.result.toString();
    }

    public Type getType() {
        return this.type;
    }

    public long getCount() {
        return this.count;
    }

    public TimeRange getRange() {
        return this.range;
    }

    public T getResult() {
        return this.result;
    }
}
