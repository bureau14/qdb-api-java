package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;


public class QdbDoubleColumnCollection extends QdbColumnCollection<Double> {
    QdbColumnDefinition column;

    public QdbDoubleColumnCollection (String alias) {
        super(new QdbColumnDefinition.Double(alias));
    }

    double[] toNative() {
        return this.stream()
            .map(QdbColumnValue::getValue)
            .map(java.lang.Double::doubleValue)
            .flatMapToDouble(n -> DoubleStream.of(n))
            .toArray();
    }
}
