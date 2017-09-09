package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

public class QdbDoubleColumn extends QdbColumn<Double> {
    public QdbDoubleColumn (String alias, Double value) {
        super (new QdbColumnDefinition.Double (alias), value);
    }
}
