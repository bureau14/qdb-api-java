package net.quasardb.qdb;

import java.io.IOException;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a columnar datapoint inside qdb
 */
public class QdbDoubleColumn extends QdbColumn {
    QdbDoubleColumn(String alias, Double value) {
        super(new QdbColumnDefinition.Double(alias), value);
    }
}
