package net.quasardb.qdb;

import java.io.IOException;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a columnar datapoint inside qdb
 */
public class QdbBlobColumn extends QdbColumn  {
    QdbBlobColumn(String alias, String value) {
        super(new QdbColumnDefinition.Blob(alias), value);
    }
}
