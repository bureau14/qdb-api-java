package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a columnar datapoint inside qdb
 */
public class QdbColumn <T> {
    QdbColumnDefinition definition;
    T value;

    QdbColumn(QdbColumnDefinition definition, T value) {
        this.definition = definition;
        this.value = value;
    }

    public QdbColumnDefinition getDefinition() {
        return this.definition;
    }

    public T getValue() {
        return this.value;
    }
}
