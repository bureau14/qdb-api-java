package net.quasardb.qdb;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import net.quasardb.qdb.jni.*;
import java.util.*;

/**
 * Represents a columnar datapoint inside qdb
 */
abstract public class QdbColumn <T> {
    QdbColumnDefinition definition;
    T value;

    protected QdbColumn(QdbColumnDefinition definition, T value) {
        this.definition = definition;
        this.value = value;
    }

    public QdbColumnDefinition getDefinition() {
        return this.definition;
    }

    public T getValue() {
        return this.value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QdbColumn)) return false;
        QdbColumn rhs = (QdbColumn)obj;

        return this.definition == rhs.definition && this.value == rhs.value;
    }

    public String toString() {
        return "QdbColumn (definition: '" + this.definition.toString() + "', value: " + this.value.toString() + ")";
    }
}
