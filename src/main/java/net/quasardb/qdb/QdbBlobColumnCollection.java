package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;
import java.util.ArrayList;

public class QdbBlobColumnCollection extends ArrayList<ByteBuffer> {
    QdbColumnDefinition column;

    public QdbBlobColumnCollection (String alias) {
        super();
        this.column = new QdbColumnDefinition.Blob(alias);
    }
}
