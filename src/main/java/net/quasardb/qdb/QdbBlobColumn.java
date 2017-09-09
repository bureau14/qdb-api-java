package net.quasardb.qdb;

import java.nio.ByteBuffer;
import net.quasardb.qdb.jni.*;

public class QdbBlobColumn extends QdbColumn<ByteBuffer> {
    public QdbBlobColumn(String alias, ByteBuffer value) {
        super (new QdbColumnDefinition.Blob (alias), value);
    }
}
