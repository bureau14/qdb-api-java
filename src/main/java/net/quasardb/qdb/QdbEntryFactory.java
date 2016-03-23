package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

final class QdbEntryFactory {
    final QdbSession session;

    public QdbEntryFactory(QdbSession session) {
        this.session = session;
    }

    public QdbEntry createEntry(String alias) {
        error_carrier err = new error_carrier();
        qdb_entry_type_t type = qdb.get_type(session.handle(), alias, err);
        QdbExceptionFactory.throwIfError(err);

        if (type == qdb_entry_type_t.errorntry_blob)
            return new QdbBlob(session, alias);

        if (type == qdb_entry_type_t.errorntry_deque)
            return new QdbDeque(session, alias);

        if (type == qdb_entry_type_t.errorntry_hset)
            return new QdbHashSet(session, alias);

        if (type == qdb_entry_type_t.errorntry_integer)
            return new QdbInteger(session, alias);

        if (type == qdb_entry_type_t.errorntry_stream)
            return new QdbStream(session, alias);

        if (type == qdb_entry_type_t.errorntry_tag)
            return new QdbTag(session, alias);

        return new QdbEntry(session, alias);
    }
}
