package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

final class QdbEntryFactory {
    final Session session;

    public QdbEntryFactory(Session session) {
        this.session = session;
    }

    public QdbEntry createEntry(String alias) {
        Reference<Integer> type = new Reference<Integer>();
        qdb.get_type(session.handle(), alias, type);
        return createEntry(type.value, alias);
    }

    public QdbEntry createEntry(int type, String alias) {
        switch (type) {
        case qdb_entry_type.blob:
            return new QdbBlob(session, alias);

        case qdb_entry_type.integer:
            return new QdbInteger(session, alias);

        case qdb_entry_type.tag:
            return new QdbTag(session, alias);

        default:
            return new QdbEntry(session, alias);
        }
    }
}
