package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

final class QdbTagEntries implements Iterable<QdbEntry> {
    QdbEntryFactory factory;
    QdbSession session;
    String tag;

    class QdbTagEntriesIterator implements Iterator<QdbEntry> {
        final tag_iterator iterator;
        boolean hasNext;

        public QdbTagEntriesIterator() {
            iterator = new tag_iterator();

            session.throwIfClosed();
            qdb_error_t err = iterator.begin(session.handle(), tag);

            hasNext = err == qdb_error_t.error_ok;
            // CAUTION: set hasNext before throwing
            QdbExceptionFactory.throwIfError(err);
        }

        public boolean hasNext() {
            return hasNext;
        }

        public QdbEntry next() {
            session.throwIfClosed();
            QdbEntry entry = factory.createEntry(iterator.type(), iterator.alias());

            qdb_error_t err = iterator.next();

            hasNext = err == qdb_error_t.error_ok;
            // CAUTION: set hasNext before throwing
            QdbExceptionFactory.throwIfError(err);

            return entry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() throws Throwable {
            iterator.close();
            super.finalize();
        }
    }

    protected QdbTagEntries(QdbSession session, String tag) {
        this.session = session;
        this.tag = tag;
        factory = new QdbEntryFactory(session);
    }

    public Iterator<QdbEntry> iterator() {
        return new QdbTagEntriesIterator();
    }
}
