package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

final class QdbTagEntries implements Iterable<QdbEntry> {
    QdbEntryFactory factory;
    QdbSession session;
    String tag;

    class QdbTagEntriesIterator implements Iterator<QdbEntry> {
        final long handle;
        boolean hasNext;

        public QdbTagEntriesIterator() {
            Reference<Long> iterator = new Reference<Long>();

            session.throwIfClosed();
            int err = qdb.tag_iterator_begin(session.handle(), tag, iterator);
            handle = iterator.value;

            hasNext = err == qdb_error.ok;
            // CAUTION: set hasNext before throwing
            QdbExceptionFactory.throwIfError(err);
        }

        public boolean hasNext() {
            return hasNext;
        }

        public QdbEntry next() {
            session.throwIfClosed();
            QdbEntry entry = factory.createEntry(qdb.tag_iterator_type(handle), qdb.tag_iterator_alias(handle));

            int err = qdb.tag_iterator_next(handle);

            hasNext = err == qdb_error.ok;
            // CAUTION: set hasNext before throwing
            QdbExceptionFactory.throwIfError(err);

            return entry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void finalize() throws Throwable {
            qdb.tag_iterator_close(handle);
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
