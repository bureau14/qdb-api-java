package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

final class QdbEntryTags implements Iterable<QdbTag> {
    QdbSession session;
    String alias;

    class QdbEntryTagsIterator implements Iterator<QdbTag> {
        final StringVec tags;
        int index;

        public QdbEntryTagsIterator(StringVec tags) {
            this.tags = tags;
            index = 0;
        }

        public boolean hasNext() {
            return index < tags.size();
        }

        public QdbTag next() {
            return new QdbTag(session, tags.get(index++));
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    protected QdbEntryTags(QdbSession session, String alias) {
        this.session = session;
        this.alias = alias;
    }

    public Iterator<QdbTag> iterator() {
        session.throwIfClosed();
        results_list res = qdb.get_tags(session.handle(), alias);
        QdbExceptionFactory.throwIfError(res.getError());
        return new QdbEntryTagsIterator(res.getResults());
    }
}
