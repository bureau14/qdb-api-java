package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

final class QdbEntryTags implements Iterable<QdbTag> {
    QdbSession session;
    String alias;

    class QdbEntryTagsIterator implements Iterator<QdbTag> {
        final String[] tags;
        int index;

        public QdbEntryTagsIterator(String[] tags) {
            this.tags = tags;
            index = 0;
        }

        public boolean hasNext() {
            return index < tags.length;
        }

        public QdbTag next() {
            return new QdbTag(session, tags[index++]);
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
        Reference<String[]> tags = new Reference<String[]>();
        int err = qdb.get_tags(session.handle(), alias, tags);
        QdbExceptionFactory.throwIfError(err);
        return new QdbEntryTagsIterator(tags.value);
    }
}
