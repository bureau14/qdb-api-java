package com.b14.qdb;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.b14.qdb.jni.SWIGTYPE_p_qdb_session;
import com.b14.qdb.jni.qdb;
import com.b14.qdb.jni.qdb_const_iterator_t;
import com.b14.qdb.jni.qdb_error_t;

/**
 * 
 */
public class QdbIterator implements Iterable<QdbEntry> {
    private final SWIGTYPE_p_qdb_session session;
    
    /**
     * 
     * @param session
     */
    protected QdbIterator(SWIGTYPE_p_qdb_session session) {
        this.session = session;
    }

    /**
     * {@inheritDoc}
     */
    public Iterator<QdbEntry> iterator() {
        return new InternalQdbIterator(session);
    }

    private final class InternalQdbIterator implements Iterator<QdbEntry> {
        private QdbEntry nextEntry = null;
        private QdbEntry lastEntry = null;
        private transient qdb_const_iterator_t iterator = null;
        private boolean iteratorStarted = false;
        private transient SWIGTYPE_p_qdb_session session;

        private InternalQdbIterator(final SWIGTYPE_p_qdb_session session) {
            this.session = session;
            nextEntry = null;
            lastEntry = null;
            iterator = null;
            iteratorStarted = false;
        }

        private final void startIterator() throws QdbException {
            // Start iterator operation
            this.iterator = new qdb_const_iterator_t();
            final qdb_error_t qdbError = qdb.iterator_begin(session, this.iterator);

            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QdbException(qdbError);
            }

            // Get alias value
            if (iterator.getContent_size() != 0) {
                final ByteBuffer buffer = qdb.iterator_content(iterator);
                nextEntry = new QdbEntry(iterator.getAlias(), buffer);
                iteratorStarted = true;
            }
        }

        private final void closeIterator() throws QdbException {
            // End iterator operation
            final qdb_error_t qdbError = qdb.iterator_close(this.iterator);

            // Handle errors
            if (qdbError != qdb_error_t.error_ok) {
                throw new QdbException(qdbError);
            }

            // Reset variables
            iteratorStarted = false;
            nextEntry = null;
            lastEntry = null;
            iterator = null;
        }

        private final void fetch() throws QdbException {
            // Get next qdb entry
            final qdb_error_t qdbError = qdb.iterator_next(iterator);

            // Handle errors
            if (qdbError == qdb_error_t.error_ok) {
                // Get alias value
                final ByteBuffer buffer = qdb.iterator_content(iterator);

                // Prepare ByteBuffer
                if (buffer != null) {
                    buffer.rewind();
                }
                nextEntry = new QdbEntry(iterator.getAlias(), buffer);
            } else if (qdbError == qdb_error_t.error_alias_not_found) {
                nextEntry = null;
                this.closeIterator();
            } else {
                throw new QdbException(qdbError);
            }
        }

        /**
         * {@inheritDoc}
         */
        public boolean hasNext() {
            try {
                if (!iteratorStarted) {
                    this.startIterator();
                }
                if (nextEntry == null) {
                    fetch();
                }
                return nextEntry != null;
            } catch (QdbException e) {
                return false;
            }
        }

        /**
         * {@inheritDoc}
         */
        public QdbEntry next() {
            if (hasNext()) {
                // Remember the lastEntry
                lastEntry = nextEntry;

                // Reset nextEntry to force fetching the next available entry
                nextEntry = null;
                return lastEntry;
            } else {
                throw new NoSuchElementException();
            }
        }

        /**
         * {@inheritDoc}
         */
        public void remove() {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }
}
