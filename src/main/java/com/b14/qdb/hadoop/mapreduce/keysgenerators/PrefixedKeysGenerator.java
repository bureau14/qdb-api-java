/**
 * Copyright (c) 2009-2015, quasardb SAS
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.b14.qdb.hadoop.mapreduce.keysgenerators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import com.b14.qdb.Quasardb;
import com.b14.qdb.QuasardbException;

/**
 * A keys generator based on a provided prefix search in the quasardb cluster.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @see IKeysGenerator
 * @version master
 * @since 1.3.0
 */
public class PrefixedKeysGenerator implements IKeysGenerator {
    private String initString;

    /**
     * No argument constructor for de-serialization
     */
    public PrefixedKeysGenerator() {
    }
    
    public PrefixedKeysGenerator(String initString) {
        this.initString = initString;
    }
    
    /**
     * {@inheritDoc}
     */
    public String getInitString() throws IOException {
        return this.initString;
    }

    /**
     * {@inheritDoc}
     */
    public void init(String initString) throws IOException {
        if (initString == null) {
            throw new IllegalArgumentException("initString cannot be null");
        }
        this.initString = initString;
    }

    /**
     * {@inheritDoc}
     */
    public Collection<String> getKeys(Quasardb qdb) {
        if (qdb == null) {
            throw new IllegalStateException("No quasardb client provided.");
        }
        if (this.initString == null) {
            throw new IllegalStateException("No initString provided for generator.");
        }
        if (!qdb.isConnected()) {
            try {
                qdb.connect();
            } catch (QuasardbException e) {
                throw new IllegalStateException("quasardb client not connected => " + e.getMessage());
            }
        }
        try {
            return qdb.startsWith(this.initString);
        } catch (QuasardbException e) {
            return new ArrayList<String>();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((initString == null) ? 0 : initString.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PrefixedKeysGenerator)) {
            return false;
        }
        PrefixedKeysGenerator other = (PrefixedKeysGenerator) obj;
        if (initString == null) {
            if (other.initString != null) {
                return false;
            }
        } else if (!initString.equals(other.initString)) {
            return false;
        }
        return true;
    }
}
