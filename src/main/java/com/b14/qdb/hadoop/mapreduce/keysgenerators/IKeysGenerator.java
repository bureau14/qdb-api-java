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
import java.util.Collection;

import com.b14.qdb.Quasardb;

/**
 * Strategy for obtaining list of keys for splits, {@link IKeysGenerator}s must have a zero arg constructor.
 *
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.3.0
 */
public interface IKeysGenerator {
    /**
     * Thanks to hadoop's configuration framework a key generator has to deserialize and serialize itself this method 
     * and init(String) below are a light weight way of doing that
     * 
     * @return a String that can be used by the implementations init method to reconstitute the state of the generator
     * @throws IOException
     */
    String getInitString() throws IOException;

    /**
     * A string (from a prior call to getInitString) that this instance will use to set itself up to list keys
     * 
     * @param initString initialization string
     * @throws IOException
     */
    void init(String initString) throws IOException;

    /**
     * Get keys with the given qdb instance
     * 
     * @param qdb quasardb instance
     * @return list of all keys that keysgenerator have to handle with.
     * @throws {@link IllegalStateException} is init was not called and the lister is not set up to get keys
     */
    Collection<String> getKeys(Quasardb qdb);
}
