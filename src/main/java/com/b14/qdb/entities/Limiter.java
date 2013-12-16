/**
 * Copyright (c) 2009-2013, Bureau 14 SARL
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
 *    * Neither the name of Bureau 14 nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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
package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

/**
 * Entity for supervision metrics.
 *
 * @author &copy; <a href="https://www.bureau14.fr/">bureau14</a> - 2013
 * @version master
 * @since 0.7.5
 */
public class Limiter implements java.io.Serializable  {
    private static final long serialVersionUID = 7947626247907905742L;
    
    long max_bytes;
    long max_in_entries_count;
    
    public Limiter(@JsonProperty("max_bytes") long max_bytes, 
                   @JsonProperty("max_in_entries_count") long max_in_entries_count) {
        super();
        this.max_bytes=max_bytes;
        this.max_in_entries_count=max_in_entries_count;
    }

    public long getMax_bytes() {
        return max_bytes;
    }

    public void setMax_bytes(long max_bytes) {
        this.max_bytes = max_bytes;
    }

    public long getMax_in_entries_count() {
        return max_in_entries_count;
    }

    public void setMax_in_entries_count(long max_in_entries_count) {
        this.max_in_entries_count = max_in_entries_count;
    }
    
}
