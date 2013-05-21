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
 * @author &copy; <a href="http://www.bureau14.fr/">bureau14</a> - 2013
 * @version quasardb 1.0.0
 * @since quasardb 0.7.5
 */
public class Operations implements java.io.Serializable {
    private static final long serialVersionUID = -7025011222312385692L;
    
    CompareAndSwap compare_and_swap;
    Find find;
    FindUpdate find_update;
    Put put;
    Remove remove;
    RemoveAll remove_all;
    Update update;
    
    public Operations(@JsonProperty("compare_and_swap") CompareAndSwap compare_and_swap, 
                 @JsonProperty("find") Find find,
                 @JsonProperty("find_update") FindUpdate find_update,
                 @JsonProperty("put") Put put,
                 @JsonProperty("remove") Remove remove,
                 @JsonProperty("remove_all") RemoveAll remove_all,
                 @JsonProperty("update") Update update) {
        super();
        this.compare_and_swap=compare_and_swap;
        this.find=find;
        this.find_update=find_update;
        this.put=put;
        this.remove=remove;
        this.remove_all=remove_all;
        this.update=update;
    }

    public CompareAndSwap getCompare_and_swap() {
        return compare_and_swap;
    }

    public void setCompare_and_swap(CompareAndSwap compare_and_swap) {
        this.compare_and_swap = compare_and_swap;
    }

    public Find getFind() {
        return find;
    }

    public void setFind(Find find) {
        this.find = find;
    }

    public FindUpdate getFind_update() {
        return find_update;
    }

    public void setFind_update(FindUpdate find_update) {
        this.find_update = find_update;
    }

    public Put getPut() {
        return put;
    }

    public void setPut(Put put) {
        this.put = put;
    }

    public Remove getRemove() {
        return remove;
    }

    public void setRemove(Remove remove) {
        this.remove = remove;
    }

    public RemoveAll getRemove_all() {
        return remove_all;
    }

    public void setRemove_all(RemoveAll remove_all) {
        this.remove_all = remove_all;
    }

    public Update getUpdate() {
        return update;
    }

    public void setUpdate(Update update) {
        this.update = update;
    }
    
}
