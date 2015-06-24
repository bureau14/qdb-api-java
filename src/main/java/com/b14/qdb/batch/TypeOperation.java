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
package com.b14.qdb.batch;

/**
 * A Quasardb batch operation type. <br>
 * Available operations are :
 * 
 * <ul>
 * <li>GET : get an entry.</li>
 * <li>PUT : put an entry.</li>
 * <li>UPDATE : update an entry.</li>
 * <li>REMOVE : remove an entry.</li>
 * <li>CAS : atomically compare a value with comparand and update if it matches.</li>
 * <li>GET_UPDATE : atomically update the value of an existing entry and return the old value.</li>
 * <li>GET_REMOVE : atomically get the entry associated with the supplied key and remove it.</li>
 * <li>REMOVE_IF : delete the object associated whith a key if the object is equal to comparand.</li>
 * <li>NO_OP : no operation.</li>
 * </ul>
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2014
 * @version 2.0.0
 * @since 1.1.0
 */
public enum TypeOperation {
    GET,
    PUT,
    UPDATE,
    REMOVE,
    CAS,
    GET_UPDATE,
    GET_REMOVE,
    REMOVE_IF,
    NO_OP
}

