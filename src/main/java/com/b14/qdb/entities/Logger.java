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
public class Logger implements java.io.Serializable {
    private static final long serialVersionUID = -1824742376700979459L;
    
    String dump_file;
    int flush_interval;
    int[] listen_on;
    int log_level;
    boolean log_to_console;
    boolean log_to_syslog;
    
    public Logger(@JsonProperty("dump_file") String dump_file,
                  @JsonProperty("flush_interval") int flush_interval,
                  @JsonProperty("log_files") int[] listen_on,
                  @JsonProperty("log_level") int log_level,
                  @JsonProperty("log_to_console") boolean log_to_console,
                  @JsonProperty("log_to_syslog") boolean log_to_syslog) {
        super();
        this.dump_file=dump_file;
        this.flush_interval=flush_interval;
        this.listen_on=listen_on;
        this.log_level=log_level;
        this.log_to_console=log_to_console;
        this.log_to_syslog=log_to_syslog;
    }
    
    public String getDump_file() {
        return dump_file;
    }

    public void setDump_file(String dump_file) {
        this.dump_file = dump_file;
    }

    public int getFlush_interval() {
        return flush_interval;
    }

    public void setFlush_interval(int flush_interval) {
        this.flush_interval = flush_interval;
    }

    public int[] getListen_on() {
        return listen_on;
    }

    public void setListen_on(int[] listen_on) {
        this.listen_on = listen_on;
    }

    public int getLog_level() {
        return log_level;
    }

    public void setLog_level(int log_level) {
        this.log_level = log_level;
    }

    public boolean isLog_to_console() {
        return log_to_console;
    }

    public void setLog_to_console(boolean log_to_console) {
        this.log_to_console = log_to_console;
    }

    public boolean isLog_to_syslog() {
        return log_to_syslog;
    }

    public void setLog_to_syslog(boolean log_to_syslog) {
        this.log_to_syslog = log_to_syslog;
    }
}
