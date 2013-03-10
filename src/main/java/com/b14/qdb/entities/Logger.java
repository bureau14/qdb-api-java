package com.b14.qdb.entities;

import com.owlike.genson.annotation.JsonProperty;

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
