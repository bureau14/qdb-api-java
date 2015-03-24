package com.b14.qdb;

import java.io.IOException;

/**
 * A integration helper which can start or stop a qdb instance.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version master
 * @since 1.2.1
 */
public enum Qdb {
    DAEMON;
    
    private Process daemon = null;
    
    public boolean start() {
        if (daemon != null) {
            this.stop();
        }
        
        String qdbdExecutable = System.getProperty("qdbd.path");
        if (qdbdExecutable == null) {
            qdbdExecutable = "D:/qdb-master-windows-64bit/bin/qdbd.exe";
        }

        Runtime runtime = Runtime.getRuntime();
        try {
            daemon = runtime.exec(new String[] { qdbdExecutable/*, "--transient"*/ } );
        } catch (IOException e) {
            System.err.println("Failed to start " + qdbdExecutable + " -> " + e.getMessage());
            return false;
        }
        
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    
    public boolean stop() {
        if (daemon != null) {
            daemon.destroy();
            daemon = null;
        } else {
            return false;
        }
        
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
