package com.b14.qdb;

import java.io.File;
import java.io.IOException;

public enum Qdb {
    DAEMON;
    
    private Process daemon = null;
    
    public boolean start() {
        if (daemon != null) {
            this.stop();
        }
        
        Runtime runtime = Runtime.getRuntime();
        boolean isWindows = System.getProperty("os.name").startsWith("Windows");
        String qdbPath = System.getProperty("qdb.path");
        if (qdbPath == null) {
            qdbPath = "D:/qdb-master-windows-64bit/bin";
        }
        try {
            daemon = runtime.exec(new String[] { isWindows ? qdbPath + File.separator + "qdbd.exe" : "qdbd"/*, "--transient"*/ } );
        } catch (IOException e) {
            e.printStackTrace();
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
