package net.quasardb.qdb;

import java.io.IOException;

/**
 * A integration helper which can start or stop a qdb instance.
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version 2.0.0
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
            qdbdExecutable = "qdbd";
        }

        Runtime runtime = Runtime.getRuntime();
        try {
            daemon = runtime.exec(new String[] { qdbdExecutable, "--transient" } );
        } catch (IOException e) {
            System.err.println("Failed to start " + qdbdExecutable + " -> " + e.getMessage());
            return false;
        }
        
        try {
            Thread.sleep(6000);
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
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
