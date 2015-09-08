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
            System.out.println("Warning: qdbd.path not set.");
        } else {
            System.out.println("qdbd.path = " + qdbdExecutable);
        }

        System.out.println("Starting " + qdbdExecutable);

        Runtime runtime = Runtime.getRuntime();
        try {
            daemon = runtime.exec(new String[] {qdbdExecutable, "--transient"});
        } catch (IOException e) {
            System.err.println("Failed to start " + qdbdExecutable + " -> " + e.getMessage());
            return false;
        }

        System.out.println(qdbdExecutable + " started, waiting...");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        System.out.println(qdbdExecutable + " ready");

        return true;
    }

    public boolean stop() {

        System.out.println("[QDBD] Kill daemon");

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
