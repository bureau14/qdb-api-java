package net.quasardb.qdb;

import java.lang.Runtime;

public class DaemonRunner {
    static Process process;

    static {
        String path = findDaemon();
        startDaemon(path);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                stopDaemon();
            }
        });
    }

    public static String uri() {
        return "qdb://127.0.0.1:" + port();
    }

    public static int port() {
        return 28360; // <- avoid default port to prevent conflict
    }

    private static String findDaemon() {
        String path = System.getProperty("qdbd.path");
        if (path == null) {
            path = "qdbd";
            System.out.println("Warning: qdbd.path not set.");
        } else {
            System.out.println("qdbd.path = " + path);
        }
        return path;
    }

    private static void startDaemon(String path) {
        try {
            Runtime runtime = Runtime.getRuntime();
            System.out.println("Start " + path);
            process = runtime.exec(new String[] {path, "--transient", "-a", "127.0.0.1:" + port()});
            System.out.println(path + " started, waiting...");
            Thread.sleep(2000);
            System.out.println(path + " ready");

        } catch (Exception e) {
            String message = "Failed to start " + path + " -> " + e.getMessage();
            System.err.println(message);
            throw new RuntimeException(message);
        }
    }

    private static void stopDaemon() {
        System.out.println("Kill qdbd process");

        if (process != null) {
            process.destroy();
        }
    }
}
