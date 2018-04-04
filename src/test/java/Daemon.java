package net.quasardb.qdb;

import java.lang.Runtime;

public class Daemon {

    public static int port() {
        return Integer.parseInt(System.getProperty("qdbd.port"));
    }

    public static int securePort() {
        return Integer.parseInt(System.getProperty("qdbd.secure.port"));
    }

    public static String uri() {
        return "qdb://127.0.0.1:" + port();
    }

    public static String secureUri() {
        return "qdb://127.0.0.1:" + securePort();
    }
}
