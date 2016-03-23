package net.quasardb.qdb;

import java.io.*;
import java.net.URL;
import javax.xml.bind.DatatypeConverter;

class JarFileHelper {

    public static String getSignature() {
        URL jarUrl = JarFileHelper.class.getProtectionDomain().getCodeSource().getLocation();

        try (InputStream s = jarUrl.openStream()) {
            s.skip(10);

            byte[] signature = new byte[4];
            s.read(signature);
            return DatatypeConverter.printHexBinary(signature);
        } catch (IOException e) {
            return "";
        }
    }
}