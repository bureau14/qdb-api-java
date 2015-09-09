package net.quasardb.qdb;

import java.io.*;

class JarFileHelper {

    public static String getSignature() throws IOException {
        return convertBinaryToString(getBinarySignature(getFileLocation()));
    }

    static byte[] getBinarySignature(String path) throws IOException {
        System.out.println("Read " + path);
        RandomAccessFile file = new RandomAccessFile(path, "r");
        byte[] signature = new byte[4];
        file.seek(10);
        file.read(signature);
        return signature;
    }

    static String convertBinaryToString(byte[] bytes) {
        return javax.xml.bind.DatatypeConverter.printHexBinary(bytes);
    }

    static String getFileLocation() {
        return JarFileHelper.class
            .getProtectionDomain()
            .getCodeSource()
            .getLocation()
            .getPath();
    }
}