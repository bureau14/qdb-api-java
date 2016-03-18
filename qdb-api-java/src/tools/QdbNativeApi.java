package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

class QdbNativeApi {

    public static void load() throws NativeLibraryLoadError {
        String os = System.getProperty("os.name");

        if (os.startsWith("Windows")) {
            NativeLibraryLoader.load("qdb_api.dll");
            NativeLibraryLoader.load("qdb_api_jni.dll");
        } else if (os.startsWith("Mac OS X")) {
            NativeLibraryLoader.load("libqdb_api.dylib");
            NativeLibraryLoader.load("libqdb_api_jni.jnilib");
        } else if (os.startsWith("Linux") || os.startsWith("FreeBSD")) {
            NativeLibraryLoader.load("libqdb_api.so");
            NativeLibraryLoader.load("libqdb_api_jni.so");
        } else {
            throw new RuntimeException("Unsupported operating system: " + os);
        }
    }

    public static List<String> resultsToList(StringVec results) {
        int vecSize = (int)results.size();

        Vector<String> entries = new Vector<String>(vecSize, 2);

        for (int i = 0; i < vecSize; i++) {
            entries.add(results.get(i));
        }

        return entries;
    }
}