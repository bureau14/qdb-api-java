package net.quasardb.qdb;

import java.util.*;
import net.quasardb.qdb.jni.*;

class QdbJniApi {

    public static void load() throws NativeLibraryLoadError {
        String os = System.getProperty("os.name");
        String arch = System.getProperty("os.arch");

        if (os.startsWith("Windows")) {
            if (arch.equals("x86")) {
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86/qdb_api.dll");
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x86/qdb_api_jni.dll");
            } else {
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x64/qdb_api.dll");
                NativeLibraryLoader.load("/net/quasardb/qdb/jni/windows/x64/qdb_api_jni.dll");
            }
        } else if (os.startsWith("Mac OS X")) {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/macosx/x64/libqdb_api.dylib");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/macosx/x64/libqdb_api_jni.jnilib");
        } else if (os.startsWith("Linux")) {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x64/libqdb_api.so");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/linux/x64/libqdb_api_jni.so");
        } else if (os.startsWith("FreeBSD")) {
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x64/libqdb_api.so");
            NativeLibraryLoader.load("/net/quasardb/qdb/jni/freebsd/x64/libqdb_api_jni.so");
        } else {
            throw new RuntimeException("Unsupported operating system: " + os);
        }
    }
}