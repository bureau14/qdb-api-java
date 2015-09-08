package net.quasardb.qdb;

import java.io.*;
import java.nio.file.*;

class NativeLibraryLoader {
    private static final Path localDirectory;

    static {
        try {
            localDirectory = Paths.get(
                System.getProperty("java.io.tmpdir"),
                "quasardb",
                JarFileHelper.getSignature());
            Files.createDirectories(localDirectory);
        } catch (Exception e) {
            e.printStackTrace();
            throw new NativeLibraryLoadError("Failed to create local directory: " + e.getMessage(), e);
        }
    }

    public static void load(String libraryName) throws NativeLibraryLoadError {
        Path localFile = localDirectory.resolve(libraryName);
        if (!Files.exists(localFile))
            extractFromJar(libraryName, localFile);
        loadLibrary(localFile);
    }

    private static void extractFromJar(String source, Path destination) throws NativeLibraryLoadError {
        System.out.println("Extracting " + destination);
        try {
            Files.copy(
                NativeLibraryLoader.class.getResourceAsStream("/" + source),
                destination);
        } catch (Exception e) {
            e.printStackTrace();
            throw new NativeLibraryLoadError("Failed to extract " + source + ": " + e.getMessage(), e);
        }
    }

    private static void loadLibrary(Path path) throws NativeLibraryLoadError {
        System.out.println("Loading " + path);
        try {
            System.load(path.toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new NativeLibraryLoadError("Failed to load " + path + ": " + e.getMessage(), e);
        }
    }
}