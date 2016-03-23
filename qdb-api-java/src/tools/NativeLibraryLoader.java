package net.quasardb.qdb;

import java.io.*;
import java.nio.file.*;
import java.net.URL;

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

    public static void load(String name) {
        Path localFile = getResourceAsLocalFile(name);
        loadLibrary(localFile);
    }

    public static Path getResourceAsLocalFile(String name) {
        URL url = NativeLibraryLoader.class.getResource(name);
        if (url == null)
            throw new NativeLibraryLoadError("Cannot find native library is classpath: " + name);

        if (url.getProtocol().equals("file"))
            return convertURLtoPath(url);

        if (url.getProtocol().equals("jar"))
            return extractFromJar(name);

        throw new NativeLibraryLoadError("Don't now how to extract: " + url);
    }

    private static Path convertURLtoPath(URL url) {
        try {
            return Paths.get(url.toURI());
        } catch (Exception e) {
            throw new NativeLibraryLoadError("Failed to get path of " + url + ": " + e, e);
        }
    }

    private static Path extractFromJar(String source) {
        Path fileName = Paths.get(source).getFileName();
        Path localFile = localDirectory.resolve(fileName);

        if (!Files.exists(localFile)) {
            System.out.println("Extract " + localFile);
            try {
                Files.copy(NativeLibraryLoader.class.getResourceAsStream(source), localFile);
            } catch (Exception e) {
                throw new NativeLibraryLoadError("Failed to extract " + source + ": " + e, e);
            }
        }

        return localFile;
    }

    private static void loadLibrary(Path path) {
        System.out.println("Loading " + path);
        try {
            System.load(path.toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new NativeLibraryLoadError("Failed to load " + path + ": " + e.getMessage(), e);
        }
    }
}