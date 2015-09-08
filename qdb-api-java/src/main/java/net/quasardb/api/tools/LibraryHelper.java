/**
 * Copyright (c) 2009-2015, quasardb SAS
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of quasardb nor the names of its contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package net.quasardb.qdb.tools;

import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.jar.Attributes.Name;

/**
 * Utility class : load quasardb natives libraries
 * 
 * @author &copy; <a href="http://www.quasardb.fr">quasardb</a> - 2015
 * @version 2.0.0
 * @since 0.5.2
 */
public class LibraryHelper {
    private static final boolean DEBUG = false;
    private static final String TMP_SUB_DIRECTORY = "qdb_native_libs";
    private static final int arch = ((System.getProperty("os.arch").indexOf("64") >= 0) ? 64 : 32);
    private static final String os = (System.getProperty("os.name").toLowerCase().contains("bsd") ? "bsd" : (System.getProperty("os.name").toLowerCase().contains("linux") ? "linux" : (arch == 64) ? "x64" : "x86"));
    private static final String[] LIBS = {
        "libjvm",
        "libmawt",
        "libjawt",
        "msvcr100",
        "msvcp100",
        "tbb",
        "tbbmalloc",
        "gcc_s",
        "stdc++",
        "qdb_api",
        "qdb_java_api"};
    private static final Map<String, Boolean> LOADED_LIBS = new HashMap<String, Boolean>();
    private static File dirTemp = null;
    private static List<JarFileInfo> lstJarFile;

    /**
     * Load native librairies from API jar
     */
    public static void loadLibrairiesFromJar() {
        // Keep a list of temp files
        lstJarFile = new ArrayList<JarFileInfo>();

        // Extract current jar to load native library
        ProtectionDomain pd = LibraryHelper.class.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL urlTopJAR = cs.getLocation(); // URL.getFile() returns "/C:/my%20dir/MyApp.jar"
        System.out.println("JAR file: " + urlTopJAR);
        String sUrlTopJAR;
        try {
            sUrlTopJAR = URLDecoder.decode(urlTopJAR.getFile(), "UTF-8");
            File fileJAR = new File(sUrlTopJAR);
            loadJar(fileJAR.getName(), fileJAR, null); // throws if not JAR
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Load uncompressed libraries
        File dir = new File(System.getProperty("java.io.tmpdir"), TMP_SUB_DIRECTORY);

        System.out.println("Loading native libraries from " + dir);

        // Sort libraries loading order
        String[] files = dir.list();
        if (files != null) {
            Arrays.sort(files, new Comparator<String>() {
                public int compare(String obj1, String obj2) {
                    int i = 0, iObj1 = 0, iObj2 = 0;
                    for (String lib : LIBS) {
                        i++;
                        if (obj1.contains(lib)) {
                            iObj1 = i;
                            continue;
                        }
                        if (obj2.contains(lib)) {
                            iObj2 = i;
                            continue;
                        }
                    }
                    if (iObj1 > iObj2)
                        return +1;
                    if (iObj1 < iObj2)
                        return -1;
                    return 0;
                }
            });

            // Load library and check if not already loaded -> cause a jvm crash with sun jvm
            for (String file : files) {
                if (LOADED_LIBS.get(file) == null) {
                    LOADED_LIBS.put(file, Boolean.TRUE);
                    loadLib(file);
                }
            }
        }

        // Cleanup temp files on exit
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                shutdown();
            }
        });
    }

    /**
     * Extract native librairies from jar
     * 
     * @param simpleName jar name
     * @param file jar file
     * @param jarFileInfoParent parent Jar file
     * @throws IOException Error on loading inner JAR
     */
    private static void loadJar(String simpleName, File file, JarFileInfo jarFileInfoParent) throws IOException {
        JarFileInfo jarFileInfo = new JarFileInfo(simpleName, file, jarFileInfoParent);
        lstJarFile.add(jarFileInfo);
        try {
            Enumeration<JarEntry> en = jarFileInfo.jarFile.entries();
            final String EXT_JAR = ".jar";
            while (en.hasMoreElements()) {
                JarEntry je = en.nextElement();
                if (je.isDirectory()) {
                    continue;
                }
                String sEntryName = je.getName().toLowerCase(); // JarEntry name
                if ((sEntryName.lastIndexOf(EXT_JAR) == sEntryName.length() - EXT_JAR.length())) {
                    JarEntryInfo inf = new JarEntryInfo(jarFileInfo, je);
                    File fileTemp = createTempFile(inf);
                    loadJar(inf.getName(), fileTemp, jarFileInfo);
                }
                if (sEntryName.contains(".so") || sEntryName.contains(".dll")) {
                    JarEntryInfo inf = new JarEntryInfo(jarFileInfo, je);
                    System.out.println("Extract native library " + sEntryName);
                    createTempFile(inf);
                }
            }
        } catch (LibraryHelperException e) {
            throw new RuntimeException("ERROR on loading inner JAR: " + e.getMessageAll());
        }
    }

    /**
     * Puts library to temp dir and loads to memory
     * @throws LibraryHelperException 
     */
    private static void loadLib(String sLib) {
        System.out.println("Look for native library " + sLib);
        for (JarFileInfo jarFileInfo : lstJarFile) {
            JarFile jarFile = jarFileInfo.jarFile;
            Enumeration<JarEntry> en = jarFile.entries();
            while (en.hasMoreElements()) {
                JarEntry je = en.nextElement();
                if (je.isDirectory()) {
                    continue;
                }
                // Example: sName is "Native.dll"
                String sEntry = je.getName(); // "Native.dll" or "abc/xyz/Native.dll"
                // sName "Native.dll" could be found, for example
                //   - in the path: abc/Native.dll/xyz/my.dll <-- do not load this one!
                //   - in the partial name: abc/aNative.dll   <-- do not load this one!
                String[] token = sEntry.split("/"); // the last token is library name
                if (token.length > 0 && sLib.contains(token[token.length - 1]) && sEntry.contains(os)) {
                    try {
                        if (DEBUG)
                            System.out.println("Loading native library '" + sLib + "' found as '" + sEntry + "' in JAR '" + jarFileInfo.simpleName + "'");
                        String file = new File(System.getProperty("java.io.tmpdir"), TMP_SUB_DIRECTORY).getAbsolutePath() + File.separator + sLib;
                        System.out.println("Load " + file);
                        System.load(file);
                    } catch (UnsatisfiedLinkError e) {
                        System.out.println("Cannot load library " + sLib);
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Create temp directories for natives librairies
     * 
     * @param inf library
     * @return library
     * @throws LibraryHelperException cannot create temp directory
     */
    private static File createTempFile(JarEntryInfo inf) throws LibraryHelperException {
        if (dirTemp == null) {
            File dir = new File(System.getProperty("java.io.tmpdir"), TMP_SUB_DIRECTORY);
            if (!dir.exists()) {
                if (!dir.mkdir()) {
                    throw new LibraryHelperException("Cannot create temp directory " + dir.getAbsolutePath());
                }
            }

            // Unix - allow temp directory RW access to all users.
            dir.setReadable(true, false);
            dir.setWritable(true, false);
            dir.setExecutable(true, false); // Unix: allow content for dir, redundant for file

            if (!dir.exists() || !dir.isDirectory()) {
                throw new LibraryHelperException("Cannot create temp directory " + dir.getAbsolutePath());
            }
            dirTemp = dir;
        }
        File fileTmp = null;
        try {
            if (LOADED_LIBS.get(inf.getName().substring(inf.getName().lastIndexOf('/') + 1, inf.getName().length())) != null) {
                return new File(dirTemp + File.separator + inf.getName());
            }

            if (inf.getName().lastIndexOf('/') != -1) {
                fileTmp = new File(dirTemp + File.separator + inf.getName().substring(0, inf.getName().lastIndexOf('/')));
                fileTmp.mkdirs();
                fileTmp.deleteOnExit();
            }

            fileTmp = new File(dirTemp + File.separator + inf.getName());
            fileTmp.deleteOnExit();

            // Unix - allow temp file deletion by any user
            fileTmp.setReadable(true, false);
            fileTmp.setWritable(true, false);
            fileTmp.setExecutable(true, false); // Unix: allow content for dir, redundant for file

            byte[] a_by = inf.getJarBytes();
            BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(fileTmp));
            os.write(a_by);
            os.close();
            return fileTmp;
        } catch (IOException e) {
            throw new LibraryHelperException(String.format("Cannot create temp file '%s' for %s", fileTmp, inf.jarEntry), e);
        }
    }

    /**
     * Remove all temporary files
     */
    private static void shutdown() {
        for (JarFileInfo jarFileInfo : lstJarFile) {
            try {
                jarFileInfo.jarFile.close();
            } catch (IOException e) {
                // Ignore. In the worst case temp files will accumulate.
            }
            if (jarFileInfo.jarFileInfoParent != null) {
                File file = jarFileInfo.file;
                if (!file.delete()) {
                    System.out.println("Cannot delete temp file -> " + file.getAbsolutePath());
                } else {
                    System.out.println("Temp file " + file.getAbsolutePath() + " was deleted.");
                }
            }
        }
    }

    /**
     * Inner class with JAR file information.
     */
    @SuppressWarnings("unused")
    private static class JarFileInfo {
        JarFile jarFile;
        File file;
        JarFileInfo jarFileInfoParent;
        String simpleName;
        Manifest mf; // required for package creation
        JarFileInfo(String simpleName, File file, JarFileInfo jarFileParent) throws IOException {
            this.jarFile = new JarFile(file);
            this.file = file;
            this.jarFileInfoParent = jarFileParent;
            this.simpleName = (jarFileParent == null ? "" : jarFileParent.simpleName + "!") + simpleName;
            try {
                this.mf = jarFile.getManifest();
            } catch (IOException e) {
                // Manifest does not exist or not available
                this.mf = new Manifest();
            }
        }
        String getSpecificationTitle() {
            return mf.getMainAttributes().getValue(Name.SPECIFICATION_TITLE);
        }
        String getSpecificationVersion() {
            return mf.getMainAttributes().getValue(Name.SPECIFICATION_VERSION);
        }
        String getSpecificationVendor() {
            return mf.getMainAttributes().getValue(Name.SPECIFICATION_VENDOR);
        }
        String getImplementationTitle() {
            return mf.getMainAttributes().getValue(Name.IMPLEMENTATION_TITLE);
        }
        String getImplementationVersion() {
            return mf.getMainAttributes().getValue(Name.IMPLEMENTATION_VERSION);
        }
        String getImplementationVendor() {
            return mf.getMainAttributes().getValue(Name.IMPLEMENTATION_VENDOR);
        }
        URL getSealURL() {
            String seal = mf.getMainAttributes().getValue(Name.SEALED);
            if (seal != null) {
                try {
                    return new URL(seal);
                } catch (MalformedURLException e) {
                    // Ignore, will return null
                }
            }
            return null;
        }
    }

    /**
     * Inner class with JAR entry information. Keeps JAR file and entry object.
     */
    private static class JarEntryInfo {
        JarFileInfo jarFileInfo;
        JarEntry jarEntry;
        JarEntryInfo(JarFileInfo jarFileInfo, JarEntry jarEntry) {
            this.jarFileInfo = jarFileInfo;
            this.jarEntry = jarEntry;
        }
        String getName() {             // used in createTempFile() and loadJar()
            return jarEntry.getName(); //.replace('/', '_');
        }
        @Override
        public String toString() {
            return "JAR: " + jarFileInfo.jarFile.getName() + " ENTRY: " + jarEntry;
        }

        /**
         * Read JAR entry and returns byte array of this JAR entry. This is
         * a helper method to load JAR entry into temporary file. 
         * 
         * @param inf JAR entry information object
         * @return byte array for the specified JAR entry
         * @throws JarClassLoaderException
         */
        byte[] getJarBytes() throws LibraryHelperException {
            DataInputStream dis = null;
            byte[] a_by = null;
            try {
                long lSize = jarEntry.getSize();
                if (lSize <= 0 || lSize >= Integer.MAX_VALUE) {
                    throw new LibraryHelperException("Invalid size " + lSize + " for entry " + jarEntry);
                }
                a_by = new byte[(int)lSize];
                InputStream is = jarFileInfo.jarFile.getInputStream(jarEntry);
                dis = new DataInputStream(is);
                dis.readFully(a_by);
            } catch (IOException e) {
                throw new LibraryHelperException(null, e);
            } finally {
                if (dis != null) {
                    try {
                        dis.close();
                    } catch (IOException e) {
                    }
                }
            }
            return a_by;
        } // getJarBytes()
    }     // inner class JarEntryInfo

    /**
     * Inner class to handle JarClassLoader exceptions.  
     */
    @SuppressWarnings("serial")
    private static class LibraryHelperException extends Exception {
        LibraryHelperException(String sMsg) {
            super(sMsg);
        }
        LibraryHelperException(String sMsg, Throwable eCause) {
            super(sMsg, eCause);
        }

        String getMessageAll() {
            StringBuilder sb = new StringBuilder();
            for (Throwable e = this; e != null; e = e.getCause()) {
                if (sb.length() > 0) {
                    sb.append(" / ");
                }
                String sMsg = e.getMessage();
                if (sMsg == null || sMsg.length() == 0) {
                    sMsg = e.getClass().getSimpleName();
                }
                sb.append(sMsg);
            }
            return sb.toString();
        }
    }
}
