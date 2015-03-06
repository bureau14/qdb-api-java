package com.b14.qdb.hadoop.mapreduce.tools;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;
 
public class FixHadoopOnWindows {
    
    /**
     * Fix the followind Hadoop problem on Windows:
     * 1) mapReduceLayer.Launcher: Backend error message during job submission java.io.IOException: Failed to set permissions of path: \tmp\hadoop-MyUsername\mapred\staging\
     * 2) java.io.IOException: Failed to set permissions of path: bla-bla-bla\.staging to 0700
     */
 
    public static void runFix() throws NotFoundException, CannotCompileException {
        if (isWindows()) { // run fix only on Windows
            setUpSystemVariables();
            fixCheckReturnValueMethod();
        }
    }
 
    // set up correct temporary directory on windows
    private static void setUpSystemVariables() {
        System.getProperties().setProperty("java.io.tmpdir", "D:/TMP/");
    }
 
    /**
     * org.apache.hadoop.fs.FileUtil#checkReturnValue doesn't work on Windows at all => so, let's change method body with Javassist on empty body
     * @throws NotFoundException 
     */
    private static void fixCheckReturnValueMethod() throws NotFoundException {
        ClassPool cp = new ClassPool(true);
        try {
            CtClass ctClass = cp.get("org.apache.hadoop.fs.FileUtil");
            CtMethod ctMethod = ctClass.getDeclaredMethod("checkReturnValue");
            ctMethod.setBody("{  }");
            CtMethod ctMethod2 = ctClass.getDeclaredMethod("execCommand");
            ctMethod2.setBody("{ return \"-rwxrwxrwx+    1 username groupname\"; }");
            ctClass.toClass();
        } catch (CannotCompileException e) {
        }
        
        CtClass ctClass2 = cp.get("org.apache.commons.io.FileUtils");
        boolean found = false;
        for (CtMethod method : ctClass2.getMethods()) {
            if ("isSymlink".equals(method.getName())) {
                found = true;
                break;
            }
        }
        if (!found) {
            try {
                CtMethod ctMethod3 = CtNewMethod.make("public static boolean isSymlink(java.io.File file) { return false; }", ctClass2);
                ctClass2.addMethod(ctMethod3);
                ctClass2.toClass();
            } catch (CannotCompileException e) {
            } 
        }
    }
 
    public static boolean isWindows() {
        String OS = System.getProperty("os.name");
        return OS.startsWith("Windows");
    }
 
    private FixHadoopOnWindows() { }
}