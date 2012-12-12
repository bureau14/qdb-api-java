/**
 * Copyright (c) 2009-2011, Bureau 14 SARL
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
 *    * Neither the name of the University of California, Berkeley nor the
 *      names of its contributors may be used to endorse or promote products
 *      derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY BUREAU 14 AND CONTRIBUTORS ``AS IS'' AND ANY
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

package com.b14.tests.tools;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.databene.contiperf.ExecutionLogger;
import org.databene.contiperf.util.ContiPerfUtil;

public class CustomContiperfFileExecutionLogger implements ExecutionLogger {
    private static final String FILENAME = "target/contiperf/contiperf.log";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final String SEPARATOR = ";";
    private static boolean firstCall = true;

    public CustomContiperfFileExecutionLogger() {
        if (firstCall) {
            createSummaryFile();
            firstCall = false;
        }
    }

    private void createSummaryFile() {
        File file = new File(".", FILENAME);
        try {
            ensureDirectoryExists(file.getParentFile());
            if (file.exists()) {
                file.delete();
            }   
        } catch (FileNotFoundException e) {
            System.out.println("Unable to create directory: " + file.getAbsolutePath());
        }
    }

    private void ensureDirectoryExists(File dir) throws FileNotFoundException {
        File parent = dir.getParentFile();
        if (!dir.exists()) {
            if (parent == null) {
                throw new FileNotFoundException();
            }
            ensureDirectoryExists(parent);
            dir.mkdir();
        }
    }

    public void logInvocation(String id, int latency, long startTime) {
        System.out.println(id + SEPARATOR + latency + SEPARATOR + startTime);
    }

    public void logSummary(String id, long elapsedTime, long invocationCount, long startTime) {
        OutputStream out = null;
        long jvmTotalMemory = Runtime.getRuntime().totalMemory();
        long jvmTotalFreeMemory = Runtime.getRuntime().freeMemory();
        long osProcs = Runtime.getRuntime().availableProcessors();
        long osCPU = 0;
        long osTotalMemory = 0;
        long osTotalFreeMemory = 0;
        long osTotalSwap = 0;
        long osTotalFreeSwap = 0;

        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (method.getName().startsWith("get") && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (Exception e) {
                    value = e;
                }
                if (method.getName().equals("getAvailableProcessors")) {
                    osProcs = Long.parseLong(value.toString());
                }
                if (method.getName().equals("getProcessCpuTime")) {
                    osCPU = Long.parseLong(value.toString());
                }
                if (method.getName().equals("getTotalPhysicalMemorySize")) {
                    osTotalMemory = Long.parseLong(value.toString());
                }
                if (method.getName().equals("getFreePhysicalMemorySize")) {
                    osTotalFreeMemory = Long.parseLong(value.toString());
                }
                if (method.getName().equals("getFreeSwapSpaceSize")) {
                    osTotalFreeSwap = Long.parseLong(value.toString());
                }
                if (method.getName().equals("getTotalSwapSpaceSize")) {
                    osTotalSwap = Long.parseLong(value.toString());
                }
            }
        }
        
        String message =    id + SEPARATOR + elapsedTime + SEPARATOR + invocationCount + SEPARATOR + startTime + SEPARATOR +
                            + jvmTotalFreeMemory + SEPARATOR + jvmTotalMemory + SEPARATOR +
                            + osTotalFreeMemory + SEPARATOR + osTotalMemory + SEPARATOR +
                            + osTotalFreeSwap + SEPARATOR + osTotalSwap + SEPARATOR +
                            + osCPU + SEPARATOR + osProcs
                            + LINE_SEPARATOR;
        try {
            out = new FileOutputStream(FILENAME, true);
            out.write(message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            ContiPerfUtil.close(out);
        }
    }
}
