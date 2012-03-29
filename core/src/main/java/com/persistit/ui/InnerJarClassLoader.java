/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit.ui;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

/**
 * This is just enough of a ClassLoader implementation to load the JavaHelp
 * classes from a nested Jar file. The purpose here is to allow jhbasic.jar to
 * reside unchanged (due to license constraints) inside the
 * persistit_jsa110_ui.jar file. When the user clicks on Help the first time,
 * AdminUI constructs an InnerJarClassLoader and calls addJar to load the
 * contents of inner jar files. The findClass and getResourceAsStream methods
 * then look up the loaded resources when called. This class keeps a Map
 * containing all the resources packed in the JAR files, so it is good to unload
 * this from memory when possible.
 * 
 * @author Peter Beaman
 * @version 1.0
 */
class InnerJarClassLoader extends ClassLoader {
    HashMap _resourceMap = new HashMap();

    InnerJarClassLoader(ClassLoader parent) throws IOException {
        super(parent);
    }

    public void addJar(String jarName) throws IOException {
        addJar(getClass().getClassLoader().getResourceAsStream(jarName));
    }

    public void addJar(InputStream is) throws IOException {
        JarInputStream jis = new JarInputStream(new BufferedInputStream(is, 32768));

        byte[] hbytes = new byte[65536];
        for (;;) {
            ZipEntry entry = jis.getNextEntry();
            if (entry == null)
                break;
            String name = entry.getName();
            int offset = 0;
            for (;;) {
                int length = jis.read(hbytes, offset, hbytes.length - offset);
                if (length == -1)
                    break;
                offset += length;
                if (offset == hbytes.length) {
                    byte[] temp = new byte[hbytes.length + 65536];
                    System.arraycopy(hbytes, 0, temp, 0, offset);
                    hbytes = temp;
                }
            }
            byte[] bytes = new byte[offset];
            System.arraycopy(hbytes, 0, bytes, 0, offset);
            _resourceMap.put(name, bytes);
            jis.closeEntry();
        }
    }

    @Override
    public Class findClass(String className) throws ClassNotFoundException {
        String fileName = className.replace('.', '/') + ".class";
        byte[] bytes = (byte[]) _resourceMap.get(fileName);
        if (bytes != null) {
            return defineClass(className, bytes, 0, bytes.length);
        } else
            throw new ClassNotFoundException(className);
    }

    @Override
    public InputStream getResourceAsStream(String name) {
        byte[] bytes = (byte[]) _resourceMap.get(name);
        if (bytes != null)
            return new ByteArrayInputStream(bytes);
        else
            return null;
    }

}
