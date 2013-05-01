/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    InnerJarClassLoader(final ClassLoader parent) throws IOException {
        super(parent);
    }

    public void addJar(final String jarName) throws IOException {
        addJar(getClass().getClassLoader().getResourceAsStream(jarName));
    }

    public void addJar(final InputStream is) throws IOException {
        final JarInputStream jis = new JarInputStream(new BufferedInputStream(is, 32768));

        byte[] hbytes = new byte[65536];
        for (;;) {
            final ZipEntry entry = jis.getNextEntry();
            if (entry == null)
                break;
            final String name = entry.getName();
            int offset = 0;
            for (;;) {
                final int length = jis.read(hbytes, offset, hbytes.length - offset);
                if (length == -1)
                    break;
                offset += length;
                if (offset == hbytes.length) {
                    final byte[] temp = new byte[hbytes.length + 65536];
                    System.arraycopy(hbytes, 0, temp, 0, offset);
                    hbytes = temp;
                }
            }
            final byte[] bytes = new byte[offset];
            System.arraycopy(hbytes, 0, bytes, 0, offset);
            _resourceMap.put(name, bytes);
            jis.closeEntry();
        }
    }

    @Override
    public Class findClass(final String className) throws ClassNotFoundException {
        final String fileName = className.replace('.', '/') + ".class";
        final byte[] bytes = (byte[]) _resourceMap.get(fileName);
        if (bytes != null) {
            return defineClass(className, bytes, 0, bytes.length);
        } else
            throw new ClassNotFoundException(className);
    }

    @Override
    public InputStream getResourceAsStream(final String name) {
        final byte[] bytes = (byte[]) _resourceMap.get(name);
        if (bytes != null)
            return new ByteArrayInputStream(bytes);
        else
            return null;
    }

}
