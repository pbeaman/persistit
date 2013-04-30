/**
 * Copyright 2012 Akiban Technologies, Inc.
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

package com.persistit.exception;

import java.io.IOException;

/**
 * Unchecked wrapper for an {@link IOException} used in a context where the
 * {@link Appendable#append} operation throws an IOException. Since most uses of
 * methods that take an <code>Appendable</code> will operate on
 * <code>StringBuilder</code>s, for which an IOException is never thrown, it is
 * desirable for calling code not to have to catch and handle
 * <code>IOException</code>. Therefore any <code>IOException</code> from
 * invoking append on a different <code>Appendable</code> implementation is
 * caught and thrown as this unchecked type.
 * 
 * @version 1.0
 */
public class AppendableIOException extends RuntimeException {

    private static final long serialVersionUID = -2096632389635542578L;

    public AppendableIOException(final IOException ioe) {
        super(ioe);
    }

}
