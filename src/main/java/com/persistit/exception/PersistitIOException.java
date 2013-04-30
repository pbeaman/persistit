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

package com.persistit.exception;

import java.io.IOException;

/**
 * This is a wrapper for an {@link IOException}. It is convenient for the caller
 * of a Persistit method to catch {@link PersistitException}s without also
 * needing to catch IOExceptions.
 * 
 * @version 1.0
 */
public class PersistitIOException extends PersistitException {
    private static final long serialVersionUID = 9108205596568958490L;

    private final String _detail;

    public PersistitIOException(final IOException ioe) {
        super(ioe);
        _detail = null;
    }

    public PersistitIOException(final String msg) {
        super(msg);
        _detail = null;
    }

    public PersistitIOException(final String msg, final IOException exception) {
        super(exception);
        _detail = msg;
    }

    /**
     * Override default implementation in {@link Throwable#getMessage()} to
     * return the detail message of the wrapped IOException.
     * 
     * @return the detail message string, including that of the cause
     */
    @Override
    public String getMessage() {
        if (getCause() == null) {
            return super.getMessage();
        } else if (_detail == null) {
            return getCause().toString();
        } else {
            return _detail + ":" + getCause().toString();
        }
    }
}
