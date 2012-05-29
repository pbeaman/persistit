/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
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

    public PersistitIOException(IOException ioe) {
        super(ioe);
        _detail = null;
    }

    public PersistitIOException(String msg) {
        super(msg);
        _detail = null;
    }

    public PersistitIOException(String msg, IOException exception) {
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
