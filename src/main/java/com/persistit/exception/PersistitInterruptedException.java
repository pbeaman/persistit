/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
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

/**
 * This is a wrapper for an {@link InterruptedException}. If a Persistit thread
 * is interrupted when sleeping or waiting, it throws an instance of this
 * Exception. This allows the caller of a Persistit method to catch
 * {@link PersistitException}s without also needing to catch
 * InterruptedExceptions.
 * <p />
 * Before throwing a PersistitInterruptedException Persistit reasserts the
 * <code>interrupted</code> status of the current Thread so that a subsequent
 * call to sleep or wait will once again be interrupted.
 * 
 * @version 1.0
 */
public class PersistitInterruptedException extends PersistitException {
    private static final long serialVersionUID = 9108205596568958490L;

    public PersistitInterruptedException(InterruptedException ioe) {
        super(ioe);
        Thread.currentThread().interrupt();
    }

}

