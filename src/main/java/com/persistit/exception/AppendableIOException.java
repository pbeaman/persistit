/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
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

    public AppendableIOException(IOException ioe) {
        super(ioe);
    }

}
