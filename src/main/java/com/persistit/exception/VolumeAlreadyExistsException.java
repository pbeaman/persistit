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

/**
 * Thrown when the Persistit configuration specifies the <code>createOnly</code>
 * flag for a volume that already exists, or when a the configuration specifies
 * a page size for a volume that does not the page size of an existing volume.
 * 
 * @version 1.0
 */
public class VolumeAlreadyExistsException extends PersistitException {
    private static final long serialVersionUID = -5943311415307074464L;

    public VolumeAlreadyExistsException() {
        super();
    }

    public VolumeAlreadyExistsException(String msg) {
        super(msg);
    }
}
