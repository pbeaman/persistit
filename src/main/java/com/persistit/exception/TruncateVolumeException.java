/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
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

/**
 * Thrown when an application attempts to truncate a
 * {@link com.persistit.Volume} that was opened without the create or createOnly
 * attribute.
 * 
 * @version 1.0
 */
public class TruncateVolumeException extends PersistitException {

    private static final long serialVersionUID = 7942773559963360102L;

    public TruncateVolumeException() {
        super();
    }

    public TruncateVolumeException(final String msg) {
        super(msg);
    }
}
