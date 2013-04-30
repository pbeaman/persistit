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
 * Thrown by {@link com.persistit.Volume} if the volume file has a different
 * internal ID value than expected. This condition can signify that a volume
 * file has been renamed or the wrong file has been restored to a configuration.
 * 
 */
public class WrongVolumeException extends PersistitException {
    private static final long serialVersionUID = 9119544306031815864L;

    public WrongVolumeException() {
        super();
    }

    public WrongVolumeException(final String msg) {
        super(msg);
    }

}
