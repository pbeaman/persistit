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

/**
 * Thrown during recovery processing when the prewrite journal contains recovery
 * data for one or more Volumes that no longer exist.
 * 
 * @version 1.0
 */
public class RecoveryMissingVolumesException extends PersistitException {
    private static final long serialVersionUID = -9042109367136062128L;

    public RecoveryMissingVolumesException() {
        super();
    }

    public RecoveryMissingVolumesException(final String msg) {
        super(msg);
    }

}
