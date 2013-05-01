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

package com.persistit.ui.renderers;

import com.persistit.ui.AdminUI;
import com.persistit.ui.ManagementTableModel.AbstractCustomTableCellRenderer;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class TaskStatusStateRenderer extends AbstractCustomTableCellRenderer {
    private AdminUI _adminUI;

    @Override
    public void setup(final AdminUI ui, final Class baseClass, final String columnSpec) {
        _adminUI = ui;
    }

    @Override
    public void setValue(final Object value) {
        String text = "?";
        if (value instanceof Integer) {
            text = _adminUI.getTaskStateString(((Integer) value).intValue());
        }
        setText(text);
    }

}
