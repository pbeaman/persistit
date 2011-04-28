#/bin/sh
#
# Copyright (C) 2011 Akiban Technologies Inc.
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
#
# Invokes the com.persisit.ManagementMXBean#execute method using jmxterm.  See
# http://www.cyclopsgroup.org/projects/jmxterm/.
#
java -jar jmxterm-1.0-alpha-4-uber.jar --verbose silent --noninteract --url $1 <<EOF
run -d com.persistit -b com.persistit:type=Persistit execute $2
EOF


