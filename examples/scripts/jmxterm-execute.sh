#
# Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3 (only) of the
# License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# This program may also be available under different license terms. For more
# information, see www.akiban.com or contact licensing@akiban.com.
#

#
# Invokes the com.persisit.ManagementMXBean#execute method using jmxterm.  See
# http://www.cyclopsgroup.org/projects/jmxterm/.
#
java -jar jmxterm-1.0-alpha-4-uber.jar --verbose silent --noninteract --url $1 <<EOF
run -d com.persistit -b com.persistit:type=Persistit execute $2
EOF


