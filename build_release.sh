#!/bin/bash
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
# Build build artifacts associated with a release
#   - Documentation (apidocs and sphinx html, for website)
#       - akiban-persistit-X.X.X-website-docs.tar.gz
#   - Bundles/packages
#       - akiban-persistit-X.X.X.zip                (binary, AGPL)
#       - akiban-persistit-X.X.X.tar.gz             (binary, AGPL)
#       - akiban-persistit-X.X.X-source.zip         (source, AGPL)
#       - akiban-persistit-X.X.X-source.tar.gz      (source, AGPL)
#       - akiban-persistit-community-X.X.X.zip      (binary, EULA)
#       - akiban-persistit-community-X.X.X.tar.gz   (binary, EULA)
#

set -e

REQUIRED_PROGS="bzr mvn javac sphinx-build curl awk sed tr zip tar"
BRANCH_DEF="lp:~akiban-technologies/akiban-persistit"
COMMUNITY_LICENSE_URL="http://www.akiban.com/akiban-persistit-community-license-agreement-plaintext"

VERSION=""
BRANCH=""
WORKSPACE="/tmp/persistit_release"

while getopts "hb:v:w:" FLAG; do
    case "${FLAG}" in
        h) ;;
        b) BRANCH="${OPTARG}" ;;
        v) VERSION="${OPTARG}" ;;
        w) WORKSPACE="${OPTARG}" ;;
        *) echo "Unhandled option" 1>&2 ; exit 1 ;;
    esac
done

if [ "${VERSION}" = "" ]; then
    echo "Missing required version arg -v" 1>&2
    exit 1
fi

if [ "${BRANCH}" = "" ]; then
    BRANCH="${BRANCH_DEF}/${VERSION}"
fi

echo "Build packages for version: ${VERSION}"
echo "Use source branch: ${BRANCH}"
echo "Use workspace location: ${WORKSPACE}"

echo "Checking for required programs"
for PROG in ${REQUIRED_PROGS}; do
    if [ "$(which ${PROG})" = "" ]; then
        echo "    ${PROG} not found in PATH" 1>&2
        exit 1
    fi
done

echo "Cleaning workspace ${WORKSPACE}"
rm -rf "${WORKSPACE}"
mkdir -p "${WORKSPACE}"
cd "${WORKSPACE}"

echo "Fetching revision number"
REVNO=$(bzr revno -q "${BRANCH}")
echo "Revision $REVNO"

echo "Exporting branch"
bzr export -q "${VERSION}" "${BRANCH}"

echo "Making package directories"
NAME_PREFIX="akiban-persistit"
OPEN_NAME="${NAME_PREFIX}-${VERSION}"
COMMUNITY_NAME="${NAME_PREFIX}-community-${VERSION}"
cp -r "${VERSION}" "${OPEN_NAME}-source"
cp -r "${VERSION}" "${OPEN_NAME}"
rm -r "${OPEN_NAME}/doc"
mkdir "${OPEN_NAME}/doc"
rm -r "${OPEN_NAME}/examples/scripts"
rm -r "${OPEN_NAME}/src"


echo "Compiling and packaging"
cd "${VERSION}"
mvn -DBZR_REVISION="${REVNO}" compile test-compile package -DskipTests=true >/dev/null

echo "Building javadoc"
mvn javadoc:javadoc >/dev/null

echo "Building docs (for packages)"
cd doc/build
APIDOC_URL="../apidocs" bash -e  build-doc.sh >/dev/null
cd ../../..

echo "Copying docs"
cp -r "${VERSION}/target/site/apidocs" "${OPEN_NAME}/doc"
cp -r "${VERSION}/target/sphinx/html" "${OPEN_NAME}/doc"
rm -r "${OPEN_NAME}/doc/html/.buildinfo"
rm -r "${OPEN_NAME}/doc/html/.doctrees"

echo "Copying jars"
cp "${VERSION}/target/${NAME_PREFIX}-${VERSION}${REVNO}.jar" "${OPEN_NAME}/${NAME_PREFIX}-${VERSION}.jar"
cp -r "${OPEN_NAME}" "${COMMUNITY_NAME}"
cp "${VERSION}/target/${NAME_PREFIX}-${VERSION}${REVNO}-sources.jar" "${OPEN_NAME}/${NAME_PREFIX}-${VERSION}-sources.jar"

echo "Downloading and formating community license"
curl -s "${COMMUNITY_LICENSE_URL}" |
    # Pull out the content between the two regexes, excluding the matches themselves
    awk '/<div class="content">/ {flag=1;next} /<\/div>/ {flag=0} flag {print}' |
    # Replace paragraph end marks for the first 4 paragraphs with newlines
    awk '{if(NR < 8) sub(/<\/p>/, "\n"); print }' |
    # Delete all: <p>, </p>, </div>, and &nbsp; occurrences
    sed -e 's/<p>//g' -e 's/<\/p>//g' -e 's/<\/div>//g' -e 's/&nbsp;//g' |
    # Replace unicode quotes with simple ones
    sed -e 's/[“”]/"/g' -e "s/’/'/g" |
    # Un-link email address(es)
    sed -e 's/<a href=".*">//g' -e 's/<\/a>//g' |
    # Collapse repeated spaces
    tr -s ' ' |
    # Wrap nicely at 80 characters 
    fold -s \
    > "${COMMUNITY_NAME}/LICENSE.txt"

echo "Creating zip and tar.gz files"
zip -r "${OPEN_NAME}.zip" "${OPEN_NAME}" >/dev/null
zip -r "${OPEN_NAME}-source.zip" "${OPEN_NAME}-source" >/dev/null
zip -r "${COMMUNITY_NAME}.zip" "${COMMUNITY_NAME}" >/dev/null
tar czf "${OPEN_NAME}.tar.gz" "${OPEN_NAME}"
tar czf "${OPEN_NAME}-source.tar.gz" "${OPEN_NAME}-source"
tar czf "${COMMUNITY_NAME}.tar.gz" "${COMMUNITY_NAME}"

echo "Building docs for website"
cd "${VERSION}"
rm -r "target/sphinx"
cd doc/build
bash -e build-doc.sh >/dev/null
cd ../../../
WEBSITE_DOCS="${NAME_PREFIX}-${VERSION}-website-docs"
mkdir "${WEBSITE_DOCS}"
cp -r "${VERSION}/target/site/apidocs" "${WEBSITE_DOCS}"
cp -r "${VERSION}/target/sphinx/html" "${WEBSITE_DOCS}"
rm -r "${WEBSITE_DOCS}/html/.buildinfo"
rm -r "${WEBSITE_DOCS}/html/.doctrees"
tar czf "${WEBSITE_DOCS}.tar.gz" "${WEBSITE_DOCS}"

echo "Signing files for Launchpad upload"
FILES_TO_SIGN="${OPEN_NAME}.zip ${OPEN_NAME}-source.zip ${OPEN_NAME}.tar.gz ${OPEN_NAME}-source.tar.gz"
for FILE in ${FILES_TO_SIGN}; do
    gpg --armor --sign --detach-sig $FILE
done

echo "All output files are in: ${WORKSPACE}"
echo "Done"

