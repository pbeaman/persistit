#!/bin/bash
#
# Copyright 2012 Akiban Technologies, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# 
# See https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide
#
# Run this script from the base directory.
#
# Package and sign artifacts.  You will need a signing key in your gpg keyring. Follow
# these instructions to set up gpg and generate a key-pair. You only need to do this once:
#
#    https://docs.sonatype.org/display/Repository/How+To+Generate+PGP+Signatures+With+Maven
#
# The gpg:sign will ask you to enter a passphrase to access your key chain.
#
mvn clean javadoc:jar package gpg:sign -Dmaven.test.skip=true
#
# Create a bundle jar.
#
pushd target
jar cvf bundle.jar akiban-persistit*
popd
#
# Note: the Nexus Maven Plugin can do this automatically, but for now we are still manual.
#
echo =======================================================================================================
echo 1. Log in to your account on https://oss.sonatype.org/index.html#stagingRepositories
echo 2. Click \"Staging Upload\"
echo 3. Select Upload Mode \"Artifact Bundle\"
echo 4. Click \"Select Bundle to Upload\"
echo 5. Navigate to the file target/bundle.jar just created
echo 6. Click \"Upload Bundle\"
echo 7. Follow instructions in 
echo
echo    https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-8a.ReleaseIt
echo
echo to close and release the staging repository.
echo =======================================================================================================

