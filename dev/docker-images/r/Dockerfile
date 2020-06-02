#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM palantirtechnologies/circle-spark-base:0.2.3

USER root

# We install r-base-dev via apt-get, and then r-base via Conda. We can't do both via apt-get, because we need older R
# versions and CRAN's Ubuntu channels don't satisfy old dependencies. We can't do both via Conda, because "official"
# channels (anaconda and forge) don't have dev builds of R. Hence this suboptimal split. The versions don't even match.

# Install r-base-dev to get shared libraries to compile R packages
# Also install qpdf (rendering of SparkR docs), and third-party dependencies libcurl and libgit
RUN apt-get update && apt-get install r-base-dev qpdf libcurl4-openssl-dev libgit2-dev
# Install R via Conda so we can control the R version
RUN $CONDA_BIN install --yes r-base=3.4.3 gxx_linux-64
ENV R_HOME=$CONDA_ROOT/lib/R

# Need gxx_linux-64 binaries on the path to compile R packages
ENV PATH="$CONDA_ROOT/bin:$PATH"

# Install SparkR dependencies (find list in R/pkg/DESCRIPTION)
# Need to provide path to pkgconfig otherwise the installer gets confused between the system pkgconfig and the Conda pkgconfig
RUN R -e "install.packages(c('knitr', 'rmarkdown', 'e1071', 'survival'), repos='https://cloud.r-project.org', configure.vars='PKG_CONFIG_PATH=$CONDA_ROOT/lib/pkgconfig')"

# Install build dependencies
# Need to install 'curl' separately
RUN R -e "install.packages('curl', repos='https://cloud.r-project.org', configure.vars='PKG_CONFIG_PATH=$CONDA_ROOT/lib/pkgconfig')"
RUN R -e "install.packages(c('devtools', 'roxygen2', 'lintr', 'testthat'), repos='https://cloud.r-project.org', configure.vars='PKG_CONFIG_PATH=$CONDA_ROOT/lib/pkgconfig')"

USER circleci
