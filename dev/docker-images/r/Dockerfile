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

# Install R and SparkR dependencies
RUN $CONDA_BIN install --yes --channel r \
    r-base=3.4.3 r-knitr r-rmarkdown r-e1071 r-survival r-testthat r-lintr r-roxygen2 r-devtools

# Install qpdf (used for SparkR documentation, required in R tests)
USER root
RUN apt-get update && apt-get install qpdf
USER circleci

# Add conda-installed R to path
ENV PATH="$CONDA_ROOT/bin:$PATH"
ENV R_HOME=$CONDA_ROOT/lib/R
