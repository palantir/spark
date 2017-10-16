#!/usr/bin/env bash
#

# A library to simplify using the SBT launcher from other packages.
# Note: This should be used by tools like giter8/conscript etc.

# TODO - Should we merge the main SBT script with this library?

if test -z "$HOME"; then
  declare -r script_dir="$(dirname "$script_path")"
else
  declare -r script_dir="$HOME/.sbt"
fi

declare -a residual_args
declare -a java_args
declare -a scalac_args
declare -a sbt_commands
declare -a maven_profiles

if test -x "$JAVA_HOME/bin/java"; then
    echo -e "Using $JAVA_HOME as default JAVA_HOME."
    echo "Note, this will be overridden by -java-home if it is set."
    declare java_cmd="$JAVA_HOME/bin/java"
else
    declare java_cmd=java
fi

echoerr () {
  echo 1>&2 "$@"
}
vlog () {
  [[ $verbose || $debug ]] && echoerr "$@"
}
dlog () {
  [[ $debug ]] && echoerr "$@"
}

acquire_sbt_jar () {
  SBT_VERSION=`awk -F "=" '/sbt\.version/ {print $2}' ./project/build.properties`
  URL1=https://dl.bintray.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/${SBT_VERSION}/sbt-launch.jar
  JAR=build/sbt-launch-${SBT_VERSION}.jar

  sbt_jar=$JAR

  if [[ ! -f "$sbt_jar" ]]; then
    # Download sbt launch jar if it hasn't been downloaded yet
    if [ ! -f "${JAR}" ]; then
    # Download
    printf "Attempting to fetch sbt\n"
    JAR_DL="${JAR}.part"
    if [ $(command -v curl) ]; then
      curl --fail --location --silent ${URL1} > "${JAR_DL}" &&\
        mv "${JAR_DL}" "${JAR}"
    elif [ $(command -v wget) ]; then
      wget --quiet ${URL1} -O "${JAR_DL}" &&\
        mv "${JAR_DL}" "${JAR}"
    else
      printf "You do not have curl or wget installed, please install sbt manually from http://www.scala-sbt.org/\n"
      exit -1
    fi
    fi
    if [ ! -f "${JAR}" ]; then
    # We failed to download
    printf "Our attempt to download sbt locally to ${JAR} failed. Please install sbt manually from http://www.scala-sbt.org/\n"
    exit -1
    fi
    printf "Launching sbt from ${JAR}\n"
  fi
}

execRunner () {
  # print the arguments one to a line, quoting any containing spaces
  [[ $verbose || $debug ]] && echo "# Executing command line:" && {
    for arg; do
      if printf "%s\n" "$arg" | grep -q ' '; then
        printf "\"%s\"\n" "$arg"
      else
        printf "%s\n" "$arg"
      fi
    done
    echo ""
  }

  "$@"
}

addJava () {
  dlog "[addJava] arg = '$1'"
  java_args=( "${java_args[@]}" "$1" )
}

enableProfile () {
  dlog "[enableProfile] arg = '$1'"
  maven_profiles=( "${maven_profiles[@]}" "$1" )
  export SBT_MAVEN_PROFILES="${maven_profiles[@]}"
}

addSbt () {
  dlog "[addSbt] arg = '$1'"
  sbt_commands=( "${sbt_commands[@]}" "$1" )
}
addResidual () {
  dlog "[residual] arg = '$1'"
  residual_args=( "${residual_args[@]}" "$1" )
}
addDebugger () {
  addJava "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=$1"
}

get_mem_opts () {
  # if we detect any of these settings in ${JAVA_OPTS} or ${JAVA_TOOL_OPTIONS} we need to NOT output our settings.
  # The reason is the Xms/Xmx, if they don't line up, cause errors.
  if [[ "${JAVA_OPTS}" == *-Xmx* ]] || [[ "${JAVA_OPTS}" == *-Xms* ]] || [[ "${JAVA_OPTS}" == *-XX:MaxPermSize* ]] || [[ "${JAVA_OPTS}" == *-XX:MaxMetaspaceSize* ]] || [[ "${JAVA_OPTS}" == *-XX:ReservedCodeCacheSize* ]]; then
    echo ""
  elif [[ "${JAVA_TOOL_OPTIONS}" == *-Xmx* ]] || [[ "${JAVA_TOOL_OPTIONS}" == *-Xms* ]] || [[ "${JAVA_TOOL_OPTIONS}" == *-XX:MaxPermSize* ]] || [[ "${JAVA_TOOL_OPTIONS}" == *-XX:MaxMetaspaceSize* ]] || [[ "${JAVA_TOOL_OPTIONS}" == *-XX:ReservedCodeCacheSize* ]]; then
    echo ""
  elif [[ "${SBT_OPTS}" == *-Xmx* ]] || [[ "${SBT_OPTS}" == *-Xms* ]] || [[ "${SBT_OPTS}" == *-XX:MaxPermSize* ]] || [[ "${SBT_OPTS}" == *-XX:MaxMetaspaceSize* ]] || [[ "${SBT_OPTS}" == *-XX:ReservedCodeCacheSize* ]]; then
    echo ""
  else
    # a ham-fisted attempt to move some memory settings in concert
    # so they need not be messed around with individually.
    local mem=${1:-2048}
    local codecache=$(( $mem / 8 ))
    (( $codecache > 128 )) || codecache=128
    (( $codecache < 512 )) || codecache=512
    local class_metadata_size=$(( $codecache * 2 ))
    local class_metadata_opt=$([[ "$java_version" < "1.8" ]] && echo "MaxPermSize" || echo "MaxMetaspaceSize")

    local arg_xms=$([[ "${java_args[@]}" == *-Xms* ]] && echo "" || echo "-Xms${mem}m")
    local arg_xmx=$([[ "${java_args[@]}" == *-Xmx* ]] && echo "" || echo "-Xmx${mem}m")
    local arg_rccs=$([[ "${java_args[@]}" == *-XX:ReservedCodeCacheSize* ]] && echo "" || echo "-XX:ReservedCodeCacheSize=${codecache}m")
    local arg_meta=$([[ "${java_args[@]}" == *-XX:${class_metadata_opt}* && ! "$java_version" < "1.8" ]] && echo "" || echo "-XX:${class_metadata_opt}=${class_metadata_size}m")

    echo "${arg_xms} ${arg_xmx} ${arg_rccs} ${arg_meta} -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSPermGenSweepingEnabled"
  fi
}

require_arg () {
  local type="$1"
  local opt="$2"
  local arg="$3"
  if [[ -z "$arg" ]] || [[ "${arg:0:1}" == "-" ]]; then
    echo "$opt requires <$type> argument" 1>&2
    exit 1
  fi
}

is_function_defined() {
  declare -f "$1" > /dev/null
}

process_args () {
  while [[ $# -gt 0 ]]; do
    case "$1" in
       -h|-help) usage; exit 1 ;;
    -v|-verbose) verbose=1 && shift ;;
      -d|-debug) debug=1 && shift ;;

           -ivy) require_arg path "$1" "$2" && addJava "-Dsbt.ivy.home=$2" && shift 2 ;;
           -mem) require_arg integer "$1" "$2" && sbt_mem="$2" && shift 2 ;;
     -jvm-debug) require_arg port "$1" "$2" && addDebugger $2 && shift 2 ;;
         -batch) exec </dev/null && shift ;;

       -sbt-jar) require_arg path "$1" "$2" && sbt_jar="$2" && shift 2 ;;
   -sbt-version) require_arg version "$1" "$2" && sbt_version="$2" && shift 2 ;;
     -java-home) require_arg path "$1" "$2" && java_cmd="$2/bin/java" && export JAVA_HOME=$2 && shift 2 ;;

            -D*) addJava "$1" && shift ;;
            -J*) addJava "${1:2}" && shift ;;
            -P*) enableProfile "$1" && shift ;;
              *) addResidual "$1" && shift ;;
    esac
  done

  is_function_defined process_my_args && {
    myargs=("${residual_args[@]}")
    residual_args=()
    process_my_args "${myargs[@]}"
  }
}

run() {
  # no jar? download it.
  [[ -f "$sbt_jar" ]] || acquire_sbt_jar "$sbt_version" || {
    # still no jar? uh-oh.
    echo "Download failed. Obtain the sbt-launch.jar manually and place it at $sbt_jar"
    exit 1
  }

  # process the combined args, then reset "$@" to the residuals
  process_args "$@"
  set -- "${residual_args[@]}"
  argumentCount=$#

  # run sbt
  execRunner "$java_cmd" \
    ${SBT_OPTS:-$default_sbt_opts} \
    $(get_mem_opts $sbt_mem) \
    ${java_opts} \
    ${java_args[@]} \
    -jar "$sbt_jar" \
    "${sbt_commands[@]}" \
    "${residual_args[@]}"
}
