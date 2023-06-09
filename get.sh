#!/usr/bin/env bash

export TERM=screen-256color
export LANGUAGE=en_US.UTF-8
export LC_ALL=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LANG=en_US.UTF-8

BASE_URL="https://github.com/noctarius/timescaledb-event-streamer/releases"

function available() {
  local ret
  ret="$(command -v "${1}")"
  if ! [ -x "${ret}" ]; then
    echo 0
  else
    echo 1
  fi
}

function arch() {
  local uname
  uname="$(uname -m)"
  case ${uname} in
    x86_64|amd64 )
      if [ "$(getconf LONG_BIT)" == "64" ]; then
        echo "amd64"
        exit 0
      else
        echo "x86"
        exit 0
      fi
    ;;

    i?86|x86 )
      echo "x86"
      exit 0
    ;;

    riscv64 )
      echo "riscv64"
      exit 0
    ;;

    armv* )
      echo "arm"
      exit 0
    ;;

    aarch64 )
      echo "arm64"
      exit 0
    ;;
  esac

  echo "unknown"
}

function os() {
  local os
  os="$(uname)"
  case ${os} in
    Linux )
      echo "linux"
      exit 0
    ;;

    WindowsNT )
      echo "windows"
      exit 0
    ;;

    Darwin )
      echo "darwin"
      exit 0
    ;;

    FreeBSD )
      echo "freebsd"
      exit 0
    ;;
  esac

  echo "unknown"
}

function download() {
  local url="$1"
  local filename="$2"

  if [[ ${cmd_curl} ]]; then
    if ! curl -o "${filename}" --silent -L "${url}"; then
      return 1
    fi
  elif [[ ${cmd_wget} ]]; then
    if ! wget -o "${filename}" "${url}" 2>/dev/null; then
      return 1
    fi
  fi
}

cmd_curl="$(available curl)"
cmd_wget="$(available wget)"
tar="$(available tar)"

echo "Downloader: timescaledb-event-streamer"
echo

echo "Checking tools..."
echo -n "curl: "
if [[ ${cmd_curl} ]]; then
  echo "available"
else
  echo "unavailable"
fi

echo -n "wget: "
if [[ ${cmd_wget} ]]; then
  echo "available"
else
  echo "unavailable"
fi
echo

function latest() {
  local url
  url="${BASE_URL}/latest"

  local ret
  if [[ ${cmd_curl} ]]; then
    ret="$(curl -v ${url} 2>&1 | grep location | perl -n -e '/releases\/tag\/(v[0-9]+.[0-9]+.[0-9]+)/ && print $1')"
  elif [[ ${cmd_wget} ]]; then
    ret="$(wget -S ${url} -O /dev/null 2>&1 | grep Location | grep following | perl -n -e '/releases\/tag\/(v[0-9]+.[0-9]+.[0-9]+)/ && print $1')"
  fi
  echo "${ret}"
}

ARCH=$(arch)
OS=$(os)

echo "Detected architecture: ${ARCH}"
echo "Detected operating system: ${OS}"
echo

if [[ "${ARCH}" == "arm" ]]; then
  if [[ "${OS}" == "darwin" || "${OS}" == "windows" || "${OS}" == "freebsd" ]]; then
    echo "ARM (32 bit) builds isn't available on ${OS}, exiting."
    exit 1
  fi
elif [[ "${ARCH}" == "riscv64" ]]; then
  if [[ "${OS}" == "darwin" || "${OS}" == "windows" || "${OS}" == "freebsd" ]]; then
    echo "RISC-V builds isn't available on ${OS}, exiting."
    exit 1
  fi
elif [[ "${ARCH}" == "unknown" ]]; then
  echo "Unknown architecture, exiting."
  exit 1
elif [[ "${OS}" == "unknown" ]]; then
  echo "Unknown operating system, exiting."
  exit 1
fi

echo -n "Latest version available: "
VERSION=$(latest)
echo "${VERSION}"
echo

# Filename of the latest version
FILENAME="timescaledb-event-streamer-${VERSION}-${OS}-${ARCH}.tar.gz"
CHECKSUM_FILENAME="timescaledb-event-streamer-${VERSION}-checksums.txt"

# Download URL of the latest version
URL="${BASE_URL}/download/${VERSION}/${FILENAME}"
CHECKSUM_URL="${BASE_URL}/download/${VERSION}/${CHECKSUM_FILENAME}"

function exit_trap() {
  echo
  echo -n "Cleanup... "
  rm -f "${CHECKSUM_FILENAME}"
  rm -f "${FILENAME}"
  echo "done."
}
trap exit_trap EXIT

if [ -f timescaledb-event-streamer ]; then
  read -p "Exising version found. Updating? (y/N) " yn
  case ${yn} in
    [yY] )
      echo "Alright, going to update."
      rm timescaledb-event-streamer
    ;;
    * )
      echo "No update, existing."
      exit 1
    ;;
  esac
fi

echo -n "Downloading... "
if ! download "${CHECKSUM_URL}" "${CHECKSUM_FILENAME}"; then
  echo "failed."
  exit 1
fi

if ! download "${URL}" "${FILENAME}"; then
  echo "failed."
  exit 1
fi
echo "done."

echo -n "Testing checksum... "
known_checksum=$(cat "${CHECKSUM_FILENAME}" | grep "${FILENAME}" | awk '{print $1}')
echo -n "${known_checksum}... "
checksum="$(shasum -a 256 "${FILENAME}" | awk '{print $1}')"
echo -n "${checksum}... "
if [[ "${checksum}" != "${known_checksum}" ]]; then
  echo "failed."
  exit 1
fi
echo "done."

echo -n "Extracting... "
tar xzf "${FILENAME}"
echo "done."

echo -n "Ensuring permissions... "
chmod +x timescaledb-event-streamer
echo "done."
