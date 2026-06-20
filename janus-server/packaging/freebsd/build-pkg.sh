#!/bin/sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
PKG_DIR="${ROOT_DIR}/janus-server/packaging/freebsd"
PREFIX="/opt/janus-server"
VERSION="${VERSION:-$(awk -F\" '/^version = / { print $2; exit }' "${ROOT_DIR}/janus-server/Cargo.toml")}"
BUILD_DIR="${BUILD_DIR:-${ROOT_DIR}/target/freebsd-pkg}"
FAKE_ROOT="${BUILD_DIR}/root"
METADATA_DIR="${BUILD_DIR}/metadata"
OUT_DIR="${OUT_DIR:-${BUILD_DIR}/packages}"
PLIST="${PKG_DIR}/pkg-plist"
BINARY="${JANUS_SERVER_BIN:-${ROOT_DIR}/target/release/janus-server}"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
    cargo build --release --locked -p janus-server
fi

if [ ! -x "${BINARY}" ]; then
    echo "janus-server binary not found or not executable: ${BINARY}" >&2
    echo "Set JANUS_SERVER_BIN=/path/to/janus-server or run without SKIP_BUILD=1." >&2
    exit 1
fi

rm -rf "${FAKE_ROOT}" "${METADATA_DIR}" "${OUT_DIR}"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/bin"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/etc/rc.d"
install -d -m 0755 "${FAKE_ROOT}${PREFIX}/libexec"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/var"
install -d -m 2770 "${FAKE_ROOT}${PREFIX}/var/repos"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/var/db"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/var/keys"
install -d -m 2770 "${FAKE_ROOT}${PREFIX}/run"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/log"
install -d -m 0750 "${FAKE_ROOT}${PREFIX}/home/git"
install -d -m 0700 "${FAKE_ROOT}${PREFIX}/home/git/.ssh"

install -m 0755 "${BINARY}" "${FAKE_ROOT}${PREFIX}/bin/janus-server"
install -m 0640 "${ROOT_DIR}/janus-server/server.example.toml" "${FAKE_ROOT}${PREFIX}/etc/server.toml.sample"
install -m 0755 "${PKG_DIR}/files/janus_server.rc" "${FAKE_ROOT}${PREFIX}/etc/rc.d/janus_server"
install -m 0755 "${PKG_DIR}/files/janus-git-ssh" "${FAKE_ROOT}${PREFIX}/libexec/janus-git-ssh"

install -d -m 0755 "${METADATA_DIR}" "${OUT_DIR}"
sed "s/^version:.*/version: \"${VERSION}\"/" "${PKG_DIR}/+MANIFEST" > "${METADATA_DIR}/+MANIFEST"
install -m 0755 "${PKG_DIR}/+PRE_INSTALL" "${METADATA_DIR}/+PRE_INSTALL"
install -m 0755 "${PKG_DIR}/+POST_INSTALL" "${METADATA_DIR}/+POST_INSTALL"
install -m 0644 "${PKG_DIR}/+DISPLAY" "${METADATA_DIR}/+DISPLAY"

pkg create -r "${FAKE_ROOT}" -m "${METADATA_DIR}" -p "${PLIST}" -o "${OUT_DIR}"

echo "Package written to ${OUT_DIR}"
echo "Install with: pkg install ${OUT_DIR}/janus-server-${VERSION}.pkg"
