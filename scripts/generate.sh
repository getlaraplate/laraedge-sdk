#!/usr/bin/env bash
# Regenerates pluginv1/*.pb.go and pluginv1/*_grpc.pb.go from proto/.
#
# Locally the script uses the protoc found on PATH and fails when its version
# is not PROTOC_VERSION. CI sets PROTOC_DOWNLOAD=1: the script then downloads
# the pinned release, checks its SHA-256 and uses it, so the generated files
# are the same everywhere. The Go plugins are installed into a temporary GOBIN
# and are build-time tools only: they are not dependencies of the module.
set -euo pipefail

# Pinned toolchain. Change these together, in this file only.
PROTOC_VERSION="36.1"
PROTOC_RELEASE_TAG="v36.1"
PROTOC_ASSET="protoc-36.1-linux-x86_64.zip"
PROTOC_ASSET_SHA256="c4bc672d9d49214dc8cafdceadf4df92182d6ca8e3ec65a56b2d7de5602669b4"
PROTOC_GEN_GO_VERSION="v1.36.12"
PROTOC_GEN_GO_GRPC_VERSION="v1.6.2"
GO_MODULE="github.com/getlaraplate/laraedge-sdk"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

protoc_bin="protoc"
if [ "${PROTOC_DOWNLOAD:-}" = "1" ]; then
  curl --fail --silent --show-error --location --output "$tmp/protoc.zip" \
    "https://github.com/protocolbuffers/protobuf/releases/download/${PROTOC_RELEASE_TAG}/${PROTOC_ASSET}"
  echo "${PROTOC_ASSET_SHA256}  $tmp/protoc.zip" | sha256sum --check --quiet -
  mkdir "$tmp/protoc"
  unzip -q "$tmp/protoc.zip" -d "$tmp/protoc"
  protoc_bin="$tmp/protoc/bin/protoc"
fi

if ! have="$("$protoc_bin" --version 2>/dev/null)"; then
  echo "generate.sh: protoc not found; install protoc ${PROTOC_VERSION}" >&2
  exit 1
fi
if [ "$have" != "libprotoc ${PROTOC_VERSION}" ]; then
  echo "generate.sh: want libprotoc ${PROTOC_VERSION}, found '${have}'" >&2
  exit 1
fi

mkdir "$tmp/bin"
GOBIN="$tmp/bin" go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"
GOBIN="$tmp/bin" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"
export PATH="$tmp/bin:$PATH"

cd "$root"
rm -f pluginv1/*.pb.go

# Output paths come from go_package (module=), so the files land in pluginv1/
# while the descriptors keep the source path laraplate/edge/plugin/v1/*.proto.
"$protoc_bin" \
  --proto_path=proto \
  --go_out=. --go_opt="module=${GO_MODULE}" \
  --go-grpc_out=. --go-grpc_opt="module=${GO_MODULE}" \
  proto/laraplate/edge/plugin/v1/*.proto
