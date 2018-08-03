#!/bin/bash

# from https://gist.github.com/sofianinho/9a78beff3d6fa04b0a2095fd49beb899

PROTOC_REPO="google/protobuf"

# Credits to: lukechilds here: https://gist.github.com/lukechilds/a83e1d7127b78fef38c2914c4ececc3c
get_latest_release() {
  URL="https://api.github.com/repos/$1/releases/latest"
  VERSION=$(curl --silent $URL |grep '"tag_name":' |sed -E 's/.*"([^"]+)".*/\1/')
  echo $VERSION
}

LATEST_VERSION=$(get_latest_release ${PROTOC_REPO})
echo "Latest version is:  ${LATEST_VERSION}"
_VERSION=${LATEST_VERSION:1}

# grab the latest version
curl -OL https://github.com/${PROTOC_REPO}/releases/download/${LATEST_VERSION}/protoc-${_VERSION}-linux-x86_64.zip

# Unzip
unzip protoc-${_VERSION}-linux-x86_64.zip -d protoc3

# Move protoc to /usr/local/bin/
sudo mv protoc3/bin/* /usr/local/bin/

# Move protoc3/include to /usr/local/include/
sudo mv protoc3/include/* /usr/local/include/

# Optional: change owner
sudo chown $USER /usr/local/bin/protoc
sudo chown -R $USER /usr/local/include/google

rm -r ./protoc3
go get -v -u github.com/golang/protobuf/protoc-gen-go

exit 0
