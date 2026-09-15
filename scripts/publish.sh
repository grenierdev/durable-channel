#!/bin/bash

VERSION=$(cat src/deno.jsonc | jq -r '.version')
TAG="${1:-latest}"

# deno pack enforce @scope/name for JSR, so we need to temporarily abide by changing the name in deno.jsonc, then change it back after packing
sed -i "s|auth-dance|@grenierdev/auth-dance|g" ./src/deno.jsonc
deno pack --set-version $VERSION --allow-dirty --output /tmp/package.tgz
sed -i "s|@grenierdev/auth-dance|auth-dance|g" ./src/deno.jsonc

# Clean up & extract temp package
rm -fr /tmp/package/* /tmp/package/.*
tar -xf /tmp/package.tgz -C /tmp
rm -f /tmp/package.tgz

# Cleanup
cp README.md .npmignore /tmp/package/
jq -s '.[0] * .[1]' /tmp/package/package.json ./src/package.jsonc > /tmp/package/package2.json
rm -f /tmp/package/package.jsonc
mv /tmp/package/package2.json /tmp/package/package.json

# Publish
cd /tmp/package && npm publish --access public --tag $TAG