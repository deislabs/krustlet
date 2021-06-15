export RUST_LOG := "wasi_provider=debug,main=debug,kubelet=debug"
export PFX_PASSWORD := "testing"
export CONFIG_DIR := env_var_or_default('CONFIG_DIR', '$HOME/.krustlet/config')
csi_binaries_path := "./csi-test-binaries/"
registrar_path := csi_binaries_path + "csi-node-driver-registrar"
provisioner_path := csi_binaries_path + "csi-provisioner"

# For backward compatibility with those running `just run-wasi`
run-wasi: run

build +FLAGS='':
    cargo build {{FLAGS}}

lint-docs:
    markdownlint '**/*.md' -c .markdownlint.json

linkcheck-docs:
    find . -name \*.md | xargs -I{} markdown-link-check -c .markdownlinkcheck.json {} 

test:
    cargo fmt --all -- --check
    cargo clippy --workspace
    cargo test --workspace --lib
    cargo test --doc --all

_download-csi-test-binaries:
    @mkdir -p {{csi_binaries_path}}
    @test -f  {{registrar_path}} || curl --fail -o {{registrar_path}} https://krustlet.blob.core.windows.net/releases/csi-node-driver-registrar-linux && chmod +x {{registrar_path}}
    @test -f {{provisioner_path}} || curl --fail -o {{provisioner_path}} https://krustlet.blob.core.windows.net/releases/csi-provisioner-linux && chmod +x {{provisioner_path}}

download-test-binaries:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{os()}}" == "linux" ]; 
    then 
        just _download-csi-test-binaries 
    fi;

test-e2e:
    cargo test --test integration_tests

test-e2e-standalone: download-test-binaries
    cargo run --bin oneclick

test-e2e-ci:
    KRUSTLET_TEST_ENV=ci cargo test --test integration_tests

test-e2e-standalone-ci: download-test-binaries
    KRUSTLET_TEST_ENV=ci cargo run --bin oneclick

run +FLAGS='': bootstrap
    KUBECONFIG=$(eval echo $CONFIG_DIR)/kubeconfig-wasi cargo run --bin krustlet-wasi {{FLAGS}} -- --node-name krustlet-wasi --port 3001 --bootstrap-file $(eval echo $CONFIG_DIR)/bootstrap.conf --cert-file $(eval echo $CONFIG_DIR)/krustlet-wasi.crt --private-key-file $(eval echo $CONFIG_DIR)/krustlet-wasi.key

bootstrap:
    @# This is to get around an issue with the default function returning a string that gets escaped
    @mkdir -p $(eval echo $CONFIG_DIR)
    @test -f  $(eval echo $CONFIG_DIR)/bootstrap.conf || CONFIG_DIR=$(eval echo $CONFIG_DIR) ./docs/howto/assets/bootstrap.sh
    @chmod 600 $(eval echo $CONFIG_DIR)/*
