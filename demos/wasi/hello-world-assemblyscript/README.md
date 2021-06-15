# Hello World AssemblyScript for WASI

A simple hello world example in AssemblyScript that will print:

- The environment variables available to the process
- Text to both stdout and stderr.
- Any args passed to the process

It is meant to be a simple demo for the wasi-provider with Krustlet.

## Running the example

This example has already been pre-built, so you only need to install it into
your Kubernetes cluster.

Create the pod and configmap with `kubectl`:

```shell
$ kubectl apply -f k8s.yaml
```

You should then be able to get the logs and see the output from the pod:

```shell
$ kubectl logs hello-world-wasi-assemblyscript
hello from stdout!
hello from stderr!
FOO=bar
CONFIG_MAP_VAL=cool stuff
POD_NAME=hello-world-wasi-assemblyscript
Args are:
```

## Building from Source

If you want to compile the demo and inspect it, you'll need to do the following.

### Prerequisites

You'll need `npm` installed in order to install and build the dependencies.
This project is using the [as-wasi](https://github.com/jedisct1/as-wasi)
dependency, which is a helpful set of wrappers around the low level WASI
bindings provided in AssemblyScript.

If you are interested in starting your own AssemblyScript project, visit the
AssemblyScript
[getting started guide](https://www.assemblyscript.org/).

If you don't have Krustlet with the WASI provider running locally, see the
instructions in the [tutorial](../../../docs/intro/tutorial03.md) for running
locally.

### Compiling

Run:

```shell
$ npm install && npm run asbuild
```

### Pushing

Detailed instructions for pushing a module can be found in the
[tutorial](../../../docs/intro/tutorial02.md).
