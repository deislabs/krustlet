# Quick Start

Get started with Krustlet in three easy steps:

1. Boot a Kubernetes cluster
2. Boot a Krustlet node
3. Deploy your first application

As Krustlet is under active development, this guide will help you set up a
cluster suitable for evaluation, development, and testing purposes.

## Step 1: Boot a Kubernetes Cluster

There are many ways to boot up a Kubernetes cluster. You may choose to get up
and running in cloud environments or locally on your laptop.

If you have already created a Kubernetes cluster, proceed to the next step to
install Krustlet on your own Kubernetes cluster.

For production use:

- [Azure](../howto/krustlet-on-azure.md)
- [Amazon Elastic Kubernetes Service (EKS)](../howto/krustlet-on-eks.md)

For development and evaluation purposes, it may make sense to use a VM-based
Kubernetes cluster for quick and easy setup and teardown:

- [Kubernetes-in-Docker (KinD)](../howto/kubernetes-on-kind.md)
- [Minikube](../howto/kubernetes-on-minikube.md)

## Step 2: Boot a Krustlet Node

Depending on whatever provider you chose in step 1, you now have a few options
to boot and register Krustlet with your Kubernetes cluster.

If you have your own Kubernetes cluster, you may want to follow the steps in the
cloud-based option guides to determine how to set up Krustlet for your own
infrastructure.

For production use, you'll want to boot Krustlet on a device that can start a
web server on an IP accessible from the Kubernetes control plane.

- [Amazon Elastic Kubernetes Service (EKS)](../howto/krustlet-on-eks.md)

For testing/development environments:

- [Kubernetes-in-Docker (KinD)](../howto/krustlet-on-kind.md)
- [MicroK8s](../howto/krustlet-on-microk8s.md)
- [Minikube](../howto/krustlet-on-minikube.md)
- [Windows Subsystem for Linux (WSL2)](../howto/krustlet-on-wsl2.md)

## Step 3: Deploy your First Application

If you just want to get started and see something running, go checkout any one
of the [demos](../../demos). Each of them has a prebuilt WebAssembly module
stored in a registry and a Kubernetes manifest that you can `kubectl apply`.

If you'd like to learn how to write your own simple module in Rust and deploy
it, [follow through the tutorial](tutorial01.md) to deploy your first
application.

[development guide]: ../community/developers.md
