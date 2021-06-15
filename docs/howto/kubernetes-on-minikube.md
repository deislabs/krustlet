# Running Kubernetes on Minikube

This tutorial will focus on using a tool called
[minikube](https://github.com/kubernetes/minikube).

If you haven't installed them already, go ahead and [install VirtualBox 5.2 or
higher](https://www.virtualbox.org/), [install
minikube](https://minikube.sigs.k8s.io/docs/start/), and [install
kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/).

You'll need `kubectl` to interact with the cluster once it's created.

## Check virtualization support

To use VM drivers, verify that your system has virtualization support enabled:

```console
$ egrep -q 'vmx|svm' /proc/cpuinfo && echo yes || echo no
```

If the above command outputs “no”:

- If you are running within a VM, your hypervisor does not allow nested
  virtualization. You will need to use the *None (bare-metal)* driver
- If you are running on a physical machine, ensure that your BIOS has hardware
  virtualization enabled

## Create a cluster

Once VirtualBox, minikube, and kubectl are installed, create a cluster with
minikube:

```console
$ minikube start --driver=virtualbox
```

This will create a cluster with a single node - perfect for local development.

You should see output similar to the following:

```console
😄  minikube v1.9.0 on Ubuntu 18.04
✨  Using the virtualbox driver based on user configuration
💿  Downloading VM boot image ...
💾  Downloading Kubernetes v1.18.0 preload ...
🔥  Creating virtualbox VM (CPUs=2, Memory=6000MB, Disk=20000MB) ...
🐳  Preparing Kubernetes v1.18.0 on Docker 19.03.8 ...
🌟  Enabling addons: default-storageclass, storage-provisioner
🏄  Done! kubectl is now configured to use "minikube"
```

Now we can interact with our cluster! Try that out now:

```console
$ kubectl cluster-info
Kubernetes master is running at https://192.168.99.164:8443
KubeDNS is running at https://192.168.99.164:8443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
```

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
