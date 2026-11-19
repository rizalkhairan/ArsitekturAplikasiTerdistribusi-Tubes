#!/usr/bin/env bash
set -euo pipefail
if [[ $(uname -s) != Linux ]] || grep -qi microsoft /proc/sys/kernel/osrelease; then
  echo 'This installs native Linux virtualization tools. On Windows/WSL2 use the kind development profile or provision Windows VMs separately.' >&2
  exit 1
fi
command -v apt-get >/dev/null || { echo 'Install libvirt, virt-install, QEMU/KVM and cloud-image-utils using your distribution package manager.' >&2; exit 1; }
sudo apt-get update
sudo apt-get install -y qemu-kvm qemu-utils libvirt-daemon-system libvirt-clients virtinst cloud-image-utils openssh-client curl make python3 python3-yaml
sudo systemctl enable --now libvirtd
echo 'Host tools installed. VM commands use sudo and qemu:///system; no group membership change is required.'
