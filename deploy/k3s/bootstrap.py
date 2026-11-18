#!/usr/bin/env python3
"""Native Linux VM bootstrap. No container nodes or nested WSL virtualization."""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import uuid
import xml.etree.ElementTree as ET

import manage

HERE = Path(__file__).resolve().parent
STATE = HERE / ".vms"
DISKS = Path("/var/lib/libvirt/images/bnpb")
MARKER = "BNPB bootstrap managed network"


def call(args, **kwargs):
    return subprocess.run([str(a) for a in args], check=True, text=True, env={**os.environ, "LC_ALL": "C"}, **kwargs)


def virsh(*args):
    return call(["sudo", "-n", "virsh", "--connect", "qemu:///system", *args], capture_output=True).stdout.strip()


def names(*args):
    return set(virsh(*args, "--name").splitlines())


def read_config():
    path = HERE / "bootstrap.json"
    cfg = json.loads((path if path.exists() else HERE / "bootstrap.example.json").read_text())
    if not re.fullmatch(r"v\d+\.\d+\.\d+\+k3s\d+", cfg["k3s_version"]):
        raise ValueError("Pin a valid k3s release")
    for key in ("network", "bridge", "username"):
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,14}", cfg[key]):
            raise ValueError(f"Invalid {key}")
    subnet = ipaddress.IPv4Network(cfg["subnet"])
    if not subnet.is_private or ipaddress.IPv4Address(cfg["gateway"]) not in subnet:
        raise ValueError("Use a private subnet with its gateway inside it")
    if {w["name"] for w in cfg["workers"]} != {"node-app", "node-store"} or len(cfg["workers"]) != 2:
        raise ValueError("Exactly node-app and node-store are required")
    for worker in cfg["workers"]:
        if ipaddress.IPv4Address(worker["ip"]) not in subnet or worker["ip"] == cfg["gateway"]:
            raise ValueError("Worker IP must be inside the VM subnet and distinct from its gateway")
        if not re.fullmatch(r"52:54:00(:[0-9a-f]{2}){3}", worker["mac"]):
            raise ValueError("Set a unique QEMU MAC for each worker")
        for key, minimum in (("memory_mib", 1024), ("cpus", 1), ("disk_gib", 24)):
            if type(worker[key]) is not int or worker[key] < minimum:
                raise ValueError(f"{key} must be an integer >= {minimum}")
    for key in ("ip", "mac"):
        if len({w[key] for w in cfg["workers"]}) != 2:
            raise ValueError(f"Workers must have distinct {key}")
    for key in ("image_url", "checksums_url"):
        if not cfg[key].startswith("https://"):
            raise ValueError("Cloud image and checksums must be downloaded over HTTPS")
    return cfg


def network_xml(cfg, network_id):
    root = ET.Element("network")
    ET.SubElement(root, "name").text = cfg["network"]
    ET.SubElement(root, "uuid").text = network_id
    ET.SubElement(root, "description").text = MARKER
    ET.SubElement(root, "forward", mode="nat")
    ET.SubElement(root, "bridge", name=cfg["bridge"], stp="on", delay="0")
    net = ipaddress.IPv4Network(cfg["subnet"])
    ip = ET.SubElement(root, "ip", address=cfg["gateway"], netmask=str(net.netmask))
    dhcp = ET.SubElement(ip, "dhcp")
    # Static reservations only: these names/IPs are stable across reboots.
    for worker in cfg["workers"]:
        ET.SubElement(dhcp, "host", mac=worker["mac"], name=worker["name"], ip=worker["ip"])
    return ET.tostring(root, encoding="unicode")


def cloud_config(cfg, worker, public_key):
    user = {"name": cfg["username"], "shell": "/bin/bash", "lock_passwd": True,
            "sudo": "ALL=(ALL) NOPASSWD:ALL", "groups": ["sudo"], "ssh_authorized_keys": [public_key]}
    data = {"hostname": worker["name"], "manage_etc_hosts": True, "ssh_pwauth": False,
            "disable_root": True, "users": [user], "package_update": True,
            "packages": ["ca-certificates", "curl", "openssh-server", "sudo", "qemu-guest-agent", "chrony"],
            "runcmd": [["systemctl", "enable", "--now", "qemu-guest-agent"], ["systemctl", "enable", "--now", "chrony"]]}
    # JSON is valid YAML after the cloud-config magic header.
    return "#cloud-config\n" + json.dumps(data, indent=2) + "\n"


def checksum_for(text, filename):
    for line in text.splitlines():
        match = re.fullmatch(r"([0-9a-fA-F]{128})\s+\*?(?:\./)?" + re.escape(filename), line.strip())
        if match:
            return match[1].lower()
    raise ValueError("Cloud image was not listed in the official SHA512SUMS")


def verified_image(cfg):
    image = STATE / "debian.qcow2"
    record = STATE / "image.json"
    if image.exists() and record.exists():
        previous = json.loads(record.read_text())
        if previous["url"] != cfg["image_url"] or previous["checksums_url"] != cfg["checksums_url"]:
            raise ValueError("Cloud image source changed; use a fresh explicit cache after reviewing existing VMs")
        digest = previous["sha512"]
    else:
        with urllib.request.urlopen(cfg["checksums_url"], timeout=60) as response:
            checksums = response.read(2 * 1024 * 1024).decode()
        digest = checksum_for(checksums, cfg["image_url"].rsplit("/", 1)[1])
        temp = STATE / "image.download"
        with urllib.request.urlopen(cfg["image_url"], timeout=60) as response, temp.open("wb") as output:
            shutil.copyfileobj(response, output)
        with temp.open("rb") as stream:
            if hashlib.file_digest(stream, "sha512").hexdigest() != digest:
                raise ValueError("Cloud image SHA512 mismatch; no VM was created")
        os.replace(temp, image)
        record.write_text(json.dumps({"url": cfg["image_url"], "checksums_url": cfg["checksums_url"], "sha512": digest}))
    with image.open("rb") as stream:
        if hashlib.file_digest(stream, "sha512").hexdigest() != digest:
            raise ValueError("Cached cloud image was modified")
    return image


def save_state(state):
    temp = STATE / "state.tmp"
    temp.write_text(json.dumps(state, indent=2) + "\n")
    os.replace(temp, STATE / "state.json")


def load_state():
    path = STATE / "state.json"
    return json.loads(path.read_text()) if path.exists() else {"network_uuid": str(uuid.uuid4()), "workers": {}}


def preflight(cfg, running=None):
    if platform.system() != "Linux" or "microsoft" in platform.release().lower():
        raise ValueError("libvirt bootstrap requires native Linux. On Windows/WSL2 use make dev-up or BOOTSTRAP_PROVIDER=existing")
    if platform.machine() != "x86_64":
        raise ValueError("The automated Debian VM profile currently supports native Linux amd64; use existing machines for other architectures")
    required = ["virsh", "virt-install", "qemu-img", "cloud-localds", "ssh", "ssh-keygen", "ip"]
    if shutil.which("k3s"):
        required.append("kubectl")
    missing = [name for name in required if not shutil.which(name)]
    if missing or not Path("/dev/kvm").exists():
        raise ValueError("Install/enable KVM and host tools first: make host-tools. Missing: " + ", ".join(missing))
    if running is None:
        return
    mem = {line.split(":")[0]: int(line.split()[1]) // 1024 for line in Path("/proc/meminfo").read_text().splitlines()}
    extra = sum(w["memory_mib"] for w in cfg["workers"] if w["name"] not in running)
    if extra and mem["MemAvailable"] < extra + 512:
        raise ValueError(f"Need {extra + 512} MiB available to start workers; have {mem['MemAvailable']} MiB. Free RAM or use other hosts")
    if sum(w["memory_mib"] for w in cfg["workers"]) + 2048 > mem["MemTotal"]:
        raise ValueError("VM allocation leaves less than 2 GiB physical RAM for the native host")


def control_record(cfg):
    existing = shutil.which("k3s") is not None
    if existing:
        call(["sudo", "-n", "systemctl", "start", "k3s"])
        call(["sudo", "-n", "k3s", "kubectl", "wait", "--for=condition=Ready", "node/node-control", "--timeout=180s"], capture_output=True)
        result = json.loads(call(["sudo", "-n", "k3s", "kubectl", "get", "node", "node-control", "-o", "json"], capture_output=True).stdout)
        if result["status"]["nodeInfo"]["kubeletVersion"] != cfg["k3s_version"]:
            raise ValueError("Existing server version differs from bootstrap.json; align the pinned version before provisioning")
        address = next(a["address"] for a in result["status"]["addresses"] if a["type"] == "InternalIP")
    else:
        routes = json.loads(call(["ip", "-j", "route", "get", "1.1.1.1"], capture_output=True).stdout)
        address = routes[0]["prefsrc"]
    model = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), platform.machine())
    ram = int(Path("/proc/meminfo").read_text().splitlines()[0].split()[1]) / 1024 / 1024
    return {"name": "node-control", "ssh": "local", "ip": address, "kind": "native",
            "physical_host": platform.node(), "host_cpu": model, "host_ram_gib": round(ram, 2),
            "node_cpu": os.cpu_count(), "node_ram_gib": round(ram, 2)}, existing


def ensure_network(cfg, state):
    present = names("net-list", "--all")
    if cfg["network"] in present:
        actual = ET.fromstring(virsh("net-dumpxml", cfg["network"]))
        if actual.findtext("uuid") != state["network_uuid"] or actual.findtext("description") != MARKER:
            raise ValueError("Refusing to replace an unmanaged libvirt network")
        if actual.find("bridge").get("name") != cfg["bridge"] or actual.find("ip").get("address") != cfg["gateway"]:
            raise ValueError("Managed network configuration was modified; reconcile it explicitly")
    else:
        path = STATE / "network.xml"
        path.write_text(network_xml(cfg, state["network_uuid"]))
        virsh("net-define", str(path))
    if cfg["network"] not in names("net-list"):
        virsh("net-start", cfg["network"])


def wait_guest(cfg, worker, key, known_hosts):
    args = ["ssh", "-i", str(key), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
            "-o", "StrictHostKeyChecking=accept-new", "-o", "UserKnownHostsFile=" + str(known_hosts),
            f"{cfg['username']}@{worker['ip']}"]
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        result = subprocess.run([*args, "true"], capture_output=True)
        if result.returncode == 0:
            break
        time.sleep(3)
    else:
        raise ValueError(f"SSH did not become ready on {worker['name']}; inspect its serial console")
    call([*args, "sudo -n cloud-init status --wait"], timeout=900)


def up():
    provider = os.getenv("BOOTSTRAP_PROVIDER") or "libvirt"
    if provider == "existing":
        manage.provision()
        return
    if provider != "libvirt":
        raise ValueError("BOOTSTRAP_PROVIDER must be libvirt or existing")
    cfg = read_config()
    preflight(cfg)
    running = names("list", "--state-running")
    preflight(cfg, running)
    control, existing = control_record(cfg)
    state = load_state()
    subnet = ipaddress.IPv4Network(cfg["subnet"])
    routes = json.loads(call(["ip", "-j", "route", "show"], capture_output=True).stdout)
    for route in routes:
        destination = route.get("dst", "default")
        if destination == "default":
            continue
        other = ipaddress.IPv4Network(destination, strict=False)
        if subnet.overlaps(other) and not (route.get("dev") == cfg["bridge"] and (STATE / "state.json").exists()):
            raise ValueError("VM subnet overlaps an existing host route; change bootstrap.json")
    fingerprint = hashlib.sha256(json.dumps(cfg, sort_keys=True).encode()).hexdigest()
    if state.get("config_hash", fingerprint) != fingerprint:
        raise ValueError("VM configuration changed. Destroy managed VMs explicitly or restore bootstrap.json")
    if (HERE / "inventory.json").exists() and not (STATE / "state.json").exists():
        raw = json.loads((HERE / "inventory.json").read_text())
        if any(n.get("ssh") != "CHANGE_ME" for n in raw["nodes"] if n["name"] != "node-control"):
            raise ValueError("Existing inventory describes other workers; use BOOTSTRAP_PROVIDER=existing")
    STATE.mkdir(mode=0o700, exist_ok=True)
    os.chmod(STATE, 0o700)
    state["config_hash"] = fingerprint
    save_state(state)
    manage.init()
    key = STATE / "id_ed25519"
    if not key.exists():
        call(["ssh-keygen", "-t", "ed25519", "-N", "", "-f", key], stdout=subprocess.DEVNULL)
    image = verified_image(cfg)
    ensure_network(cfg, state)
    call(["sudo", "-n", "install", "-d", "-m", "0755", DISKS])
    workers = []
    for worker in cfg["workers"]:
        name = worker["name"]
        disk, seed = DISKS / (name + ".qcow2"), DISKS / (name + "-seed.iso")
        if name not in state["workers"] and subprocess.run(["sudo", "-n", "test", "-e", disk]).returncode == 0:
            raise ValueError("Refusing to reuse a disk that has no bootstrap ownership record")
        record = state["workers"].setdefault(name, {"uuid": str(uuid.uuid4())})
        save_state(state)
        if name in names("list", "--all"):
            if virsh("domuuid", name) != record["uuid"]:
                raise ValueError("Refusing to reuse an unmanaged VM named " + name)
        else:
            folder = STATE / name
            folder.mkdir(exist_ok=True)
            (folder / "user-data").write_text(cloud_config(cfg, worker, key.with_suffix(".pub").read_text().strip()))
            (folder / "meta-data").write_text(json.dumps({"instance-id": record["uuid"], "local-hostname": name}))
            (folder / "network-config").write_text(json.dumps({"version": 1, "config": [{"type": "physical", "name": "ens3", "mac_address": worker["mac"], "subnets": [{"type": "dhcp"}]}]}))
            call(["cloud-localds", "--network-config=" + str(folder / "network-config"), folder / "seed.iso", folder / "user-data", folder / "meta-data"])
            # Interrupted copies are replaced atomically before any VM is defined.
            if subprocess.run(["sudo", "-n", "test", "-f", disk]).returncode != 0:
                temp = str(disk) + ".partial"
                call(["sudo", "-n", "qemu-img", "convert", "-f", "qcow2", "-O", "qcow2", image, temp])
                call(["sudo", "-n", "qemu-img", "resize", temp, str(worker["disk_gib"]) + "G"])
                call(["sudo", "-n", "mv", temp, disk])
            call(["sudo", "-n", "install", "-m", "0644", folder / "seed.iso", seed])
            call(["sudo", "-n", "virt-install", "--connect", "qemu:///system", "--name", name, "--uuid", record["uuid"],
                  "--memory", worker["memory_mib"], "--vcpus", worker["cpus"], "--virt-type", "kvm", "--osinfo", "debian12",
                  "--import", "--disk", f"path={disk},format=qcow2,bus=virtio", "--disk", f"path={seed},device=cdrom",
                  "--network", f"network={cfg['network']},mac={worker['mac']},model=virtio", "--graphics", "none", "--noautoconsole"])
        if name not in names("list", "--state-running"):
            virsh("start", name)
        print("Waiting for guest OS and packages on " + name, flush=True)
        known_hosts = STATE / "known_hosts"
        wait_guest(cfg, worker, key, known_hosts)
        workers.append({**control, "name": name, "kind": "vm", "ssh": f"{cfg['username']}@{worker['ip']}", "ip": worker["ip"],
                        "node_cpu": worker["cpus"], "node_ram_gib": worker["memory_mib"] / 1024,
                        "identity_file": str(key), "known_hosts_file": str(known_hosts)})
    inventory = {"k3s_version": cfg["k3s_version"], "architecture": "amd64", "manage_control_plane": not existing,
                 "nodes": [control, *workers]}
    path = HERE / "inventory.json"
    path.write_text(json.dumps(inventory, indent=2) + "\n")
    os.chmod(path, 0o600)
    manage.provision()


def stop():
    cfg, state = read_config(), load_state()
    for worker in cfg["workers"]:
        name = worker["name"]
        if name in names("list", "--all"):
            if state["workers"].get(name, {}).get("uuid") != virsh("domuuid", name):
                raise ValueError("Refusing to stop an unmanaged VM")
            if name in names("list", "--state-running"):
                virsh("shutdown", name)


def destroy():
    cfg, state = read_config(), load_state()
    present = names("list", "--all")
    # Validate every target before destroying the first machine.
    targets = []
    for worker in cfg["workers"]:
        name = worker["name"]
        if name not in present:
            if name in state["workers"]:
                targets.append((name, None))  # Clean an interrupted, journaled VM creation.
            continue
        if state["workers"].get(name, {}).get("uuid") != virsh("domuuid", name):
            raise ValueError("Refusing to delete an unmanaged VM")
        xml = ET.fromstring(virsh("dumpxml", name))
        allowed = {str(DISKS / (name + ".qcow2")), str(DISKS / (name + "-seed.iso"))}
        if any(source.get("file") not in allowed for source in xml.findall("./devices/disk/source")):
            raise ValueError("Managed VM has unexpected attached storage; refusing deletion")
        targets.append((name, xml.find("./os/nvram") is not None))
    for name, nvram in targets:
        if nvram is not None:
            if name in names("list", "--state-running"):
                virsh("destroy", name)
            virsh("undefine", name, *(["--nvram"] if nvram else []))
        call(["sudo", "-n", "rm", "-f", "--", DISKS / (name + ".qcow2"), DISKS / (name + "-seed.iso")])
        state["workers"].pop(name, None)
        save_state(state)
    # Keep the network/OS cache and SSH identity; clear only our VM host-key records.
    if targets and (STATE / "known_hosts").exists():
        (STATE / "known_hosts").unlink()
    if not state["workers"]:
        state.pop("config_hash", None)
        save_state(state)
    print("Managed worker VMs and their disks deleted. Run make bootstrap to recreate them.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("up", "plan", "stop", "destroy"))
    action = parser.parse_args().action
    try:
        if action == "plan":
            print(json.dumps(read_config(), indent=2))
            print("Creates two worker VMs; reuses the native k3s server when installed. No changes made.")
        else:
            globals()[action]()
    except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
        print("Bootstrap failed: " + str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
