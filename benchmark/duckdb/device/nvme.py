import csv
import pathlib
import statistics
import subprocess
import time
import re
import os
from typing import Any 

def run_cmd(cmd: str):
    completed_process = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    return completed_process.stdout

class NvmeDeviceNamespace:
    def __init__(self, device_path: str, namespace_id: int, number_of_blocks: int, is_mounted: bool = False):
        self.device_path = device_path
        self.namespace_id = namespace_id
        self.is_mounted = is_mounted
        self.number_of_blocks = number_of_blocks
        self.block_size = 4096

        match = re.search(r'nvme(\d+)', device_path)
        if not match: raise ValueError(f"Invalid NVMe device path: {device_path}")
        self.device_id = int(match.group(1))

    def delete(self):
        """
        Deletes a namespace on the device. After this is called the namespace is no longer usable
        """
        if self.is_mounted:
            run_cmd(f"umount -l {self.get_device_path()}")
        run_cmd(f"nvme delete-ns {self.device_path} --namespace-id={self.namespace_id}")

    def deallocate_blocks(self):
        """
        Deallocates all blocks on the device
        """
        device_ns_path = self.get_device_path()
        run_cmd(f"nvme dsm {device_ns_path} --ad --slbs=0 --blocks={self.number_of_blocks}")
    
    def get_generic_device_path(self):
        """
        Returns the generic device path for the namespace
        """
        return f"/dev/ng{self.device_id}n{self.namespace_id}"
    
    def get_device_path(self):
        """
        Returns the device path for the namespace
        """
        return f"/dev/nvme{self.device_id}n{self.namespace_id}"

    def get_written_bytes(self):
        h_out = subprocess.check_output(f"nvme smart-log {self.device_path}", shell=True, text=True)
        h_match = re.search(r"Data Units Written.+ (\d+)", h_out)
        host_written = int(h_match.group(1)) * 512000 if h_match else 0

        m_out = subprocess.check_output(f"nvme ocp smart-add-log {self.device_path}", shell=True, text=True)
        m_match = re.search(r"Physical media units written.+\d+ (\d+)", m_out)
        media_written = int(m_match.group(1)) if m_match else 0

        return host_written, media_written

class NvmeDevice:
    """
    Represents an NVMe device. This class is used to interact with the administrative interface of the NVMe device using the nvme client.
    This is without the namespace suffix,e.g. /dev/nvme0
    """
    def __init__(self, device_path: str):
        self.namespaces = []
        self.device_path = device_path
        self.block_size = 4096

        match = re.search(r'nvme(\d+)', device_path)
        if not match:
            raise ValueError(f"Invalid NVMe device path: {device_path}")
        self.device_id = int(match.group(1))

        self.number_of_blocks, self.unallocated_number_of_blocks = self.__get_device_info()
    
    def __get_device_info(self):
        total_blocks_command = f"nvme id-ctrl {self.device_path} | grep 'tnvmcap' | sed 's/,//g' | awk -v BS={self.block_size} '{{print $3/BS}}'"
        unallocated_blocks_command = f"nvme id-ctrl {self.device_path} | grep 'unvmcap' | sed 's/,//g' | awk -v BS={self.block_size} '{{print $3/BS}}'"

        block_output = subprocess.check_output(total_blocks_command, shell=True)
        unallocated_block_output = subprocess.check_output(unallocated_blocks_command, shell=True)

        number_of_blocks = int(block_output)
        unallocated_number_of_blocks = int(unallocated_block_output) - 713958 # Based on experience that some metadata needs allocated on the device

        return number_of_blocks, unallocated_number_of_blocks
    
    def get_ns_block_amount(self, namespace_id: int):
        """
        Returns the number of blocks in the namespace
        """
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                return namespace.number_of_blocks
        
        command = f"nvme id-ns {self.device_path} --namespace-id={namespace_id} | grep 'nvmcap' | sed 's/,//g' | awk -v BS={self.block_size} '{{print $3/BS}}'"
        block_output = subprocess.check_output(command, shell=True)
        number_of_blocks = int(block_output) 

        return number_of_blocks

    def deallocate(self, namespace: NvmeDeviceNamespace):
        """
        Deallocates all blocks on the device
        """
        namespace.deallocate_blocks()
    
    def deallocate_nsid(self, namespace_id: int):
        """
        Deallocates all blocks on the device
        """
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                namespace.deallocate_blocks()
                return
        
        number_of_blocks = self.get_ns_block_amount(namespace_id)
        run_cmd(f"nvme dsm {self.device_path}n{namespace_id} --ad --slbs=0 --blocks={number_of_blocks}")


    def enable_fdp(self, endgrp_id: int = 1):
        """
        Enables flexible data placement(FDP) on the device
        """
        run_cmd(f"nvme fdp feature {self.device_path} --endgrp-id={endgrp_id} --enable-conf-idx=0")

    def disable_fdp(self, endgrp_id: int = 1):
        """
        Disables flexible data placement(FDP) on the device
        """
        run_cmd(f"nvme fdp feature {self.device_path} --endgrp-id={endgrp_id} --disable")

    def delete_namespace(self, namespace: NvmeDeviceNamespace):
        """
        Deletes a namespace on the device
        """
        namespace.delete()

    def delete_namespace_nsid(self, namespace_id: int):
        """
        Deletes a namespace on the device
        """
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                namespace.delete()
                return
        
        run_cmd(f"nvme delete-ns {self.device_path} --namespace-id={namespace_id}")

    def create_namespace(self, 
                        namespace_id: int, enable_fdp: bool = False, 
                        should_mount: bool = False, endgrp_id: int = 1, size_blocks: int = 0, 
                        precondition: bool = False, fio_file: str = None, settle_seconds: int = 0, 
                        dsm_after_precondition: bool = True, fdp_handles: list = None):
        # Create a namespace on the device
        ns_number_of_blocks = size_blocks if size_blocks > 0 else self.unallocated_number_of_blocks
        print(f"Creating namespace {namespace_id} with {ns_number_of_blocks} blocks")
        
        if enable_fdp:
            handles = fdp_handles if fdp_handles else [1, 2, 3, 4]
            nphndls = len(handles)
            phndls = ",".join(str(h) for h in handles)
            print(f"Attaching {nphndls} placement handle(s): {phndls}")
            run_cmd(f"nvme create-ns {self.device_path} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks} --flbas=0 --endg-id={endgrp_id} --nphndls={nphndls} --phndls={phndls}")
        else: 
            run_cmd(f"nvme create-ns {self.device_path} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks} --flbas=0")

        run_cmd(f"nvme attach-ns {self.device_path} --namespace-id={namespace_id} --controllers=0x7")
        run_cmd(f"nvme ns-rescan {self.device_path}")
        
        new_namespace = NvmeDeviceNamespace(self.device_path, namespace_id, ns_number_of_blocks, is_mounted=False)

        subprocess.run("udevadm settle", shell=True, stderr=subprocess.DEVNULL)
        time.sleep(5)

        # Sequential Fill + Random Scramble/Writes
        if precondition:
            run_cmd("rm -f steadystate_iops.*")

            precondition_path = new_namespace.get_device_path()
            SEQ_PASSES = 2
            RANDOM_RUNTIME_S = 1800 # 30 seconds
            
            print(f"Block-device preconditioning on {precondition_path}...")
            for i in range(SEQ_PASSES):
                print(f"  Sequential fill pass {i + 1}/{SEQ_PASSES}...")
                run_cmd(
                    f"fio --name=seq-fill-{i} --filename={precondition_path} "
                    f"--rw=write --bs=1M --iodepth=32 --direct=1 --ioengine=libaio "
                    f"--refill_buffers=1 --scramble_buffers=1 --size=100%"
                )

            print(f"Random-write precondition ({RANDOM_RUNTIME_S}s)...")
            run_cmd(
                f"fio --name=random-writes --filename={precondition_path} "
                f"--rw=randwrite --bs=4k --iodepth=64 --direct=1 --ioengine=libaio "
                f"--time_based --runtime={RANDOM_RUNTIME_S} --size=100% "
                f"--refill_buffers=1 --scramble_buffers=1 "
                f"--write_iops_log=steadystate_uniform --log_avg_msec=60000"
            )

            if settle_seconds > 0:
                print(f"Waiting {settle_seconds}s for FTL to settle...")
                time.sleep(settle_seconds)

            if dsm_after_precondition:
                print(f"DSM on workload namespace {new_namespace.namespace_id}...")
                new_namespace.deallocate_blocks()

                # Mount
        mount_path = None
        if should_mount:
            time.sleep(10)
            device_path = new_namespace.get_device_path()

            uid = os.getuid()
            gid = os.getgid()
            run_cmd(f"mkfs.ext4 -E root_owner={uid}:{gid} {device_path}")
            run_cmd(f"udevadm settle")

            mount_output = run_cmd(f"udisksctl mount -b {device_path} --no-user-interaction")
            match = re.search(r"^Mounted \/dev\/nvme\dn\d at (\/run\/media\/itu\/[a-f\d-]+)", mount_output)
            if match is not None:
                mount_path = match.group(1)
                new_namespace.is_mounted = True

        self.namespaces.append(new_namespace)
        return new_namespace, mount_path

    def create_workload_namespace(self, namespace_id: int, ns_size_blocks: int, workload_blocks: int = 0,
        enable_fdp: bool = False, endgrp_id: int = 1, fdp_handles: list = None) -> NvmeDeviceNamespace:

        if workload_blocks == 0:
            workload_blocks = ns_size_blocks
        if workload_blocks > ns_size_blocks:
            raise ValueError(f"workload_blocks ({workload_blocks}) > ns_size_blocks ({ns_size_blocks})")

        if enable_fdp:
            handles = fdp_handles if fdp_handles else [1, 2, 3, 4]
            nphndls = len(handles)
            phndls = ",".join(str(h) for h in handles)
            print(f"  Attaching {nphndls} placement handle(s): {phndls}")
            run_cmd(
                f"nvme create-ns {self.device_path} --nsze={ns_size_blocks} "
                f"--ncap={ns_size_blocks} --flbas=0 --endg-id={endgrp_id} "
                f"--nphndls={nphndls} --phndls={phndls}"
            )
        else:
            run_cmd(
                f"nvme create-ns {self.device_path} --nsze={ns_size_blocks} "
                f"--ncap={ns_size_blocks} --flbas=0"
            )

        run_cmd(f"nvme attach-ns {self.device_path} --namespace-id={namespace_id} --controllers=0x7")
        run_cmd(f"nvme ns-rescan {self.device_path}")

        ns = NvmeDeviceNamespace(self.device_path, namespace_id, ns_size_blocks, is_mounted=False)
        subprocess.run("udevadm settle", shell=True, stderr=subprocess.DEVNULL)
        time.sleep(5)

        self.namespaces.append(ns)
        return ns
    
    def fill_filler_region(self, namespace: NvmeDeviceNamespace, workload_blocks: int,
        ns_size_blocks: int, passes: int = 2):
        filler_blocks = ns_size_blocks - workload_blocks
        # Populate filler region with valid sequential data
        if filler_blocks > 0:
            filler_offset = workload_blocks * self.block_size
            filler_size = filler_blocks * self.block_size
            print(
                f"  Populating filler region NS {namespace.namespace_id}: "
                f"offset={filler_offset:,}B size={filler_size:,}B"
            )

            for i in range(passes):
                print(f"    Filler pass {i + 1}/{passes}...")
                run_cmd(
                    f"fio --name=ns{namespace.namespace_id}-filler-{i} "
                    f"--filename={namespace.get_device_path()} "
                    f"--rw=write --bs=1M --iodepth=32 --direct=1 --ioengine=libaio "
                    f"--refill_buffers=1 --scramble_buffers=1 "
                    f"--offset={filler_offset} --size={filler_size}"
                )

    def precondition_workload_region(self, namespace: NvmeDeviceNamespace, workload_blocks: int,
        sequential_passes: int = 2, random_write_seconds: int = 1800):
        
        workload_bytes = workload_blocks * self.block_size
        device_ns_path = namespace.get_device_path()

        for i in range(sequential_passes):
            print(f"  Sequential pass {i + 1}/{sequential_passes}...")
            run_cmd(
                f"fio --name=ns{namespace.namespace_id}-precond-seq-{i} "
                f"--filename={device_ns_path} "
                f"--rw=write --bs=1M --iodepth=32 --direct=1 --ioengine=libaio "
                f"--refill_buffers=1 --scramble_buffers=1 "
                f"--offset=0 --size={workload_bytes}"
            )
        if random_write_seconds > 0:
            print(f"  Random writes for {random_write_seconds}s...")
            run_cmd(
                f"fio --name=ns{namespace.namespace_id}-precond-rand "
                f"--filename={device_ns_path} "
                f"--rw=randwrite --bs=4k --iodepth=64 --direct=1 --ioengine=libaio "
                f"--time_based --runtime={random_write_seconds} "
                f"--offset=0 --size={workload_bytes} "
                f"--refill_buffers=1 --scramble_buffers=1"
            )
    
    def dsm_workload_region(self, namespace: NvmeDeviceNamespace, workload_blocks: int):
        run_cmd(
            f"nvme dsm {namespace.get_device_path()} --ad "
            f"--slbs=0 --blocks={workload_blocks}"
        )
    
    def format_and_mount_workload_region(self, namespace: NvmeDeviceNamespace, workload_blocks: int) -> str:
        device_path = namespace.get_device_path()
        uid = os.getuid()
        gid = os.getgid()

        run_cmd(
            f"mkfs.ext4 -F -b 4096 -E root_owner={uid}:{gid} "
            f"{device_path} {workload_blocks}"
        )

        run_cmd("udevadm settle")
        time.sleep(2)
 
        mount_output = run_cmd(
            f"udisksctl mount -b {device_path} --no-user-interaction"
        )
        match = re.search(
            r"^Mounted \/dev\/nvme\dn\d+ at (\/run\/media\/itu\/[a-f\d-]+)",
            mount_output, re.MULTILINE,
        )
        if match is None:
            raise RuntimeError(
                f"Failed to parse udisksctl mount output for {device_path}: "
                f"{mount_output!r}"
            )
        mount_path = match.group(1)
        namespace.is_mounted = True
        print(f"  Mounted at {mount_path}")
        return mount_path


    def get_written_bytes_nsid(self, namespace_id: int):
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                return namespace.get_written_bytes()
        raise Exception(f"Namespace {namespace_id} not found")

    def get_written_bytes(self):
        h_out = subprocess.check_output(f"nvme smart-log {self.device_path}", shell=True, text=True)
        h_match = re.search(r"Data Units Written.+ (\d+)", h_out)
        host_written = int(h_match.group(1)) * 512000 if h_match else 0

        m_out = subprocess.check_output(f"nvme ocp smart-add-log {self.device_path}", shell=True, text=True)
        m_match = re.search(r"Physical media units written.+\d+ (\d+)", m_out)
        media_written = int(m_match.group(1)) if m_match else 0

        return host_written, media_written

    def get_written_bytes_fdp(self, enable_fdp: bool = False, endgrp_id: int = 1):
        cmd_out = subprocess.check_output(f"nvme fdp stats {self.device_path} -e {endgrp_id}", shell=True, text=True)
        h_match = re.search(r"Host Bytes with Metadata Written \(HBMW\):+ (\d+)", cmd_out)
        host_written = int(h_match.group(1)) if h_match else 0

        m_match = re.search(r"Media Bytes with Metadata Written \(MBMW\):+ (\d+)", cmd_out)
        media_written = int(m_match.group(1)) if m_match else 0

        return host_written, media_written

    def reset(self):
        """
        Reset the device by deleting all namespaces and unmounting mounted namespaces
        """
        # Query hardware for active namespaces
        try:
            ns_list_out = subprocess.check_output(f"nvme list-ns --all {self.device_path}", shell=True, text=True)
            active_nsids = []
            for line in ns_list_out.strip().split('\n'):
                if ':' in line and '[' in line:
                    nsid_str = line.split(':')[1].strip()
                    active_nsids.append(int(nsid_str, 16)) # Convert hex (0x1) to int (1)
        except subprocess.CalledProcessError:
            active_nsids = []

        for nsid in active_nsids:   
            subprocess.run(f"nvme detach-ns {self.device_path} --namespace-id={nsid} --controllers=0x7", shell=True, stderr=subprocess.DEVNULL)
            subprocess.run(f"nvme delete-ns {self.device_path} --namespace-id={nsid}", shell=True, stderr=subprocess.DEVNULL)

        self.namespaces = []
        self.number_of_blocks, self.unallocated_number_of_blocks = self.__get_device_info()
    
    def create_filler_namespace(self, namespace_id: int, size_blocks: int, enable_fdp: bool = False, endgrp_id: int = 1, phndls: str = "0"):
        """
        Creates a namespace intended to hold static cold data that occupies device capacity without being touched by the workload.
        """
        print(f"Creating filler namespace {namespace_id} with {size_blocks} blocks")

        if enable_fdp:
            # Filler namespace gets its own dedicated reclaim unit handle
            run_cmd(f"nvme create-ns {self.device_path} --nsze={size_blocks} --ncap={size_blocks} "
                    f"--flbas=0 --endg-id={endgrp_id} --nphndls=1 --phndls={phndls}")
        else:
            run_cmd(f"nvme create-ns {self.device_path} --nsze={size_blocks} --ncap={size_blocks} --flbas=0")
        
        run_cmd(f"nvme attach-ns {self.device_path} --namespace-id={namespace_id} --controllers=0x7")
        run_cmd(f"nvme ns-rescan {self.device_path}")

        return NvmeDeviceNamespace(self.device_path, namespace_id, size_blocks, is_mounted=False)

def fill_namespace_with_data(namespace: NvmeDeviceNamespace, passes: int = 2):
    device_path = namespace.get_device_path()
    print(f"Filling {device_path} ({passes} pass(es))...")

    subprocess.run("udevadm settle", shell=True, stderr=subprocess.DEVNULL)
    time.sleep(2)

    for i in range(passes):
        print(f"  Pass {i + 1}/{passes}...")
        fill_cmd = (
            f"fio --name=filler-pass{i} --filename={device_path} "
            f"--rw=write --bs=1M --iodepth=32 --direct=1 --ioengine=libaio "
            f"--refill_buffers=1 --scramble_buffers=1 --size=100%"
        )
        subprocess.run(fill_cmd, shell=True, check=True)

    print(f"Filler namespace {namespace.namespace_id} ready ({passes} passes complete).")

def calculate_waf(host_written_bytes, media_written_bytes):
    """
    Calculates the Write Amplification Factor (WAF) based on host and media written bytes
    """
    if host_written_bytes == 0:
        return 0
    return media_written_bytes / host_written_bytes

def setup_device(device: NvmeDevice, namespace_id: int = 1, enable_fdp: bool = False, should_mount: bool = False,
                 endgrp_id: int = 1, size_blocks: int = 0, precondition: bool = False,
                 fio_file: str = None, settle_seconds: int = 0,
                 dsm_after_precondition: bool = False, fdp_handles: list = None) -> tuple[NvmeDeviceNamespace, str | Any | None]:
    """
    Create a workload namespace and optionally precondition it.
    """
    device_ns_path = pathlib.Path(f"{device.device_path}n{namespace_id}")
    if device_ns_path.exists():
        subprocess.run(f"umount -l {device_ns_path}", shell=True, stderr=subprocess.DEVNULL)
        device.deallocate_nsid(namespace_id)
        device.delete_namespace_nsid(namespace_id)

    new_namespace, mount_path = device.create_namespace(
        namespace_id, enable_fdp,
        should_mount=should_mount, endgrp_id=endgrp_id, size_blocks=size_blocks,
        precondition=precondition,
        fio_file=fio_file,
        settle_seconds=settle_seconds,
        dsm_after_precondition=dsm_after_precondition,
        fdp_handles=fdp_handles,
    )

    return new_namespace, mount_path

def verify_steady_state(log_file="steadystate_iops.1.log", evaluation_window_samples=25, max_cv_percent=5.0):
    """
    Verifies the steady state of the device
    Parses fio IOPS log to verify if the NVMe device has reached a steady state
    """
    if not os.path.exists(log_file):
        print(f"Log file {log_file} not found")
        return

    iops_data = []
    with open(log_file, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                try:
                    iops_data.append(float(row[1].strip()))
                except ValueError:
                    continue

    if len(iops_data) < evaluation_window_samples:
        print(f"Not enough data: got {len(iops_data)} samples, need {evaluation_window_samples}")
        return

    tail_data = iops_data[-evaluation_window_samples:]
    mean_iops = statistics.mean(tail_data)
    std_dev = statistics.stdev(tail_data)
    cv_percent = (std_dev / mean_iops) * 100 if mean_iops > 0 else float('inf')

    print("\nSteady State:")
    print(f"Mean IOPS (μ):           {mean_iops:.2f}")
    print(f"Standard Deviation:  {std_dev:.2f}")
    print(f"Coefficient of Variance: {cv_percent:.2f}%")

    if cv_percent <= max_cv_percent:
        print("The NVMe Drive is in a steady state")
    else:
        print("The NVMe Drive is not in a steady state")

def parse_fdp_handles(mapping: str) -> list[int]:
    """
    Extract sorted, unique non-zero RUH IDs from a mapping string.
    'tpch.db:1,tpch.wal:2,ycsb.db:3,.tmp:5' -> [1, 2, 3, 5]
    RUH 0 is the default and is always available; don't include it.
    """
    if not mapping:
        return []
    handles = set()
    for pair in mapping.split(','):
        kv = pair.split(':')
        if len(kv) == 2:
            try:
                ruh = int(kv[1].strip())
                if ruh > 0:
                    handles.add(ruh)
            except ValueError:
                pass
    return sorted(handles)