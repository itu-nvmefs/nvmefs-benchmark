import pathlib
import subprocess
import time
import re

def run_cmd(cmd: str):
    subprocess.run(cmd, shell=True, check=True)

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
        run_cmd(f"nvme dsm {self.device_path} --namespace-id={self.namespace_id} --ad --slbs=0 --blocks={self.number_of_blocks}")
    
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

    def create_namespace(self, namespace_id: int, enable_fdp: bool = False, mount_path:str = None, endgrp_id: int = 1, size_blocks: int = 0, precondition: bool = False):
        """
        Creates a namespace on the device and attaches it

        :param namespace_id: The ID of the namespace to create
        :param enable_fdp: Whether to enable flexible data placement
        :param mount_path: The mount path of the namespace
        :param endgrp_id: The ID of the endurance group on the device
        :param size_blocks: The number of blocks to allocate on the device
        :param precondition: Whether to sequentially fill the device to ensure a consistent state
        """

        # Create a namespace on the device
        ns_number_of_blocks = size_blocks if size_blocks > 0 else self.unallocated_number_of_blocks
        print(f"Creating namespace {namespace_id} with {ns_number_of_blocks} blocks")
        
        if enable_fdp:
            run_cmd(f"nvme create-ns {self.device_path} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks} --flbas=0 --endg-id={endgrp_id} --nphndls=4 --phndls=0,1,2,3")
        else: 
            run_cmd(f"nvme create-ns {self.device_path} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks} --flbas=0")

        run_cmd(f"nvme attach-ns {self.device_path} --namespace-id={namespace_id} --controllers=0x7")
        run_cmd(f"nvme ns-rescan {self.device_path}")
        
        is_mounted = mount_path is not None
        new_namespace = NvmeDeviceNamespace(self.device_path, namespace_id, ns_number_of_blocks, is_mounted)
        self.namespaces.append(new_namespace)

        if precondition:
            print(f"Preconditioning {new_namespace.get_device_path()}...")
            run_cmd(
                f"fio --name=precondition --filename={new_namespace.get_device_path()} --rw=write --bs=1M --iodepth=32 --direct=1 --ioengine=libaio --size=100%")

        if is_mounted:
            time.sleep(10) 
            run_cmd(f"mkfs.ext4 {new_namespace.get_device_path()} -b {self.block_size} {ns_number_of_blocks}") 
            run_cmd(f"mount {new_namespace.get_device_path()} {mount_path}")
        
        return new_namespace

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

    def reset(self):
        """
        Reset the device by deleting all namespaces and unmounting mounted namespaces
        """
        for namespace in self.namespaces:
            namespace.deallocate_blocks()
            namespace.delete()

def calculate_waf(host_written_bytes, media_written_bytes):
    """
    Calculates the Write Amplification Factor (WAF) based on host and media written bytes
    """
    if host_written_bytes == 0:
        return 0
    return media_written_bytes / host_written_bytes

def setup_device(device: NvmeDevice, namespace_id: int = 1, enable_fdp: bool = False, mount_path: str = None, endgrp_id: int = 1, size_blocks: int = 0, precondition: bool = False) -> NvmeDeviceNamespace:
    """
    Sets up the device by creating a namespace and enabling FDP if required
    """

    # TODO: Check if unknown namespace is already mounted and unmount before dealocating and delete of ns
    device_ns_path = pathlib.Path(f"{device.device_path}n{namespace_id}")

    if device_ns_path.exists():
        device.deallocate_nsid(namespace_id)
        device.delete_namespace_nsid(namespace_id)

    if enable_fdp:
        device.enable_fdp(endgrp_id)
    else:
        device.disable_fdp(endgrp_id)
    
    # Create new namespace with a new configuration
    return device.create_namespace(namespace_id, enable_fdp, mount_path=mount_path, endgrp_id=endgrp_id, size_blocks=size_blocks, precondition=precondition)
