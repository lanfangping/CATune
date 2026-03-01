import psutil

def get_hardware_info(unit='MB'):
    available_cpu_cores = psutil.cpu_count(logical=False)
    memory = psutil.virtual_memory()
    total_memory = memory.total

    if unit == 'MB':
        divider = 1024 * 1024
    elif unit == 'GB':
        divider = 1024 * 1024 * 1024
    else:
        raise ValueError("Unsupported unit. Use 'MB' or 'GB'.")

    total_memory = total_memory / divider
    root_disk = psutil.disk_usage('/')
    total_disk_space = root_disk.total
    total_disk_space = total_disk_space / divider 
    return available_cpu_cores, int(total_memory), int(total_disk_space)            
