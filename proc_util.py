import sys
import subprocess

def exec_command(cmd_list, log, exit_on_fail=True, verbose=True):
    log.info(f'Executing Command: {" ".join([str(arg) for arg in cmd_list])}')
    result = subprocess.run(cmd_list, capture_output=True, text=True)
    if result.stdout and verbose:
        log.debug(f'Command stdout:\n {result.stdout}')
    if result.stderr and verbose:
        log.error(f'Command stderr:\n {result.stderr}')
    try:
      result.check_returncode()
    except subprocess.CalledProcessError:
        if verbose:
            log.info(f'Error Executing Command: {" ".join([str(arg) for arg in cmd_list])}')
            log.error(f'Return Code: {result.returncode}')
        if exit_on_fail:
           log.info("Exiting")
           sys.exit(1)
    return result.returncode, result.stdout


