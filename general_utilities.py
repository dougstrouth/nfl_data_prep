import logging
import os
import sys
import inspect
from IPython import get_ipython

def setup_logging(log_file=None):
    if log_file is None:
        if 'ipykernel' in sys.modules:
            try:
                ipython = get_ipython()
                if ipython is not None:
                    # Access the current notebook path from IPython
                    notebook_name = ipython.get_parent()['content']['name']
                    log_file = os.path.splitext(notebook_name)[0] + "_debug.log"
                else:
                    # Fallback if IPython is not available
                    log_file = "notebook_debug.log"
            except Exception:
                # Fallback if the notebook name can't be determined
                log_file = "notebook_debug.log"
        else:
            # Get the name of the script that called setup_logging
            caller_frame = inspect.stack()[1]  # Get the caller frame
            caller_filename = caller_frame.filename
            log_file = os.path.splitext(os.path.basename(caller_filename))[0] + "_debug.log"
    
    logging.basicConfig(
        filename=log_file,
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )