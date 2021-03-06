# slmagic.py
# This module switches a threaded program to a tasklet based one.

import runpy
import sys
from .monkeypatch import patch_all
from .app import install_stackless

import stackless
from . import main


# The actual __main__ will be run here in a tasklet
def run():
    try:
        # Shift command line arguments.
        if len(sys.argv) > 1:
            target = sys.argv.pop(1)
            if target == "-m" and len(sys.argv) > 1:
                #support the -m syntax after "magic"
                target = sys.argv.pop(1)
                runpy.run_module(target, run_name="__main__", alter_sys=True)
            else:
                runpy.run_path(target, run_name="__main__")
    except Exception:
        main.mainloop.exception = sys.exc_info()
        raise
    finally:
        main.mainloop.running = False

if __name__ == "__main__":
    patch_all()
    install_stackless()
    stackless.tasklet(run)()
    main.mainloop.run()
