#test
# TODO: Turn into TestClass
# TODO: Write subprocess replacement for python 3

import os
import re
import threading

from ..replacements.popen import FileChannel


os_popen4 = getattr(os, "popen4", None)
def popen4(cmd, mode='t', bufsize=-1):
    #no stdin support yet
    pstdin, pstdout = FileChannel(), FileChannel()

    def func():
        try:
            for i in range(10):
                l = ("%d"%i)*10 + "\n"
                #print "sending", l
                #print pstdout.balance, pstdout.closed, pstdout.closing
                pstdout.send(l)
        except Exception, e:
            c, e = sys.exc_info()[:2]
            import traceback
            traceback.print_exc()
            pstdout.send_exception(c, e)
        finally:
            #print "done"
            pstdout.close()
    t = threading.Thread(target=func)
    t.start()
    return pstdin, pstdout


def read_process(cmd, args=""):
    pipein, pipeout = popen4("%s %s" % (cmd, args))
    try:
        firstline = pipeout.readline()
        if (re.search(r"(not recognized|No such file|not found)", firstline,
                      re.IGNORECASE)):
            raise IOError('%s must be on your system path.' % cmd)
        output = firstline + pipeout.read()
    finally:
        pipeout.close()
    return output


done = False
def foo():
    try:
        output = read_process("foo")
        # print "got output", repr(output)
    finally:
        global done
        done = True

if __name__ == "__main__":
    import stackless
    stackless.tasklet(foo)()
    while not done:
        stackless.run()
