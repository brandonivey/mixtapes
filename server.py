#!/usr/bin/python
import sys


class Log:
    """
    Allows log files to replace sys.stderr or sys.stdout
    If multipule file discriptors are passed to __init__, it will write
    to each one
    """
    def __init__(self, *fds):
        self.fds = fds

    def write(self, data):
        for fd in self.fds:
            fd.write(data)
            fd.flush()

    def close(self):
        for fd in self.fds:
            try:
                fd.close()
            except AttributeError:
                pass


# This odd structure is me trying to get imported modules to write errors
# correctly when logging is enable
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Processes an approved\
        mixtape ZIP file and uploads it to S3')
    parser.add_argument('-d', '--keep-dirs', action="store_true", default=False,
        help="Keep the temporary directories instead of deleteing")
    parser.add_argument('-k', '--keep-orig', action="store_true",
        default=False, help='Keep original ZIP')
    parser.add_argument('-o', '--output',help="If this is set, program will\
     output to given file instead of to STDOUT")
    parser.add_argument("--save-rest", action="store_false", default=False,
        help="Don't wipe the directory of non-ZIP files")

    # Makes a command line interface with arguments
    args = vars(parser.parse_args())

    if args["output"]:
        # if there is an output file passed
        output = open(args["output"], "a")
        log = Log(sys.stdout, output)
        sys.stdout = log
        sys.stderr = log
    # all arguments are passed to process_zip, and it will not accept "output"
    # process.process_mixtape will try to find args, we need to give it
    del args["output"]


from twisted.internet import reactor, protocol
from twisted.internet.defer import DeferredQueue, DeferredSemaphore
from twisted.internet.threads import deferToThread
from process import debug
import process
import os
import json

# A note about python syntax
#
# This script makes frequant use of * and ** in functions funcs.
# func(*[1, 2, 3]) is the exact same thing as func(1, 2, 3)
# func(*{'foo': 1, 'bar': 2, 'baz': 3}) is the exact same thing as:
# func(foo=1, bar=2, baz=3)


class Processor():
    """
    Whenever mixtapeReceived is called, deferToThread is scheduled to be run as
    soon as a "slot" for being run is available. There is currently 1 slot
    deferToThread runs process_mixtape in another thread, and releases the
    slot when its that process is done
    """
    def __init__(self):
        self.sem = DeferredSemaphore(1) #do one thing at a time

    def mixtapeReceived(self, mixtape):
        debug("Adding %s to be processed" % mixtape)
        self.sem.run(deferToThread, process.process_mixtape, *mixtape)
        # DeferredSemaphore.run will not .release() until the defer returned by
        # deferToThread fires


class AddToQueue(protocol.Protocol):
    """
    Whenever someone connects, an instance of this protocol is made that
    describes how to interact with them
    """
    processor = Processor()

    def __init__(self):
        self.info = ""

    def connectionMade(self):
        debug("Connection made")

    def dataReceived(self, data):
        """
        This method is called whenever the client sends data
        We are trying to get a number enclosed in square braces, because that's
        easy to parse using JSON (JavaScript Object Notation)
        """
        debug("Data received: %s" % data)
        self.info += data
        if self.info.endswith("]"):
            try:
                # Parses the recieved information
                info = json.loads(self.info)
                # Verify that it's exactly what we want
                if type(info[0]) is not int:
                    raise Exception("ID %s is not int" % type(info[0]))
                if len(info) is not 1:
                    raise Exception("%s args, expected exactly 1" % len(info))
                self.processor.mixtapeReceived(info)
                self.transport.write("OK")
            except (ValueError, IndexError, Exception) as e:
                # In the case of JSON not being able to parse, in the case of
                # info[0] not making sense, or in the case of my own errors
                self.transport.write(e.message)
                debug("Error!" + str(e))
            finally:
                debug("Ending connection")
                self.transport.loseConnection()


def verify_mixtape_counter():
    """
    Ensures that a mixtape.counter file exists at the path of the script
    See process.Connection.__enter__ for better documentation of this stuff
    """
    pathbase = os.path.dirname(__file__)
    pathbase = pathbase if pathbase else '.'

    try:
        f = open(os.path.join(pathbase, "mixtapes.counter"), 'r')
        int(f.read())
    except (OSError, IOError, Exception):
        f = open(os.path.join(pathbase, "mixtapes.counter"), 'w')
        f.write("0")
        f.close()


def main():
    """This runs the the above on port 8000"""
    factory = protocol.ServerFactory()
    factory.protocol = AddToQueue
    reactor.listenTCP(8000,factory)
    verify_mixtape_counter()
    process.reactor = reactor
    reactor.run()
    # Don't try to understand this.


if __name__ == "__main__":
    # this next part will run main(), and always close output if it exists but
    # not raise an error if it does not
    process.args = args
    try:
        main()
    finally:
        try:
            output.close()
        except NameError:
            pass
