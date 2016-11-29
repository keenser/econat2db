#!/usr/bin/env python3.5
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import asyncio
import socket
from collections import namedtuple
import time
import logging
import logging.handlers
import json

version = "1.0-0"
servicename = "econat"
servicedesc = "econat activator"

class User(namedtuple('User', ('id', 'ip', 'speed', 'service', 'port', 'status'))):
    def __new__(cls, id=None, ip=None, speed=None, service=None, port=None, status=None):
        return super(User, cls).__new__(cls, id, ip, speed, service, port, status)

def dict_compare(d1, d2):
    d1_keys = set(d1.keys())
    d2_keys = set(d2.keys())
    intersect_keys = d1_keys.intersection(d2_keys)
    added = d1_keys - d2_keys
    removed = d2_keys - d1_keys
    modified = {o for o in intersect_keys if d1[o] != d2[o]}
    return added, removed, modified

import aiopg
import psycopg2
class DbInfo:
    """
    radius=> select * from active_users;
     id |     ip     |  speed   | service |                port                | status
    ----+------------+----------+---------+------------------------------------+--------
      1 | 100.64.1.3 | 20971520 | 20Mbit  | eltex-1-1 eth 100/2:100            | 1
      2 | 100.64.3.2 | 10485760 | 10Mbit  | eltex-1-1 eth 100/1:100            | 1
      2 | 100.64.1.4 | 10485760 | 10Mbit  | eltex-1-1 eth 100/1:100            | 1
      3 | 100.64.2.2 | 20971520 | 20Mbit  | access-1 GigabitEthernet0/0/21:100 | 0
    """
    def __init__(self, loop, user='radius', password='radius', dbname='radius'):
        self.logid = 'DbInfo'
        self.log = logging.getLogger(self.logid)

        self.loop = loop
        self.user = user
        self.password = password
        self.dbname = dbname
        self.queuetask = None

    @property
    def queue(self):
        if not self.__queue:
            self.queue = asyncio.Queue()
        return self.__queue

    @queue.setter
    def queue(self, queue):
        self.__queue = queue
        self.queuetask = self.loop.create_task(self.listen())

    def connect(self):
        self.log.info('connecting to db %s', self.dbname)
        return aiopg.connect(dsn='dbname={dbname} user={user} password={password}'.format(dbname=self.dbname, user=self.user, password=self.password))

    def stop(self):
        if self.queuetask:
            self.queuetask.cancel()

    async def execute(self, query, args=None):
        try:
            async with self.connect() as db:
                async with db.cursor() as cur:
                    await cur.execute(query, args)
                    return await cur.fetchall()
        except psycopg2.Error as e:
            self.log.error('execute: %s %s', query, e)

    async def listen(self):
        while True:
            try:
                async with self.connect() as db:
                    self.log.info('listen econat_notify')
                    async with db.cursor() as cur:
                        await cur.execute("LISTEN econat_notify")
                        while True:
                            msg = await db.notifies.get()
                            self.log.log(1, '%s', msg)
                            try:
                                command = json.loads(msg.payload)
                                await self.queue.put(command)
                            except json.decoder.JSONDecodeError as e:
                                self.log.error('receive %s %s', e, data)
            except psycopg2.Error as e:
                self.log.error('listen: %s', e)
                await asyncio.sleep(10)
            else:
                break

    async def rid(self):
        data = await self.execute('select user_id, service_id from user_service where status = 1')
        if data:
            return dict(data)

    async def userlist(self, user_tmpl, identificator, users):
        if users:
            fetchall = await self.execute("select %s from active_users where %s in ('%s')" 
                        % (','.join(user_tmpl._fields), identificator, "','".join(map(str, users)))
                    )
            self.log.log(1, "ret %s", fetchall)
            return map(user_tmpl._make, fetchall)
        return []

class TestUsers:
    def stop(self):
        pass
    def rid(self):
        userlist = {}
        for i in range(1, 16):
            userlist[i] = (i % 4)+1
        return userlist
    def userlist(self, user_tmpl, identificator, users):
        userlist = []
        for i in users:
                userlist.append(User(id=i,ip='100.%d.%d.%d' % (64 + i % (65536 * 256) // 65536, i % 65536 // 256, i % 256), speed='%dM' % ((i % 4) + 1), service=((i % 4) + 1), port='-'))
        return userlist

class EcoNat:
    user = User
    def __init__(self, loop, server = '192.168.100.200', port = 2225):
        self.logid = 'EcoNat'
        self.log = logging.getLogger(self.logid)

        self.loop = loop
        self.server = server
        self.port = port

    def stop(self):
        pass

    async def rid(self):
        data = await self.ask(b'testRID\n')
        return dict(map(int, i.split(b'-')) for i in data.split())

    async def processusers(self, action_users={}):
        mlist = []
        mlist.append(self.addformat(action_users.get('add')))
        mlist.append(self.delformat(action_users.get('del')))
        data = ''.join(mlist).encode()
        if data:
            await self.ask(data, read=False)

    async def ask(self, message, read=True):
        data = b''
        try:
            self.log.debug('open connection to %s:%d', self.server, self.port)
            reader, writer = await asyncio.open_connection(self.server,self.port, loop=self.loop)
            self.log.debug('send %s', message)
            writer.write(message)
            if read:
                data = await reader.readline()
                self.log.debug('recive %s', data)
            self.log.debug('close connection to %s:%d', self.server, self.port)
            writer.close()
        except OSError as e:
            self.log.error('testRID connecting to %s:%d %s', self.server, self.port, e)
        return data

    def addformat(self, userlist):
        return ''.join('add\t{0}\t{{oid}} LIM{1}/LIM{1} {2}, // RULE{3}\n'.format(user.id, user.speed, user.ip, user.service) for user in userlist)

    def delformat(self, useridlist):
        return ''.join('remove\t{0}\t\n'.format(user.id) for user in useridlist)

from pyrad import dictionary, packet
class RadiusHandler:
    def __init__(self, loop, message):
        self.logid = 'RadiusHandler{}'.format(message.get(1))
        self.log = logging.getLogger(self.logid)
        self.loop = loop
        self.message = message
        self.transport = None
        self.data = None
        self.scheduler = None
        self.retransmit = 3

    def send(self):
        if self.retransmit:
            self.log.debug('send %s', self.message)
            self.retransmit -= 1
            if not self.data:
                self.data = self.message.RequestPacket()
            self.transport.sendto(self.data)
            self.scheduler = self.loop.call_later(2, self.send)
        else:
            self.transport.close()

    def connection_made(self, transport):
        self.log.debug('Connection made')
        self.transport = transport
        self.send()

    def datagram_received(self, data, addr):
        pkt = self.message.CreateReply(packet=data)
        self.log.info("Received: %d", pkt.code)
        if self.scheduler:
            self.scheduler.cancel()
        self.transport.close()

    def error_received(self, exc):
        self.log.error('Error received: %s', exc)

    def connection_lost(self, exc):
        self.log.debug("Socket closed")

class RadiusClient:
    raddict = dictionary.Dictionary("/usr/share/freeradius/dictionary.rfc2865")
    user = namedtuple('User', ('ip'))
    def __init__(self, loop, server='192.168.100.200', port=1812, secret=b''):
        self.loop = loop
        self.logid = 'RadiusClient'
        self.log = logging.getLogger(self.logid)
        self.server = server
        self.port = port
        self.secret = secret
        self.message = packet.AcctPacket(code=packet.DisconnectRequest, dict=self.raddict, secret=self.secret)

    def stop(self):
        pass

    async def processusers(self, action_users={}):
        for action, userlist in action_users.items():
            for user in userlist:
                self.log.info('send radius_disconnect for %s', user.ip)
                self.message['User-Name'] = user.ip
                await self.loop.create_datagram_endpoint(lambda: RadiusHandler(self.loop, self.message), remote_addr=(self.server, self.port))

import os
class UnixSocket:
    def __init__(self, loop, file=None):
        self.logid = 'UnixSocket'
        self.log = logging.getLogger(self.logid)

        self.filename = file

        self.socket = socket.socket( socket.AF_UNIX, socket.SOCK_DGRAM )
        if os.path.exists(self.filename):
            os.unlink(self.filename)
        self.socket.bind(self.filename)
        os.chmod(self.filename,0o1777)
        self.loop = loop
        self.transport = self.loop.create_datagram_endpoint(lambda: self, sock=self.socket)
        self.loop.run_until_complete(self.transport)

    @property
    def queue(self):
        if not self.__queue:
            self.queue = asyncio.Queue()
        return self.__queue

    @queue.setter
    def queue(self, queue):
        self.__queue = queue

    def connection_made(self, transport):
        pass

    def datagram_received(self, data, addr):
        self.log.log(1, 'notify %s', data)
        try:
            command = json.loads(data.decode())
            self.queue.put_nowait(command)
        except json.decoder.JSONDecodeError as e:
            self.log.error('receive %s %s', e, data)

    def stop(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)
        if self.transport:
            self.transport.close()

#import stat
#import fcntl
#import pyinotify
#class FifoPipe(pyinotify.ProcessEvent):
#    def my_init(self, loop, file=None):
#        self.loop = loop
#        if not file:
#            raise ValueError("file keyword argument must be provided")
#
#        self.filename = file
#
#        if not os.path.exists(self.filename):
#            os.mkfifo(self.filename)
#
#        if not stat.S_ISFIFO(os.stat(self.filename).st_mode):
#            raise TypeError("File %s is not a fifo file" % self.filename)
#
#        self.fd = open(os.open(self.filename, os.O_RDONLY|os.O_NONBLOCK), 'r')
#        self.queue = None
#        wm = pyinotify.WatchManager()
#        wm.add_watch(self.filename, pyinotify.IN_MODIFY)
#        notifier = pyinotify.AsyncioNotifier(wm, self.loop, default_proc_fun=self)
#
#    def process_IN_MODIFY(self, event):
#        for data in self.fd:
#            self.queue.put_nowait(data)
#
#    def stop(self):
#        self.fd.close()
#
#    @property
#    def queue(self):
#        if not self.__queue:
#            self.__queue = asyncio.Queue()
#        return self.__queue
#
#    @queue.setter
#    def queue(self, queue):
#        self.__queue = queue

class Communicator:
    def __init__(self, loop, options):
        self.logid = 'Communicator'
        self.log = logging.getLogger(self.logid)

        self.loop = loop
        self.queuetask = None
        self.queue = asyncio.Queue()

        if options.rid:
            self.nat = EcoNat(loop, server=options.server)
        else:
            self.nat = RadiusClient(loop, server=options.server, port=options.port, secret=options.secret.encode())
        self.db = DbInfo(loop, user=options.user, password=options.password, dbname=options.dbname)
        self.db.queue = self.queue

        if options.rid:
            self.ridtask = self.loop.create_task(self.rid())
        else:
            self.ridtask = None

    @property
    def queue(self):
        if not self.__queue:
            self.queue = asyncio.Queue()
        return self.__queue

    @queue.setter
    def queue(self, queue):
        self.__queue = queue
        self.queuetask = self.loop.create_task(self.queueloop())

    def stop(self):
        if self.queuetask:
            self.queuetask.cancel()
        if self.ridtask:
            self.ridtask.cancel()
        if self.db:
            self.db.stop()
        if self.nat:
            self.nat.stop()

    async def rid(self):
        while True:
            try:
                dbrid = await self.db.rid()
                if dbrid == None:
                    self.log.info('no dbrid info from database')
                    continue 
                self.log.debug('dbrid %s', dbrid)
                natrid = await self.nat.rid()
                self.log.debug('natrid %s', natrid)
                added, removed, modified = dict_compare(dbrid,natrid)
                self.log.debug('added %s', added)
                self.log.debug('removed %s', removed)
                self.log.debug('modified %s', modified)

                mlist = {}
                if removed:
                    mlist['del'] = {'id': removed}

                toadd = added.union(modified)
                if toadd:
                    mlist['add'] = {'id': toadd}

                if mlist:
                    await self.queue.put(mlist)
            finally:
                self.log.debug('sleep %d', 30)
                await asyncio.sleep(30)

    async def queueloop(self):
        pool = {}
        while True:
            self.log.log(1, 'queueloop wait')
            part = await self.queue.get()
            self.log.debug('part %s', str(part))
            try:
                # get all data from queue:
                if not isinstance(part, dict):
                    continue
                for action, users in part.items():
                    if not isinstance(users, dict):
                        continue
                    for ids, values in users.items():
                        if isinstance(values, (int, str)):
                            values = [values]
                        pool.setdefault(action, {}).setdefault(ids, set()).update(values)

                # get info from db:
                if self.queue.empty():
                    self.log.debug('pool %s', str(pool))
                    action_users = {}
                    for action, users in pool.items():
                        for ids, values in users.items():
                            #userlist = None
                            #if action == 'add':
                                #userlist = await self.db.userlist(ids, values)
                            #elif action == 'del':
                                #if ids == 'id':
                                #    userlist = {User(id=userid) for userid in values}
                                #else:
                                #   userlist = await self.db.userlist(ids, values)
                            userlist = await self.db.userlist(self.nat.user, ids, values)
                            if userlist:
                                action_users.setdefault(action, []).extend(userlist)
                    pool.clear()

                    # send to activator
                    if action_users:
                        await self.nat.processusers(action_users)
            except Exception as e:
                self.log.error('queueloop: %s', e)
            self.log.log(1, 'queueloop done')

def getoptions():
    import argparse
    parser = argparse.ArgumentParser(
        description="{0} {1}".format(servicedesc, version))

    parser.add_argument("-v", "--verbose",
        dest="verbosity",
        action="count",
        help="print more diagnostic messages (option can be given multiple times)",
        default=0
    )

    parser.add_argument("-l", "--log",
        dest="logfile",
        nargs="?",
        help="log file, default: %(default)s, %(const)s if enabled",
        const="/var/log/{0}.log".format(servicename)
    )

    parser.add_argument("-s", "--syslog",
        dest="syslog",
        action="store_true",
        help="log to syslog (default off)",
        default=False
    )

    parser.add_argument("-p", "--pid",
        dest="pid",
        nargs="?",
        help="pid file, default: %(default)s, %(const)s if enabled",
        const="/var/run/{0}.pid".format(servicename)
    )

    parser.add_argument("-f", "--foreground",
        dest="foreground",
        action="store_true",
        help="stay in foreground (default off)",
        default=False
    )

    parser.add_argument("--socket",
        dest="pipe",
        help="pipe file, default: %(default)s",
        default="/var/run/{0}/{0}.socket".format(servicename)
    )

    group = parser.add_argument_group(
        "econat server",
        "econat server settings"
    )

    group.add_argument("--rid",
        dest="rid",
        action="store_true",
        help="use RID insted of radius, default: use radius",
        default=False
    )

    group.add_argument("--server",
        dest="server",
        help="econat server ip, default: %(default)s",
        default="192.168.100.200"
    )

    group.add_argument("--port",
        dest="port",
        type=int,
        help="econat server port, default: %(default)s",
        default=1812
    )

    group.add_argument("--secret",
        dest="secret",
        help="econat radius coa_password, default: %(default)s",
        default="radius"
    )

    group = parser.add_argument_group(
        "database",
        "Postgresql database connection settings"
    )

    group.add_argument("--user",
        dest="user",
        help="database user name, default: %(default)s",
        default="radius"
    )

    group.add_argument("--password",
        dest="password",
        help="database password, default: %(default)s",
        default="radius"
    )

    group.add_argument("--dbname",
        dest="dbname",
        help="database name to connect to, default: %(default)s",
        default="radius"
    )

    return parser.parse_args()

import signal
import os
import sys
import atexit
class daemon:
    """A generic daemon class.

    Usage: subclass the daemon class and override the run() method."""

    def __init__(self, pidfile): self.pidfile = pidfile

    def daemonize(self, secondfork=False):
        """Deamonize class. UNIX double fork mechanism."""

        try:
            pid = os.fork()
            if pid > 0:
                # exit first parent
                sys.exit(0)
        except OSError as err:
            sys.stderr.write('fork #1 failed: {0}\n'.format(err))
            sys.exit(1)

        # decouple from parent environment
        os.chdir('/')
        os.setsid()
        os.umask(0)

        # do second fork
        if secondfork:
            try:
                pid = os.fork()
                if pid > 0:

                    # exit from second parent
                    sys.exit(0)
            except OSError as err:
                sys.stderr.write('fork #2 failed: {0}\n'.format(err))
                sys.exit(1)

        # write pidfile
        try:
            if self.pidfile:
                pid = str(os.getpid())
                with open(self.pidfile, 'w+') as f:
                    f.write(pid + '\n')
                atexit.register(self.delpid)
        except PermissionError:
            sys.stderr.write('can not write pidfile %s\n' % self.pidfile)
            sys.exit(1)

        # redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = open(os.devnull, 'r')
        so = open(os.devnull, 'a+')
        se = open(os.devnull, 'a+')

        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

    def delpid(self):
        os.remove(self.pidfile)

    def start(self, foreground):
        """Start the daemon."""

        # Check for a pidfile to see if the daemon already runs
        try:
            with open(self.pidfile, 'r') as pf:
                pid = int(pf.read().strip())
        except (IOError, TypeError, ValueError):
            pid = None

        if pid:
            message = "pidfile {0} already exist. " + \
                    "Daemon already running?\n"
            sys.stderr.write(message.format(self.pidfile))
            sys.exit(1)

        # catch TERM
        signal.signal(signal.SIGTERM, lambda signum, stack_frame: sys.exit(0))

        # Start the daemon
        if not foreground:
            self.daemonize()

def main():
    options = getoptions()

    dm = daemon(options.pid)
    dm.start(options.foreground)

    if options.verbosity > 3:
        options.verbosity = 3

    level = (
        logging.WARNING,
        logging.INFO,
        logging.DEBUG,
        logging.NOTSET,
        )[options.verbosity]

    logger = logging.getLogger('')
    logger.addHandler(logging.NullHandler())
    logger.setLevel(level)
    logformat = '%(asctime)s %(levelname)s:%(name)s: %(message)s'

    if options.logfile:
        filelogger = logging.handlers.WatchedFileHandler(options.logfile)
        filelogger.setFormatter(logging.Formatter(logformat))
        logger.addHandler(filelogger)

    if options.syslog:
        syslogger = logging.handlers.SysLogHandler(address = '/dev/log', facility = logging.handlers.SysLogHandler.LOG_LOCAL0)
        syslogger.setFormatter(logging.Formatter('%(name)s: %(message)s'))
        logger.addHandler(syslogger)

    if options.foreground:
        conslogger = logging.StreamHandler()
        conslogger.setFormatter(logging.Formatter(logformat))
        logger.addHandler(conslogger)

    sys.excepthook = lambda excType, excValue, traceback: logging.getLogger('exception').error("Uncaught exception", exc_info=(excType, excValue, traceback))

    log = logging.getLogger(servicename)

    log.info("starting %s version %s", servicename, version)

    loop = asyncio.get_event_loop()
    def callback(loop, exception):
        log.error('ex: %s', exception)
    loop.set_exception_handler(callback)

    #fifo = UnixSocket(loop, file=options.pipe)
    #fifo = FifoPipe(loop, file=options.pipe)
    com = Communicator(loop, options)
    #fifo.queue = com.queue

    log.info('run loop')
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        com.stop()
        #fifo.stop()
        loop.stop()
        loop.run_forever()      # tasks cancel finishing
        loop.close()
        log.info("exit")

if __name__ == '__main__':
    main()
