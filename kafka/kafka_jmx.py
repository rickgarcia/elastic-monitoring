#!/usr/bin/python
import os
import sys
import json
import requests
import subprocess
import threading
import time
import logging

from pprint import pprint

log = logging.getLogger(__name__)

# override some threading methods so we can easily access return values
class sthread(threading.Thread):
    # standard join always returns "None" - modify
    # join to return the stored function result
    def join(self, timeout=None):
       super(sthread, self).join(timeout)
       return self._return
    # modify "run" to store the function result
    def run(self):
       self._return = None
       if self._Thread__target is not None:
           self._return = self._Thread__target(*self._Thread__args, **self._Thread__kwargs)

# and a nice little decorator so we don't have to redeclare for every thread
def threaded(fn):
    def wrapper(*k, **kw):
        t = sthread(target=fn, args=k, kwargs=kw)
        t.setDaemon(True)
        t.start()
        return t
    return wrapper

def get_t_id():
    return repr(threading.current_thread())

# Here's the command we're emulating:
#
# java -Xmx8G -Xms8G
#  -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent
#  -Djava.awt.headless=true
#  -Dcom.sun.management.jmxremote
#  -Dcom.sun.management.jmxremote.authenticate=false
#  -Dcom.sun.management.jmxremote.ssl=false
#  -Dkafka.logs.dir=/opt/kafka/current/bin/../logs
#  -Dlog4j.configuration=file:/opt/kafka/current/bin/../etc/kafka/tools-log4j.properties
#  -cp /opt/kafka/current/bin/../share/java/kafka/*:/opt/kafka/current/bin/../share/java/confluent-support-metrics/*:/usr/share/java/confluent-support-metrics/*
# kafka.admin.ConsumerGroupCommand --bootstrap-server localhost:9092 --describe --group ocp_application_out


# TODO: output parser handling is a bit fuzzy - might be best to handle it in the subclasses
class kafka_command(object):

    def opts(self):
        # combine the cmdline options for a system call
        return ' '.join([
            ' '.join(self.jvm['HEAP_OPTS']),
            ' '.join(self.jvm['SYS_OPTS']),
            '-cp',
            ':'.join(self.jvm['CLASSPATH'])])

    def kfk_exec(self, *args):
        #exec_line = "%s %s %s" % (self.jexec, self.opts(), ' '.join(args))
        #print exec_line
        # subprocess.Popen likes everything split up - join all *args and split again
        if (not args):
            cmd_args = self.cmd.split()
        else:
            cmd_args = ' '.join(args).split()

        # split up all arguments for the subprocess.popen array
        sp_popen_args = [self.jexec] + \
            self.jvm['HEAP_OPTS'] + \
            self.jvm['SYS_OPTS'] + \
            ['-cp',':'.join(self.jvm['CLASSPATH'])] + \
            cmd_args

        log.debug("Initiating subprocess %s" % sp_popen_args)
        try:
            p = subprocess.Popen(sp_popen_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        except subprocess.CalledProcessError as e:
            log.error("subprocess\n%s\n\nfailed: %s" % (sp_popen_args, repr(e)))
            return None

        output,stderr = p.communicate()
        rc = p.returncode

        if not (rc == 0):
            # log stderr
            return None
        # it's better to handle the parsing with a subclass,
        # but if you really wanna, it'll run here too
        # Note: deprecate parser and remove from this class
        if (self.parser is None):
            return output
        else:
            return self.parser(output)

    @threaded
    def kfk_exec_t(self, *args):
        return kfk_exec_out

    def __init__(self, cmd = None, parser = None):
        # set defaults
        self.jexec = 'java'
        self.parser = parser
        self.cmd = cmd
        self.jvm = {}
        self.jvm['HEAP_OPTS'] = [
            "-Xmx256M",
            "-Xms64M",
            "-server",
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=20",
            "-XX:InitiatingHeapOccupancyPercent=35",
            "-XX:+ExplicitGCInvokesConcurrent"
        ]
        self.jvm['SYS_OPTS'] = [
            "-Djava.awt.headless=true",
            "-Dcom.sun.management.jmxremote",
            "-Dcom.sun.management.jmxremote.authenticate=false",
            "-Dcom.sun.management.jmxremote.ssl=false",
            "-Dkafka.logs.dir=/opt/kafka/current/logs",
            "-Dlog4j.configuration=file:/opt/kafka/current/etc/kafka/tools-log4j.properties"
        ]
        self.jvm['CLASSPATH'] = [
            "/opt/kafka/current/share/java/kafka/*",
            "/opt/kafka/current/share/java/confluent-support-metrics/*",
            "/usr/share/java/confluent-support-metrics/*"
        ]


# wrapper for kafka_command
class consumer_group_command(kafka_command):

    def cmd_prefix(self):
        return "%s --bootstrap-server %s:%d" % (self.kfk_cmd, self.kafka_server, self.kafka_port)

    def default_kgd_parser(self, raw_txt):
        try:
            lines = filter(None, raw_txt.split("\n"))
        except AttributeError as e:
            return None
        gd_fields = ['topic','partition','current_offset','log_end_offset','lag','consumer_id','host','client_id']
        gd = []
        # grab any threaded attributes (includes main thread, so works for single threaded too)
        t_attrs = self._t_attrs[get_t_id()]
        # first line is "TOPIC...PARTITION... etc" - skip it
        for line in lines[1:]:
            grp_desc = dict(zip(gd_fields, line.split()))
            # remove empty fields
            for gd_key in grp_desc.keys():
                if (grp_desc[gd_key] == '-'):
                    del(grp_desc[gd_key])

            grp_desc['timestamp'] = t_attrs['cmd_timestamp']
            grp_desc['group'] = t_attrs['group']
            #TODO: cleanup the host field
            gd.append(grp_desc)
        # return the group descriptions
        return gd

    # setup the description command - thread safe, but can be called
    # from main thread
    def describe(self, group):
        t_id = get_t_id()
        self._t_attrs[t_id] = {}
        raw_desc_txt = self.kfk_exec("%s --describe --group %s" % (self.cmd_prefix(), group))

        self._t_attrs[t_id]['cmd_timestamp'] = int(time.time() * 1000)
        self._t_attrs[t_id]['group'] = group

        if (self.parser is None):
            return self.default_kgd_parser(raw_desc_txt)
        else:
            return self.parser(raw_desc_txt)

    @threaded
    def describe_t(self, group):
        return self.describe(group)

    #
    # group listing defs
    def default_kgl_parser(self, raw_txt):
        return filter(None, raw_txt.split("\n"))

    #
    def group_list(self):
        raw_group_txt = self.kfk_exec("%s --list" % self.cmd_prefix())
        if (self.parser is None):
            return self.default_kgl_parser(raw_group_txt)
        else:
            return self.parser(raw_group_txt)

    #
    def __init__(self, server=None, port=None, cmd=None, parser=None):
        # not sure how I want to handle the command parsing - pass thru for now
        super(consumer_group_command, self).__init__(cmd=cmd, parser=parser)
        self.kfk_cmd = 'kafka.admin.ConsumerGroupCommand'
        # an internal object dict to handle threaded vars
        self.set_elastic_fields = True
        self._t_attrs = {}

        if (server is None):
            self.kafka_server = os.uname()[1]
        else:
            self.kafka_server = server
        if (port is None):
            self.kafka_port = 9092
        else:
            self.kafka_port = port


# parser subroutines
# kafka group list
def kgl_parser(subp_out):
    return filter(None, subp_out.split("\n"))

# kafka group description parser
def kgd_parser(subp_out):
    try:
        lines = filter(None, subp_out.split("\n"))
    except AttributeError as e:
        return None

    gd_fields = ['topic','partition','current_offset','log_end_offset','lag','consumer_id','host','client_id']
    gd = []
    # first line is "TOPIC...PARTITION... etc" - skip it
    for line in lines[1:]:
        gd.append(dict(zip(gd_fields, line.split())))
    return gd


def kafka_command_example_usage():

    # setup the command object with the arguments for a group list request
    # include a parser to translate the raw stdout txt to a python list
    kafka_server = os.uname()[1]
    kafka_port = 9092
    kafka_cmd = kafka_command(
        cmd='kafka.admin.ConsumerGroupCommand --bootstrap-server %s:%d--list' % (kafka_server, kafka_port),
        parser=kgl_parser)
    # execute the command  - kfk_exec() returns the command output
    group_list = kafka_cmd.kfk_exec()

    # threaded example of requesting descriptions for all groups in the list
    group_threads = {}
    group_description = {}
    # we can either create a new object or just change the output parser
    kafka_groups.parser = kgd_parser

    # simple thread creation - kfk_exec_t returns the thread object
    for group in group_list:
        group_threads[group] = kafka_groups.kfk_exec_t("kafka.admin.ConsumerGroupCommand --bootstrap-server %s:%d--describe --group %s" % (kafka_server, kafka_port, group)

    pprint(group_threads)

    # wait for the threads to finish, and capture the output
    # (standard threading class has been modified here to cause "join()"
    # to return the target function return value
    for group in group_list:
        group_description[group] = group_threads[group].join()
    # dump out the descriptions
    pprint(group_description)
    return


def consumer_group_command_example_usage():

    # the consumer_group_command class currently supports
    # list and describe commands - it currently sets the default
    # server:port to the hostname:9092 - this can be overridden
    # during or after object creation
    kcg_cmd = consumer_group_command()
    # group list command generated
    k_groups = kcg_cmd.group_list()
    print ("Kafka group list: %s" % repr(k_groups))

    # setup the description threads
    group_threads = {}
    for group in k_groups:
        group_threads[group] = kcg_cmd.describe_t(group)
    pprint(group_threads)

    # ...and wait for them to finish; collect the output and proceed
    group_description = {}
    for group in k_groups:
        group_description[group] = group_threads[group].join()

    pprint(group_description)
    return


def main():
    consumer_group_command_example_usage()
    sys.exit(0)


if __name__ == "__main__":
    main()
