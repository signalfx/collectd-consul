#!/usr/bin/env python
import collectd
import json
import logging
import re
import socket
import threading
import time
import urllib2
import urllib_ssl_handler


PLUGIN = 'consul'


def _check_split(splitted_list):
    '''
    checks for malformed data on split
    '''
    if len(splitted_list) == 2 and '' not in splitted_list:
        return True
    return False


def _format_dimensions(dimensions):
    '''
    Formats a dictionary as a comma-delimited list of key=value tokens.
    This was copied from docker-collectd-plugin.
    '''
    return ','.join(['='.join((key.replace('.', '_'), value))
                    for key, value in dimensions.iteritems()])


def _dimensions_str_to_dict(dimensions_str):

    dimensions = {}
    dimensions_list = dimensions_str.strip().split(',')

    for dimension in dimensions_list:
        key_val_split = dimension.strip().split('=')
        if _check_split(key_val_split):
            dimensions[key_val_split[0]] = key_val_split[1]
        else:
            LOGGER.warning('Malformed dimension key=value pair: {0}'
                           .format(key_val_split))

    return dimensions


def _str_to_bool(value):
    '''
    Python 2.x does not have a casting mechanism for booleans.  The built in
    bool() will return true for any string with a length greater than 0.  It
    does not cast a string with the text "true" or "false" to the
    corresponding bool value.  This method is a casting function.  It is
    insensitive to case and leading/trailing spaces.  An Exception is raised
    if a cast can not be made.
    This was copied from docker-collectd-plugin.py
    '''
    value = str(value).strip().lower()
    if value == 'true':
        return True
    elif value == 'false':
        return False
    else:
        raise ValueError('Unable to cast value (%s) to boolean' % value)


def compute_rtt(coord_a, coord_b):
    '''
    Computes network round trip time between nodes using network coordinates.
    https://www.consul.io/docs/internals/coordinates.html
    '''
    sum_sq = 0.0
    for vec_a, vec_b in zip(coord_a['Vec'], coord_b['Vec']):
        diff = vec_a - vec_b
        sum_sq += diff**2

    rtt = sum_sq**0.5 + coord_a['Height'] + coord_b['Height']

    adjusted = rtt + coord_a['Adjustment'] + coord_b['Adjustment']
    if adjusted > 0:
        rtt = adjusted
    rtt_milliseconds = rtt * 1000
    return rtt_milliseconds


class CollectdLogHandler(logging.Handler):
    '''
    Log handler to forward statements to collectd
    A custom log handler that forwards log messages raised
    at level debug, info, warning, and error
    to collectd's built in logging.  Suppresses extraneous
    info and debug statements using a "verbose" boolean
    Inherits from logging.Handler
    This was copied from docker-collectd-plugin.py
    Arguments
        plugin_name -- name of the plugin (default 'unknown')
        verbose -- enable/disable verbose messages (default False)
    '''

    def __init__(self, plugin_name='unknown', debug=False):
        '''
        Initializes CollectdLogHandler
        Arguments
            plugin_name -- string name of the plugin (default 'unknown')
            debug  -- boolean to enable debug level logging, defaults to false
        '''
        self.plugin_name = plugin_name
        self.enable_debug = debug

        logging.Handler.__init__(self, level=logging.NOTSET)

    def emit(self, record):
        '''
        Emits a log record to the appropriate collectd log function
        Arguments
        record -- str log record to be emitted
        '''
        if record.msg is not None:
            if record.levelname == 'ERROR':
                collectd.error(self.format(record))
            elif record.levelname == 'WARNING':
                collectd.warning(self.format(record))
            elif record.levelname == 'INFO':
                collectd.info(self.format(record))
            elif record.levelname == 'DEBUG' and self.enable_debug:
                collectd.debug(self.format(record))


class MetricDefinition(object):
    '''
    Struct for information needed to build a metric.
    Constructor Arguments:
        metric_name: The name of the metric
        metric_type: The kind of metric, e.g. guage or counter
    '''

    def __init__(self, metric_name, metric_type):
        self.name = metric_name
        self.type = metric_type


class MetricRecord(object):
    '''
    Struct for all information needed to emit a single collectd metric.
    MetricSink is the expected consumer of instances of this class.
    Taken from collectd-nginx-plus plugun.
    '''
    TO_STRING_FORMAT = '[name={},type={},value={},dimensions={},timestamp={}]'

    def __init__(self, metric_name, metric_type, value,
                 dimensions=None, timestamp=None):
        self.name = metric_name
        self.type = metric_type
        self.value = value
        self.dimensions = dimensions or {}
        self.timestamp = timestamp or time.time()

    def to_string(self):
        return MetricRecord.TO_STRING_FORMAT.format(self.name,
                                                    self.type,
                                                    self.value,
                                                    self.instance_id,
                                                    self.dimensions,
                                                    self.timestamp)


class MetricSink(object):
    '''
    Responsible for transforming and dispatching a MetricRecord via collectd.
    Taken from collectd-nginx-plus plugun.
    '''
    def emit(self, metric_record):
        '''
        Construct a single collectd Values instance from the given MetricRecord
        and dispatch.
        '''
        emit_value = collectd.Values()

        emit_value.time = metric_record.timestamp
        emit_value.plugin = PLUGIN
        emit_value.values = [metric_record.value]
        emit_value.type = metric_record.type
        emit_value.type_instance = metric_record.name
        emit_value.plugin_instance = '[{0}]'.format(
            _format_dimensions(metric_record.dimensions))

        # With some versions of CollectD, a dummy metadata map must be added
        # to each value for it to be correctly serialized to JSON by the
        # write_http plugin. See
        # https://github.com/collectd/collectd/issues/716
        emit_value.meta = {'true': 'true'}

        emit_value.dispatch()


class UDPServer(threading.Thread):
    '''
    Class to collect telemetry data from Consul sent via UDP packets
    Creates a thread which receives packets from Consul agent on a UDP socket.
    The timeout interval ensures that the thread does not block on receive call
    '''
    def __init__(self, host, port,
                 default_regex,
                 enhanced_metrics=False,
                 include_regex=None,
                 exclude_regex=None,
                 max_buffer_size=1432, timeout_interval=20):

        threading.Thread.__init__(self)
        self._host = host
        self._port = port
        self._bufsize = max_buffer_size
        self._timeout = timeout_interval
        self.default_regex = default_regex
        self.include_regex = include_regex
        self.enhanced_metrics = enhanced_metrics
        self.exclude_regex = exclude_regex
        self.lock = threading.Lock()
        self.stats = {}
        self.terminate = threading.Event()
        self.read_complete = threading.Event()
        self.metrics = {}
        self.timers = {}
        self.start()

    def run(self):

        LOGGER.info('Starting UDP server on {0}:{1} '
                    'to collect Consul\'s telemetry data. Timeout set to {2}.'
                    .format(self._host, self._port, self._timeout))

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.bind((self._host, self._port))
            self.socket.settimeout(self._timeout)

            while not self.terminate.isSet():
                try:
                    data, addr = self.socket.recvfrom(self._bufsize)
                except socket.timeout:
                    LOGGER.warn('UDP Server timed out waiting for '
                                'telemetry data on {0}:{1}.'
                                .format(self._host, self._port))
                    continue

                self.sanitize_data(data)

                with self.lock:
                    if self.read_complete.isSet():
                        self.metrics.clear()
                        self.timers.clear()
                        self.read_complete.clear()
                    else:
                        self.stats.update(self.metrics)

            # Terminate event is set. Close the socket and exit.
            self.socket.close()
            LOGGER.info('Exiting UDP Server thread.')

        except socket.error, e:
            LOGGER.error('Error in UDP socket at {0}:{1} : {2}'
                         .format(self._host, self._port, e))

    def sanitize_data(self, data):
        '''
        Sanitizes the data from udp packets.
        '''
        for datapoint in data.splitlines():
            metric_split = datapoint.split(':')
            if not (len(metric_split) == 2 and '' not in metric_split):
                LOGGER.warning('Malformed metric record from UDP packet. '
                               'Increase max udp buffer size.')
                continue
            metric_name = metric_split.pop(0)
            metric_split = metric_split[0].split('|')
            if not (len(metric_split) == 2 and '' not in metric_split):
                # In case the packet was truncated while receiving.
                LOGGER.warning('Malformed metric record from UDP packet. '
                               'Increase max udp buffer size.')
                continue
            if not self._include_metric(metric_name):
                continue
            metric_type = 'gauge'
            if metric_split[1] == 'c' and metric_name in self.metrics:
                self.metrics[metric_name] = {
                                    'type': metric_type,
                                    'value': self.metrics[metric_name]['value']
                                    + int(float(metric_split[0])),
                                    'timestamp': time.time()}
            elif metric_split[1] in ['c', 'g']:
                if metric_split[1] != 'c':
                    val = float(metric_split[0])
                else:
                    val = int(float(metric_split[0]))
                self.metrics[metric_name] = {'type': metric_type,
                                             'value': val,
                                             'timestamp': time.time()}
            elif metric_split[1] == 'ms':
                self.timers[metric_name] = self.timers.get(metric_name, [])
                self.timers[metric_name].append(float(metric_split[0]))
                val = reduce(lambda x, y: x + y, self.timers[metric_name])
                val = val/len(self.timers[metric_name])
                self.metrics['{0}.avg'.format(metric_name)] = {
                                                    'type': metric_type,
                                                    'value': val,
                                                    'timestamp': time.time()}
                self.metrics['{0}.min'.format(metric_name)] = {
                                        'type': metric_type,
                                        'value': min(self.timers[metric_name]),
                                        'timestamp': time.time()}
                self.metrics['{0}.max'.format(metric_name)] = {
                                        'type': metric_type,
                                        'value': max(self.timers[metric_name]),
                                        'timestamp': time.time()}

    def _include_metric(self, metric_name):
        if self.default_regex.search(metric_name) is not None:
            return True
        elif self.enhanced_metrics:
            if self.exclude_regex is None or \
              (self.exclude_regex is not None and
               self.exclude_regex.match(metric_name) is None):
                return True
        elif (self.include_regex is not None and
              self.include_regex.match(metric_name) is not None):
            return True
        return False


consul_server_state = MetricDefinition('consul.is_leader', 'gauge')
consul_peers = MetricDefinition('consul.peers', 'gauge')

catalog_services = MetricDefinition('consul.catalog.services.total',
                                    'gauge')
catalog_nodes = MetricDefinition('consul.catalog.nodes.total',
                                 'gauge')
catalog_nodes_by_service = MetricDefinition('consul.catalog.nodes_by_service',
                                            'gauge')
catalog_services_by_node = MetricDefinition('consul.catalog.services_by_node',
                                            'gauge')

dc_network_latency_avg = MetricDefinition('consul.network.dc.latency.avg',
                                          'gauge')
dc_network_latency_min = MetricDefinition('consul.network.dc.latency.min',
                                          'gauge')
dc_network_latency_max = MetricDefinition('consul.network.dc.latency.max',
                                          'gauge')

node_network_latency_avg = MetricDefinition('consul.network.node.latency.avg',
                                            'gauge')
node_network_latency_min = MetricDefinition('consul.network.node.latency.min',
                                            'gauge')
node_network_latency_max = MetricDefinition('consul.network.node.latency.max',
                                            'gauge')

health_services_passing = MetricDefinition('consul.health.services.passing',
                                           'gauge')
health_services_warning = MetricDefinition('consul.health.services.warning',
                                           'gauge')
health_services_critical = MetricDefinition('consul.health.services.critical',
                                            'gauge')

health_nodes_passing = MetricDefinition('consul.health.nodes.passing',
                                        'gauge')
health_nodes_warning = MetricDefinition('consul.health.nodes.warning',
                                        'gauge')
health_nodes_critical = MetricDefinition('consul.health.nodes.critical',
                                         'gauge')

default_telemetry = ['consul.raft.state.leader',
                     'consul.raft.state.candidate',
                     'consul.raft.leader.lastContact',
                     'consul.raft.leader.dispatchLog',
                     'consul.raft.commitTime',
                     'consul.raft.apply',
                     'consul.raft.replication.appendEntries.rpc',
                     'consul.rpc.query',
                     'consul.consul.leader.reconcile',
                     'consul.serf.events',
                     'consul.serf.queue.Event',
                     'consul.serf.queue.Query',
                     'consul.serf.member.join',
                     'consul.serf.member.left',
                     'runtime.heap_objects',
                     'runtime.alloc_bytes',
                     'runtime.num_goroutines',
                     'consul.dns.domain_query',
                     'consul.dns.ptr_query',
                     'consul.dns.stale_queries',
                     'consul.serf.member.flap',
                     'consul.memberlist.msg.suspect']


def configure_callback(conf):
    '''
    Configures plugin with config provided from collectd.
    '''
    LOGGER.info('Starting Consul Plugin configuration.')
    # Default values of config options
    api_host = 'localhost'
    api_port = 8500
    api_protocol = 'http'
    telemetry_server = False
    telemetry_host = 'localhost'
    telemetry_port = 8125
    acl_token = None
    sfx_token = None
    ssl_certs = {'ca_cert': None, 'client_cert': None, 'client_key': None}
    enhanced_metrics = False
    exclude_metrics = []
    include_metrics = []
    custom_dimensions = {}
    default_telemetry_regex = re.compile('|'.join('(?:{0})'
                                         .format(re.escape(x))
                                         for x in default_telemetry))

    for node in conf.children:
        if node.key == 'ApiHost':
            api_host = node.values[0]
        elif node.key == 'ApiPort':
            api_port = int(node.values[0])
        elif node.key == 'ApiProtocol':
            api_protocol = node.values[0]
        elif node.key == 'TelemetryServer':
            telemetry_server = _str_to_bool(node.values[0])
        elif node.key == 'TelemetryHost':
            telemetry_host = node.values[0]
        elif node.key == 'TelemetryPort':
            telemetry_port = int(node.values[0])
        elif node.key == 'AclToken':
            acl_token = node.values[0]
        elif node.key == 'Dimensions' or node.key == 'Dimension':
            custom_dimensions.update(_dimensions_str_to_dict(node.values[0]))
        elif node.key == 'SfxToken':
            sfx_token = node.values[0]
        elif node.key == 'CaCertificate':
            ssl_certs['ca_cert'] = node.values[0]
        elif node.key == 'ClientCertificate':
            ssl_certs['client_cert'] = node.values[0]
        elif node.key == 'ClientKey':
            ssl_certs['client_key'] = node.values[0]
        elif node.key == 'Debug':
            log_handler.enable_debug = _str_to_bool(node.values[0])
        elif node.key == 'EnhancedMetrics':
            enhanced_metrics = _str_to_bool(node.values[0])
        elif node.key == 'ExcludeMetric':
            exclude_metrics.append(re.escape(node.values[0]))
        elif node.key == 'IncludeMetric':
            include_metrics.append(re.escape(node.values[0]))

    # the values of the 'exclude_metric' parameter are used
    # to block metrics using prefix matching - e.g. for the config
    # exclude_metric "consul.http", we block all metrics from
    # 'agent/metrics' endpoint or from udp packets starting with
    # "consul.http".

    # compile a combined regex for all metrics to exclude
    # from the /metrics endpoint or the udp packets.
    if exclude_metrics:
        exclude_metrics_regex = re.compile('|'.join('(?:{0})'.format(x)
                                           for x in exclude_metrics))
    else:
        exclude_metrics_regex = None

    if include_metrics:
        include_metrics_regex = re.compile('|'.join('(?:{0})'.format(x)
                                           for x in include_metrics))
    else:
        include_metrics_regex = None

    plugin_conf = {'api_host': api_host,
                   'api_port': api_port,
                   'api_protocol': api_protocol,
                   'telemetry_server': telemetry_server,
                   'telemetry_host': telemetry_host,
                   'telemetry_port': telemetry_port,
                   'acl_token': acl_token,
                   'sfx_token': sfx_token,
                   'ssl_certs': ssl_certs,
                   'default_telemetry_regex': default_telemetry_regex,
                   'enhanced_metrics': enhanced_metrics,
                   'exclude_metrics_regex': exclude_metrics_regex,
                   'include_metrics_regex': include_metrics_regex,
                   'custom_dimensions': custom_dimensions,
                   'debug': log_handler.enable_debug
                   }

    LOGGER.debug('Plugin Configurations - ')
    for k, v in plugin_conf.items():
        if k == 'exclude_metrics_regex':
            k, v = 'exclude_metrics', exclude_metrics
        elif k == 'include_metrics_regex':
            k, v = 'include_metrics', include_metrics
        LOGGER.debug('{0} : {1}'.format(k, v))

    consul_plugin = ConsulPlugin(plugin_conf)

    collectd.register_read(
                        consul_plugin.read,
                        name='{0}:{1}'.format(api_host, api_port)
                        )
    collectd.register_shutdown(consul_plugin.shutdown)


class ConsulPlugin(object):

    def __init__(self, plugin_conf):

        self.global_dimensions = {}
        self.enable_server = plugin_conf['telemetry_server']
        self.global_dimensions.update(plugin_conf['custom_dimensions'])
        self.default_regex = plugin_conf['default_telemetry_regex']
        self.enhanced_metrics = plugin_conf['enhanced_metrics']
        self.exclude_regex = plugin_conf['exclude_metrics_regex']
        self.include_regex = plugin_conf['include_metrics_regex']
        if self.enable_server:
            self.udp_server = UDPServer(plugin_conf['telemetry_host'],
                                        plugin_conf['telemetry_port'],
                                        plugin_conf['default_telemetry_regex'],
                                        plugin_conf['enhanced_metrics'],
                                        plugin_conf['include_metrics_regex'],
                                        plugin_conf['exclude_metrics_regex'])
        else:
            self.udp_server = None
        self.consul_agent = ConsulAgent(plugin_conf['api_host'],
                                        plugin_conf['api_port'],
                                        plugin_conf['api_protocol'],
                                        plugin_conf['acl_token'],
                                        plugin_conf['sfx_token'],
                                        plugin_conf['ssl_certs'])
        self.metric_sink = MetricSink()

    def read(self):
        '''
        Collect all metrics and emit to collectd. Will be called once/interval.
        '''
        LOGGER.debug('Starting metrics collection in read callback.')
        self.consul_agent.update_local_config()

        # Return from read callback if config for the agent is not found
        if self.consul_agent.config is None:
            LOGGER.warning('Did not find config of consul at /agent/self.')
            return

        self.global_dimensions.update(
                        self.consul_agent.get_global_dimensions())
        metric_records = []
        # Metrics to be sent if instance is currently the leader in it's dc
        if self.consul_agent.is_leader():
            metric_records.extend(self._fetch_peers())
            metric_records.extend(self._fetch_catalog_metrics())
            metric_records.extend(self._fetch_health_ckecks())
            metric_records.extend(self._fetch_dc_network_metrics())
            metric_records.extend(self._fetch_server_state_leader())

        else:
            # collect only if instance is not the leader
            metric_records.extend(self._fetch_server_state())
        # collected by all instances
        metric_records.extend(self._fetch_telemetry_metrics())
        metric_records.extend(self._fetch_node_network_metrics())
        # Emit all gathered metrics
        for metric_record in metric_records:
            self.metric_sink.emit(metric_record)

    def _fetch_server_state(self):
        # If the current instance is running in server mode and is a follower
        dimensions = {}
        metric_records = []
        if self.consul_agent.config.get('Config', {}).get('Server', False):
            LOGGER.debug('Consul node is a peer in follower state.')
            dimensions['consul_server_state'] = 'follower'
            dimensions.update(self.global_dimensions)
            metric_records.append(MetricRecord(consul_server_state.name,
                                               consul_server_state.type, 0,
                                               dimensions, time.time()))

        return metric_records

    def _fetch_server_state_leader(self):
        '''
        Only leader instance calls this function to
        update the consul_server_state metric
        '''
        LOGGER.debug('Consul node is a peer in leader state.')
        metric_records = []
        dimensions = {'consul_server_state': 'leader'}
        dimensions.update(self.global_dimensions)
        metric_records.append(MetricRecord(consul_server_state.name,
                                           consul_server_state.type,
                                           1,
                                           dimensions,
                                           time.time()))

        return metric_records

    def _fetch_peers(self):
        '''
        This function will collect number of raft peers in a datacenter.
        '''
        LOGGER.debug('Collecting number of peers.')
        metric_records = []
        # if self.consul_agent.config['Server']:
        peers = len(self.consul_agent.get_peers_in_dc())

        if peers:
            metric_records.append(MetricRecord(consul_peers.name,
                                               consul_peers.type,
                                               peers,
                                               self.global_dimensions,
                                               time.time()))

        return metric_records

    def _fetch_catalog_metrics(self):
        '''
        This function will collect catalog.* type metrics.
        Only leader instance collects these metrics.
        '''
        LOGGER.debug('Collecting catalog metrics.')
        metric_records = []

        catalog_map = self.consul_agent.get_catalog_map()

        if not catalog_map:
            return metric_records

        total_nodes = len(catalog_map['Nodes'])
        total_services = len(catalog_map['Services'])

        metric_records.append(MetricRecord(catalog_services.name,
                                           catalog_services.type,
                                           total_services,
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(catalog_nodes.name,
                                           catalog_nodes.type,
                                           total_nodes,
                                           self.global_dimensions,
                                           time.time()))

        for service, num_nodes in catalog_map['Services'].iteritems():
            dimensions = {'consul_service': service}
            dimensions.update(self.global_dimensions)
            del dimensions['consul_node']
            metric_records.append(MetricRecord(catalog_nodes_by_service.name,
                                               catalog_nodes_by_service.type,
                                               num_nodes,
                                               dimensions,
                                               time.time()))

        for node, num_services in catalog_map['Nodes'].iteritems():
            dimensions = {}
            dimensions.update(self.global_dimensions)
            dimensions['consul_node'] = node
            metric_records.append(MetricRecord(catalog_services_by_node.name,
                                               catalog_services_by_node.type,
                                               num_services,
                                               dimensions,
                                               time.time()))

        return metric_records

    def _fetch_dc_network_metrics(self):
        '''
        Get the avg, minimum and maximum network rtt
        between instance datacenter and other datacenters.
        Collected by leader instance only
        '''
        LOGGER.debug('Collecting inter-datacenter network latencies.')
        metric_records = []

        latencies = self.consul_agent.calculate_inter_dc_latency()

        if not latencies:
            return metric_records

        for dest_dc, metrics in latencies.iteritems():
            dimensions = {'destination_dc': dest_dc}
            dimensions.update(self.global_dimensions)
            metric_records.append(MetricRecord(dc_network_latency_avg.name,
                                               dc_network_latency_avg.type,
                                               metrics['avg'],
                                               dimensions,
                                               time.time()))
            metric_records.append(MetricRecord(dc_network_latency_min.name,
                                               dc_network_latency_min.type,
                                               metrics['min'],
                                               dimensions,
                                               time.time()))
            metric_records.append(MetricRecord(dc_network_latency_max.name,
                                               dc_network_latency_max.type,
                                               metrics['max'],
                                               dimensions,
                                               time.time()))

        return metric_records

    def _fetch_node_network_metrics(self):
        '''
        Get the avg, minimum and maximum network rtt between instance and
        other nodes in the datacenter.
        Collected by leader instance only.
        '''
        LOGGER.debug('Collecting intra-datacenter network latencies.')
        metric_records = []

        latencies = self.consul_agent.calculate_intra_dc_latency()

        if latencies:
            metric_records.append(MetricRecord(node_network_latency_avg.name,
                                               node_network_latency_avg.type,
                                               latencies['avg'],
                                               self.global_dimensions,
                                               time.time()))
            metric_records.append(MetricRecord(node_network_latency_min.name,
                                               node_network_latency_min.type,
                                               latencies['min'],
                                               self.global_dimensions,
                                               time.time()))
            metric_records.append(MetricRecord(node_network_latency_max.name,
                                               node_network_latency_max.type,
                                               latencies['max'],
                                               self.global_dimensions,
                                               time.time()))

        return metric_records

    def _fetch_health_ckecks(self):
        '''
        Collect the consul.health.* type metrics
        possible states - passing, warning, critical
        '''
        LOGGER.debug('Collecting health check metrics.')
        metric_records = []
        checks_map = self.consul_agent.get_health_check_stats()

        if not checks_map:
            return metric_records

        service_checks = checks_map['service']
        node_checks = checks_map['node']

        metric_records.append(MetricRecord(health_services_critical.name,
                                           health_services_critical.type,
                                           service_checks.get('critical', 0),
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(health_services_warning.name,
                                           health_services_warning.type,
                                           service_checks.get('warning', 0),
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(health_services_passing.name,
                                           health_services_passing.type,
                                           service_checks.get('passing', 0),
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(health_nodes_critical.name,
                                           health_nodes_critical.type,
                                           node_checks.get('critical', 0),
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(health_nodes_warning.name,
                                           health_nodes_warning.type,
                                           node_checks.get('warning', 0),
                                           self.global_dimensions,
                                           time.time()))
        metric_records.append(MetricRecord(health_nodes_passing.name,
                                           health_nodes_passing.type,
                                           node_checks.get('passing', 0),
                                           self.global_dimensions,
                                           time.time()))

        return metric_records

    def _fetch_telemetry_metrics(self):

        '''
        Collect metrics pertaining to the internal health of Consul.
        These metrics can be collected from either the metrics endpoint
        or from udp packets emiited by consul agent.
        '''
        LOGGER.debug('Collecting Consul\'s internal telemetry.')
        metric_records = []

        # get telemetry data from either the udp server or the metrics endpoint
        if self.enable_server:
            # acquire lock on shared data
            with self.udp_server.lock:
                for metric_name, stat in self.udp_server.stats.items():
                    metric_records.append(MetricRecord(metric_name,
                                                       stat['type'],
                                                       stat['value'],
                                                       self.global_dimensions,
                                                       stat['timestamp']))
                    # delete the metric so we don't read stale metric again,
                    # in case the metric value is not updated between two reads
                    del self.udp_server.stats[metric_name]
                self.udp_server.read_complete.set()

        elif self.consul_agent.metrics_enabled:
            # get metrics and sanitize
            metrics = self.consul_agent.get_metrics()
            if not metrics:
                return metric_records

            for metric_type, metrics_list in metrics.items():
                if metric_type == 'Timestamp':
                    continue
                for metric in metrics_list:
                    if self._include_metric(metric['Name']):
                        metric_records.extend(
                              self._sanitize_telemetry(metric_type, metric))

        return metric_records

    def _include_metric(self, metric_name):
        if self.default_regex.search(metric_name) is not None:
            return True
        elif self.enhanced_metrics:
            if self.exclude_regex is None or \
              (self.exclude_regex is not None and
               self.exclude_regex.match(metric_name) is None):
                return True
        elif (self.include_regex is not None and
              self.include_regex.match(metric_name) is not None):
            return True
        return False

    def _sanitize_telemetry(self, metric_type, metric):
        if metric_type == 'Gauges':
            return [MetricRecord(metric['Name'],
                                 'gauge',
                                 metric['Value'],
                                 self.global_dimensions,
                                 time.time())]
        elif metric_type == 'Counters':
            return [MetricRecord(metric['Name'],
                                 'gauge',
                                 metric['Sum'],
                                 self.global_dimensions,
                                 time.time())]
        elif metric_type == 'Samples':
            metric_records = []
            metric_avg = '{0}.avg'.format(metric['Name'])
            metric_max = '{0}.max'.format(metric['Name'])
            metric_min = '{0}.min'.format(metric['Name'])
            metric_records.append(
                            MetricRecord(metric_avg,
                                         'gauge',
                                         metric['Mean'],
                                         self.global_dimensions,
                                         time.time()))
            metric_records.append(
                            MetricRecord(metric_max,
                                         'gauge',
                                         metric['Max'],
                                         self.global_dimensions,
                                         time.time()))
            metric_records.append(
                            MetricRecord(metric_min,
                                         'gauge',
                                         metric['Min'],
                                         self.global_dimensions,
                                         time.time()))
            return metric_records

    def shutdown(self):
        '''
        on termination, set terminate event to indicte thread to close socket
        '''
        LOGGER.info('Cleaning up for shutdown.')
        if self.enable_server:
            self.udp_server.terminate.set()


class ConsulAgent(object):
    '''
    Helper class for interacting with consul's http api
    '''
    def __init__(self, api_host, api_port, api_protocol,
                 acl_token, sfx_token, ssl_certs):

        self.api_host = api_host
        self.api_port = api_port
        self.api_protocol = api_protocol
        self.sfx_token = sfx_token
        self.ssl_certs = ssl_certs

        # If acl_token provided, add to header
        self.acl_token = acl_token
        self.headers = {'X-Consul-Token': acl_token} if self.acl_token else {}

        self.base_url = '{0}://{1}:{2}/v1'.format(self.api_protocol,
                                                  self.api_host,
                                                  self.api_port)

        # Endpoint to get config of consul instance
        self.local_config_url = '{0}/agent/self'.format(self.base_url)

        # Catalog endpoints to get strongly consistent view of datacenters,
        # nodes (for given dc) and services (for given dc) running in cluster.
        self.list_nodes_url = '{0}/catalog/nodes'.format(self.base_url)
        self.list_services_for_node_url = '{0}/catalog/node'\
                                          .format(self.base_url)

        # Status endpoints to get leader and peers
        self.leader_status_url = '{0}/status/leader'.format(self.base_url)
        self.list_peers_url = '{0}/status/peers'.format(self.base_url)

        # Coordinate endpoints to get node coordinates
        self.list_wan_coordinates_url = '{0}/coordinate/datacenters'\
                                        .format(self.base_url)
        self.list_lan_coordinates_url = '{0}/coordinate/nodes'\
                                        .format(self.base_url)

        # Health check endpoint
        self.health_checks_url = '{0}/health/state/any'.format(self.base_url)

        # endpoint to query telemetry data. Available in v 0.9.1
        self.list_metrics_url = '{0}/agent/metrics'.format(self.base_url)

        # Ingest Url to send event
        self._event_url = 'https://ingest.signalfx.com/v2/event'

        self.config = {}
        self.metrics_enabled = False
        self.last_leader = None

    def update_local_config(self):
        conf = self.get_local_config()
        if not conf:
            self.config = None
            return
        self.config = conf
        self.metrics_enabled = self.check_metrics_endpoint_available()

    def check_metrics_endpoint_available(self):
        # /agent/metrics endpoint is available from version 0.9.1
        major, minor, revision = map(lambda x: int(x),
                                     self.config.get('Config', {}).get('Version', '0.0.0').split('.'))

        if (major == 0 and minor == 9 and revision >= 1) or major > 0:
            return True
        return False

    def get_local_config(self):
        return self._send_request(self.local_config_url)

    def get_dc_leader(self):
        return self._send_request(self.leader_status_url)

    def get_nodes_in_dc(self):
        return self._send_request(self.list_nodes_url)

    def get_services_for_node(self, node):
        return self._send_request('{0}/{1}'.format(
                                            self.list_services_for_node_url,
                                            node))

    def get_peers_in_dc(self):
        return self._send_request(self.list_peers_url)

    def get_wan_coordinates(self):
        return self._send_request(self.list_wan_coordinates_url)

    def get_lan_coordinates(self):
        return self._send_request(self.list_lan_coordinates_url)

    def get_health_checks(self):
        return self._send_request(self.health_checks_url)

    def get_metrics(self):
        return self._send_request(self.list_metrics_url)

    def is_leader(self):
        '''
        Might return no leader, if consul is in between elections or
        majority is broken.
        '''
        curr_leader = self.get_dc_leader()

        # pre v1.0.0
        host = self.config.get('Config', {}).get('AdvertiseAddr', None)
        if not host:
            host = self.config.get('Member', {}).get('Addr', {})

        # prev 1.0.0
        port = self.config.get('Config', {}).get('Ports', {}).get('Server', None)
        if not port:
            port = self.config.get('DebugConfig', {}).get('ServerPort', None)

        agent_addr = '{0}:{1}'.format(host, port)
        if curr_leader == agent_addr:
            if self.last_leader is not None and \
               self.last_leader != curr_leader and \
               self.sfx_token:

                LOGGER.debug('Change in leader.')
                dimensions = {'old_leader': self.last_leader.split(':')[0],
                              'new_leader': curr_leader.split(':')[0],
                              'datacenter': self.config.get('Config', {}).get('Datacenter', '')}
                self._send_leader_change_event(dimensions)
                self.last_leader = curr_leader

            return True

        if curr_leader:
            self.last_leader = curr_leader
        else:
            LOGGER.warn('Did not find any consul cluster leader.')

        return False

    def get_catalog_map(self):
        '''
        returns a dictionary with count of services on a node and the number of
        nodes providing a service.
        Ex - { 'Nodes': {
                'node_1': 2, 'nodes_2': 1 },
                'Services': {
                'web': 1, 'consul': 2, 'redis': 1
                }
            }
        This means node_1 is providing 2 services while the service 'web'
        is running on 1 node
        '''
        nodes = self.get_nodes_in_dc()

        if not nodes:
            return None

        node_to_services_map = {}
        service_to_nodes_map = {}
        catalog_map = {}

        for node in nodes:
            seen_services = set()
            node_id = node['Node'] or node['ID']
            services = self.get_services_for_node(node_id)
            node_to_services_map[node_id] = len(services['Services'])

            for k, v in services['Services'].iteritems():
                if v['Service'] not in seen_services:
                    # in case multiple instances of service are running on node
                    seen_services.add(v['Service'])
                    service_to_nodes_map[v['Service']] = \
                        service_to_nodes_map.get(v['Service'], 0) + 1

        catalog_map = {'Nodes': node_to_services_map,
                       'Services': service_to_nodes_map}

        return catalog_map

    def calculate_inter_dc_latency(self):
        '''
        Calculates latencies from every node in instance dc to every node
        in a destination dc.
        Returns an average, minimum and maximum of these latencies for every
        possible destination dc.
        '''
        inter_dc_coords = self.get_wan_coordinates()
        if not inter_dc_coords:
            return None

        agent_dc_coords = {}
        dc_latency_map = {}
        agent_dc = self.config.get('Config', {}).get('Datacenter', '')

        for idx, dc in enumerate(inter_dc_coords):
            if dc['Datacenter'] == agent_dc:
                agent_dc_coords = dc['Coordinates']
                # found the instance dc, remove it from list
                inter_dc_coords.pop(idx)
                break

        for dc in inter_dc_coords:
            latencies = []
            for node_a in dc['Coordinates']:
                for node_b in agent_dc_coords:
                    latencies.append(compute_rtt(node_a['Coord'],
                                     node_b['Coord']))
            avg = reduce(lambda x, y: x + y, latencies)/len(latencies)
            dc_latency_map[dc['Datacenter']] = {'avg': avg,
                                                'min': min(latencies),
                                                'max': max(latencies)}

        return dc_latency_map

    def calculate_intra_dc_latency(self):
        '''
        Calculates latencies from instance node to
        every node within the instance dc.
        Returns an average, minimum and maximum of these latencies.
        '''
        intra_dc_coords = self.get_lan_coordinates()
        if not intra_dc_coords:
            return None

        agent_node_coords = {}
        latency_metrics = {}

        for idx, node in enumerate(intra_dc_coords):

            if node['Node'] == self.config.get('Config', {}).get('NodeName', None):
                agent_node_coords = node['Coord']
                # found the instance, remove it from list
                intra_dc_coords.pop(idx)
                break
        # skip if did not find instance node's coordinates and
        # if single node in dc
        if agent_node_coords and len(intra_dc_coords):
            latencies = []
            for node in intra_dc_coords:
                latencies.append(compute_rtt(node['Coord'], agent_node_coords))

            avg = reduce(lambda x, y: x + y, latencies)/len(latencies)
            latency_metrics['avg'] = avg
            latency_metrics['min'] = min(latencies)
            latency_metrics['max'] = max(latencies)

        return latency_metrics

    def get_health_check_stats(self):
        '''
        Count service and nodes in states - passing, warning, critical
        '''
        checks_map = {'service': {}, 'node': {}}
        checks = self.get_health_checks()
        if not checks:
            return None

        for check in checks:
            status = check['Status']
            # if serviceID field is not empty,
            # means the check is a service check
            if check['ServiceID']:
                checks_map['service'][status] = \
                                    checks_map['service'].get(status, 0) + 1
            # check is a node check
            else:
                checks_map['node'][status] = \
                                        checks_map['node'].get(status, 0) + 1

        return checks_map

    def get_global_dimensions(self):
        '''
        These dimensions are added to all metrics -
        datacenter the consul node belongs to, node name, and node id.
        '''
        dimensions = {}
        if self.config is None:
            return dimensions

        dimensions['datacenter'] = self.config.get('Config', {}).get('Datacenter', '')
        dimensions['consul_node'] = self.config.get('Config', {}).get('NodeName', '')
        if self.config.get('Config', {}).get('Server', False):
            dimensions['consul_mode'] = 'server'
        else:
            dimensions['consul_mode'] = 'client'
        return dimensions

    def _send_leader_change_event(self, dimensions):
        '''
        send an event to signalfx on leader transition
        '''
        payload = json.dumps([{'category': 'USER_DEFINED',
                               'eventType': 'consul leader changed',
                               'dimensions': dimensions,
                               'timestamp': int(time.time() * 1000)
                               }])
        sfx_headers = {'Content-Type': 'application/json',
                       'X-SF-TOKEN': self.sfx_token}

        try:
            req = urllib2.Request(self._event_url, payload)
            for k, v in sfx_headers.items():
                req.add_header(k, v)
            response = urllib2.urlopen(req)
            LOGGER.debug('Sent leader change event to SignalFX with '
                         'response code {0}'.format(response.code))
        except urllib2.HTTPError, e:
            LOGGER.error('HTTPError - status code: {0}, '
                         'received from {1}'.format(e.code, self._event_url))
        except urllib2.URLError, e:
            LOGGER.error('URLError - {0} {1}'.format(self._event_url, e.reason))

    def _send_request(self, url):
        '''
        Performs a GET against the given url.
        '''
        LOGGER.debug('Making an api call to {0}'.format(url))
        data = None

        try:

            opener = urllib2.build_opener(urllib_ssl_handler.HTTPSHandler(
                            ca_certs=self.ssl_certs['ca_cert'],
                            cert_file=self.ssl_certs['client_cert'],
                            key_file=self.ssl_certs['client_key']))
            opener.addheaders = []
            for k, v in self.headers.items():
                opener.addheaders.append((k, v))
            response = opener.open(url)
            data = response.read()
            data = json.loads(data)
        except urllib2.HTTPError, e:
            LOGGER.error('HTTPError - status code: {0}, '
                         'received from {1}'.format(e.code, url))
        except urllib2.URLError, e:
            LOGGER.error('URLError - {0} {1}'.format(url, e.reason))
        except ValueError, e:
            LOGGER.error('Error parsing JSON for url {0}. {1}'.format(url, e))

        return data


# Set up logging
LOG_FILE_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE_MESSAGE_FORMAT = '[%(levelname)s] [consul-collectd] '\
                          '[%(asctime)s UTC]: %(message)s'
formatter = logging.Formatter(fmt=LOG_FILE_MESSAGE_FORMAT,
                              datefmt=LOG_FILE_DATE_FORMAT)
log_handler = CollectdLogHandler('consul-collectd')
log_handler.setFormatter(formatter)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.propagate = False
LOGGER.addHandler(log_handler)

if __name__ == '__main__':
    pass
else:

    collectd.register_config(configure_callback)
