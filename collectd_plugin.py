#!/usr/bin/env python
import socket
import time
import sys, os
import logging
import threading
import urllib2, urllib_ssl_handler
import json
import collectd

PLUGIN = 'consul'

API_PROTOCOL = 'ApiProtocol'
API_HOST = 'ApiHost'
API_PORT = 'ApiPort'
TELEMETRY_SERVER = 'TelemetryServer'
TELEMETRY_HOST = 'TelemetryHost'
TELEMETRY_PORT = 'TelemetryPort'
ACL_TOKEN = 'AclToken'
SFX_TOKEN = 'SignalFxToken'
CA_CERTIFICATE = 'CACertificate'
CLIENT_CERTIFICATE = 'ClientCertificate'
CLIENT_KEY = 'ClientKey'
DIMENSION = 'Dimension'
DIMENSIONS = 'Dimensions'

def compute_rtt( coord_a, coord_b):
	'''
	Computes network round trip time between nodes represented as network coordinates.
	https://www.consul.io/docs/internals/coordinates.html
	'''
	sum_sq = 0.0
	for vec_a, vec_b in zip(coord_a['Vec'], coord_b['Vec']):
		diff = vec_a - vec_b
		sum_sq += diff**2

	rtt = sum_sq**0.5 + coord_a['Height'] + coord_b['Height']

	adjusted = rtt + coord_a['Adjustment'] + coord_b['Adjustment']
	if adjusted > 0 :
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
		try:
			if record.msg is not None:
				if record.levelname == 'ERROR':
					collectd.error(self.format(record))
				elif record.levelname == 'WARNING':
					collectd.warning(self.format(record))
				elif record.levelname == 'INFO':
					collectd.info(self.format(record))
				elif record.levelname == 'DEBUG' and self.debug:
					collectd.debug(self.format(record))
		except Exception as e:
			collectd.warning(('{p} [ERROR]: Failed to write log statement due '
							'to: {e}').format(p=self.plugin_name, e=e))

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

	def __init__(self, metric_name, metric_type, value, dimensions=None, timestamp=None):
		self.name = metric_name
		self.type = metric_type
		self.value = value
		self.dimensions = dimensions or {}
		self.timestamp = timestamp or time.time()

	def to_string(self):
		return MetricRecord.TO_STRING_FORMAT.format(self.name, self.type, self.value,\
			self.instance_id, self.dimensions, self.timestamp)

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
		emit_value.plugin = 'consul'
		emit_value.values = [metric_record.value]
		emit_value.type = metric_record.type
		emit_value.type_instance = metric_record.name
		emit_value.plugin_instance = '[{0}]'.format(self._format_dimensions(metric_record.dimensions))

		# With some versions of CollectD, a dummy metadata map must to be added
		# to each value for it to be correctly serialized to JSON by the
		# write_http plugin. See
		# https://github.com/collectd/collectd/issues/716
		emit_value.meta = {'true': 'true'}

		emit_value.dispatch()

	def _format_dimensions(self, dimensions):
		'''
		Formats a dictionary of key/value pairs as a comma-delimited list of key=value tokens.
		This was copied from docker-collectd-plugin.
		'''
		return ','.join(['='.join((key.replace('.', '_'), value)) for key, value in dimensions.iteritems()])

class UDPServer(threading.Thread):
	'''
	Class to collect telemetry data from Consul sent via UDP packets
	Creates a thread which receives packets from Consul agent on a UDP socket.
	The timeout interval ensures that the thread does not block on a receive call.
	'''
	def __init__(self, host, port, max_buffer_size = 1432, timeout_interval = 20):
		
		threading.Thread.__init__(self)
		#self.daemon = True
		self._host = host
		self._port = port
		self._bufsize = max_buffer_size
		self._timeout = timeout_interval
		self.lock = threading.Lock()
		self.stats = {}
		self.terminate = threading.Event()
		self.read_complete = threading.Event()
		self.start()

	def run(self):
		
		LOGGER.info('Starting UDP server on {0}:{1} to collect Consul\'s telemetry data. Timeout set to {2}.'\
			.format(self._host, self._port, self._timeout))
		
		try:
			self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			self.socket.bind((self._host, self._port))
			self.socket.settimeout(self._timeout)
			
			metrics = {}
			timers = {}
			while not self.terminate.isSet():
				try:
					data, addr = self.socket.recvfrom(self._bufsize)
					if self.read_complete.isSet():
						metrics.clear()
						timers.clear()
						self.read_complete.clear()
					'''
					Sanitize the data from udp packets.
					Store only the latest value of metric, in case a packet has multiple instances of a metric.
					'''
					for datapoint in data.splitlines():
						metric_split = datapoint.split(':')
						if len(metric_split) == 2 and '' not in metric_split:
							metric_name = metric_split.pop(0)
							metric_split = metric_split[0].split('|')
							if len(metric_split) == 2 and '' not in metric_split:
								metric_type = 'gauge'
								if metric_split[1] == 'c' and metric_name in metrics:
									metrics[metric_name] = {'type': metric_type, \
															'value': metrics[metric_name]['value'] + int(float(metric_split[0])),\
															'timestamp': time.time()}					
								elif metric_split[1] in ['c', 'g']:
									metrics[metric_name] = {'type': metric_type,\
															'value': float(metric_split[0]) if metric_split[1] != 'c' else int(float(metric_split[0])),\
															'timestamp': time.time()}
								elif metric_split[1] == 'ms':
									timers[metric_name] = timers.get(metric_name, [])
									timers[metric_name].append(float(metric_split[0]))
									metrics['{0}_mean'.format(metric_name)] = {'type': metric_type,\
									'value': reduce(lambda x,y: x + y, timers[metric_name])/len(timers[metric_name]),\
									'timestamp': time.time()}
									metrics['{0}_min'.format(metric_name)] = {'type': metric_type,\
									'value' : min(timers[metric_name]),\
									'timestamp': time.time()}
									metrics['{0}_max'.format(metric_name)] = {'type': metric_type,\
									'value' : max(timers[metric_name]),\
									'timestamp': time.time()}
							else:
								'''
								In case the packet was truncated while receiving.
								'''
								LOGGER.warning('Malformed metric record from UDP packet. Increase max udp buffer size.')
						else:
							LOGGER.warning('Malformed metric record from UDP packet. Increase max udp buffer size.')
					'''
					Update the shared data structure once lock is acquired
					'''
					with self.lock:
						self.stats.update(metrics)
				
				except socket.timeout:
					LOGGER.warn('UDP Server timed out waiting for telemetry data on {0}:{1}.'\
						.format(self._host, self._port))
			
			''' 
			Terminate event is set. Close the socket and exit.
			'''
			self.socket.close()
			LOGGER.info('Exiting UDP Server thread.')
		
		except socket.error, e:
			LOGGER.error('Error in UDP socket at {0}:{1} : {2}'.format(self._host, self._port, e))

consul_server_state = MetricDefinition('consul.is_leader', 'gauge')
consul_peers = MetricDefinition('consul.peers', 'gauge')

catalog_services = MetricDefinition('consul.catalog.services.total', 'gauge')
catalog_nodes	= MetricDefinition('consul.catalog.nodes.total', 'gauge')
catalog_nodes_by_service = MetricDefinition('consul.catalog.nodes_by_service', 'gauge')
catalog_services_by_node =	MetricDefinition('consul.catalog.services_by_node', 'gauge')

dc_network_latency_avg = MetricDefinition('consul.network.dc.latency.avg', 'gauge')
dc_network_latency_min = MetricDefinition('consul.network.dc.latency.min', 'gauge')
dc_network_latency_max = MetricDefinition('consul.network.dc.latency.max', 'gauge')

node_network_latency_avg = MetricDefinition('consul.network.node.latency.avg', 'gauge')
node_network_latency_min = MetricDefinition('consul.network.node.latency.min', 'gauge')
node_network_latency_max = MetricDefinition('consul.network.node.latency.max', 'gauge')

health_services_passing = MetricDefinition('consul.health.services.passing', 'gauge')
health_services_warning = MetricDefinition('consul.health.services.warning', 'gauge')
health_services_critical = MetricDefinition('consul.health.services.critical', 'gauge')

health_nodes_passing = MetricDefinition('consul.health.nodes.passing', 'gauge')
health_nodes_warning = MetricDefinition('consul.health.nodes.warning', 'gauge')
health_nodes_critical = MetricDefinition('consul.health.nodes.critical', 'gauge')


class ConsulPlugin(object):

	def __init__(self):
		'''
		Add all parameters to be maintained per consul agent instance here
		'''
		self.global_dimensions = {}
		self.metric_sink = None

	def configure(self, conf):
		'''
		Configure plugin with config provided from collectd.
		'''
		LOGGER.info('Starting Consul Plugin configuration.')

		'''
		Default values of config options
		'''
		api_host = 'localhost'
		api_port = 8500
		api_protocol = 'http'
		telemetry_server = False
		telemetry_host = '127.0.0.1'
		telemetry_port = 8125
		acl_token = None
		sfx_token = None
		ssl_certs = {'ca_cert':None, 'client_cert':None, 'client_key':None}

		for node in conf.children:
			if node.key == API_HOST:
				api_host = node.values[0]
			elif node.key == API_PORT:
				api_port = int(node.values[0])
			elif node.key == API_PROTOCOL:
				api_protocol = node.values[0]
			elif self._check_bool_config_enabled(node, TELEMETRY_SERVER):
				telemetry_server = self._str_to_bool(node.values[0])
			elif node.key == TELEMETRY_HOST:
				telemetry_host = node.values[0]
			elif node.key == TELEMETRY_PORT:
				telemetry_port = int(node.values[0])
			elif node.key == ACL_TOKEN:
				acl_token = node.values[0]
			elif node.key == DIMENSIONS or node.key == DIMENSION:
				self.global_dimensions.update(self._dimensions_str_to_dict(node.values[0]))
			elif node.key == SFX_TOKEN:
				sfx_token = node.values[0]
			elif node.key == CA_CERTIFICATE:
				ssl_certs['ca_cert'] = node.values[0]
			elif node.key == CLIENT_CERTIFICATE:
				ssl_certs['client_cert'] = node.values[0]
			elif node.key == CLIENT_KEY:
				ssl_certs['client_key'] = node.values[0]

		self.enable_server = telemetry_server
		if self.enable_server:
			self.udp_server = UDPServer(telemetry_host, telemetry_port)
		self.consul_agent = ConsulAgent(api_host, api_port, api_protocol, acl_token, sfx_token, ssl_certs)
		self.global_dimensions.update(self.consul_agent.get_global_dimensions())
		self.metric_sink = MetricSink()

	def read(self):
		'''
		Collect all metrics and emit to collectd. Will be called once per interval.
		'''
		self.consul_agent.update_local_config()
		metric_records = []
		'''
		Metrics to be sent if instance is currently the leader in it's datacenter.
		'''
		if self.consul_agent.is_leader():
			metric_records.extend(self._fetch_peers())
			metric_records.extend(self._fetch_catalog_metrics())
			metric_records.extend(self._fetch_health_ckecks())
			metric_records.extend(self._fetch_dc_network_metrics())
			metric_records.extend(self._fetch_server_state_leader())
		else:
			''' collect only if instance is not the leader
			'''
			metric_records.extend(self._fetch_server_state())
		''' collected by all instances
		'''
		metric_records.extend(self._fetch_telemetry_metrics())
		metric_records.extend(self._fetch_node_network_metrics())
		'''
		Emit all gathered metrics
		'''
		for metric_record in metric_records:
			self.metric_sink.emit(metric_record)

	def _fetch_server_state(self):
		'''
		If the current instance is running in server mode and is a follower
		'''
		dimensions = {}
		metric_records = []
		if self.consul_agent.config['Server']:
			dimensions['consul_server_state'] = 'follower'
			dimensions.update(self.global_dimensions)
			metric_records.append(MetricRecord(consul_server_state.name, consul_server_state.type, 0, dimensions, time.time()))
		
		return metric_records

	def _fetch_server_state_leader(self):
		'''
		Only leader instance calls this function to update the consul_server_state metric
		'''
		metric_records = []
		dimensions = {'consul_server_state' : 'leader'}
		dimensions.update(self.global_dimensions)
		metric_records.append(MetricRecord(consul_server_state.name, consul_server_state.type, 1, dimensions, time.time()))
		
		return metric_records

	def _fetch_peers(self):
		'''
		Will collect number of raft peers in a datacenter.
		'''
		metric_records = []
		# if self.consul_agent.config['Server']:
		peers = len(self.consul_agent.get_peers_in_dc())
		metric_records.append(MetricRecord(consul_peers.name, consul_peers.type, peers, self.global_dimensions, time.time()))

		return metric_records

	def _fetch_catalog_metrics(self):
		'''
		Will collect catalog.* type metrics.
		Only leader instance collects these metrics.
		'''
		metric_records = []

		catalog_map = self.consul_agent.get_catalog_map()
		total_nodes = len(catalog_map['Nodes'])
		total_services = len(catalog_map['Services'])

		metric_records.append(MetricRecord(catalog_services.name, catalog_services.type, total_services, self.global_dimensions, time.time()))
		metric_records.append(MetricRecord(catalog_nodes.name, catalog_nodes.type, total_nodes, self.global_dimensions, time.time()))

		for service, num_nodes in catalog_map['Services'].iteritems():
			dimensions = {'consul_service':service}
			dimensions.update(self.global_dimensions)
			del dimensions['consul_node']
			metric_records.append(MetricRecord(catalog_nodes_by_service.name, catalog_nodes_by_service.type, num_nodes, dimensions, time.time()))

		for node, num_services in catalog_map['Nodes'].iteritems():
			dimensions = {}
			dimensions.update(self.global_dimensions)
			dimensions['consul_node'] = node
			metric_records.append(MetricRecord(catalog_services_by_node.name, catalog_services_by_node.type, num_services, dimensions, time.time()))

		return metric_records

	def _fetch_dc_network_metrics(self):
		'''
		Get the avg, minimum and maximum network rtt between instance datacenter and other datacenters.
		Collected by leader instance only
		'''
		metric_records = []

		latencies = self.consul_agent.calculate_inter_dc_latency()

		for dest_dc, metrics in latencies.iteritems():
			dimensions = {'destination_dc' : dest_dc}
			dimensions.update(self.global_dimensions)
			metric_records.append(MetricRecord(dc_network_latency_avg.name, dc_network_latency_avg.type, metrics['avg'], dimensions, time.time()))
			metric_records.append(MetricRecord(dc_network_latency_min.name, dc_network_latency_min.type, metrics['min'], dimensions, time.time()))
			metric_records.append(MetricRecord(dc_network_latency_avg.name, dc_network_latency_max.type, metrics['max'], dimensions, time.time()))
		
		return metric_records

	def _fetch_node_network_metrics(self):
		'''
		Get the avg, minimum and maximum network rtt between instance and other nodes in the datacenter.
		Collected by leader instance only.
		Add consul_node dimension
		'''
		metric_records = []

		latencies = self.consul_agent.calculate_intra_dc_latency()

		if latencies:
			metric_records.append(MetricRecord(node_network_latency_avg.name, node_network_latency_avg.type, latencies['avg'], self.global_dimensions, time.time()))
			metric_records.append(MetricRecord(node_network_latency_min.name, node_network_latency_min.type, latencies['min'], self.global_dimensions, time.time()))
			metric_records.append(MetricRecord(node_network_latency_max.name, node_network_latency_max.type, latencies['max'], self.global_dimensions, time.time()))
		
		return metric_records

	def _fetch_health_ckecks(self):
		'''
		Collect the consul.health.* type metrics
		possible states - passing, warning, critical
		'''
		metric_records = []
		checks_map = self.consul_agent.get_health_check_stats()
		service_checks = checks_map['service']
		node_checks = checks_map['node']

		metric_records.append(MetricRecord(health_services_critical.name, health_services_critical.type, service_checks.get('critical', 0), self.global_dimensions, time.time()))
		metric_records.append(MetricRecord(health_services_warning.name, health_services_warning.type, service_checks.get('warning', 0), self.global_dimensions, time.time()))
		metric_records.append(MetricRecord(health_services_passing.name, health_services_passing.type, service_checks.get('passing', 0), self.global_dimensions, time.time()))

		metric_records.append(MetricRecord(health_nodes_critical.name, health_nodes_critical.type, node_checks.get('critical', 0), self.global_dimensions, time.time()))
		metric_records.append(MetricRecord(health_nodes_warning.name, health_nodes_warning.type, node_checks.get('warning', 0), self.global_dimensions, time.time()))
		metric_records.append(MetricRecord(health_nodes_passing.name, health_nodes_passing.type, node_checks.get('passing', 0), self.global_dimensions, time.time()))

		return metric_records

	def _fetch_telemetry_metrics(self):

		metric_records = []

		''' get telemetry data from either the udp server or the metrics endpoint'''
		if self.enable_server:
			''' acquire lock on shared data '''
			with self.udp_server.lock:
				for metric_name, stat in self.udp_server.stats.items():
					metric_records.append(MetricRecord(metric_name, stat['type'], stat['value'], self.global_dimensions, stat['timestamp']))
					'''
					delete the metric to make sure we do not read a stale metric again, 
					in case the metric value is not updated between two reads. (avoids 'value too old' warning from collectd)
					'''
					del self.udp_server.stats[metric_name]
			self.udp_server.read_complete.set()

		elif self.consul_agent.metrics_enabled:
			''' get metrics and sanitize '''
			metrics = self.consul_agent.get_metrics()
			if metrics:
				for metric_type, metrics_list in metrics.items():
					for metric in metrics_list:
						if metric_type == 'Gauges':
							metric_records.append(MetricRecord(metric['Name'], 'gauge', metric['Value'], self.global_dimensions, time.time()))
						elif metric_type == 'Counters':
							metric_records.append(MetricRecord(metric['Name'], 'gauge', metric['Sum'], self.global_dimensions, time.time()))
						elif metric_type == 'Samples':
							metric_mean = '{0}_mean'.format(metric['Name'])
							metric_max = '{0}_max'.format(metric['Name'])
							metric_min = '{0}_min'.format(metric['Name'])
							metric_records.append(MetricRecord(metric_mean, 'gauge', metric['Mean'], self.global_dimensions, time.time()))
							metric_records.append(MetricRecord(metric_max, 'gauge', metric['Max'], self.global_dimensions, time.time()))
							metric_records.append(MetricRecord(metric_min, 'gauge', metric['Min'], self.global_dimensions, time.time()))

		return metric_records

	def shutdown(self):
		'''
		on termination, set the terminate event to indicte udp server thread to close socket
		'''
		LOGGER.info('Cleaning up for shutdown.')
		if self.enable_server:
			self.udp_server.terminate.set()	

	def _check_split(self, splitted_list):
		'''
		check for malformed data on split
		'''
		if len(splitted_list) == 2 and '' not in splitted_list:
			return True
		return False

	def _format_dimensions(self, dimensions):
		'''
		Formats a dictionary of key/value pairs as a comma-delimited list of key=value tokens.
		This was copied from docker-collectd-plugin.
		'''
		return ','.join(['='.join((key.replace('.', '_'), value)) for key, value in dimensions.iteritems()])

	def _dimensions_str_to_dict(self, dimensions_str):

		dimensions = {}
		dimensions_list = dimensions_str.strip().split(',')

		for dimension in dimensions_list:
			key_val_split = dimension.strip().split('=')
			if self._check_split(key_val_split):
				dimensions[key_val_split[0]] = key_val_split[1]
			else:
				LOGGER.warning('Malformed dimension key=value pair: {0}'.format(key_val_split))
		
		return dimensions

	def _str_to_bool(self, value):
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

	def _check_bool_config_enabled(self, config_node, key):
		'''
		Convenience method to check if a collectd Config node contains the given
		key and if that key's value is a True bool.
		'''
		return config_node.key == key and self._str_to_bool(config_node.values[0])

class ConsulPluginManager(object):
	'''
	Class to manage instances of ConsulPlugin.
	'''

	def __init__(self):
		self.plugin_instances = []

	def configure_callback(self, conf):
		'''
		The method is registered with collectd for config.
		It will be called by collectd for each config instance.
		Creates an instance of ConsulPlugin.
		'''
		plugin_instance = ConsulPlugin()
		plugin_instance.configure(conf)
		self.plugin_instances.append(plugin_instance)

	def read_callback(self):
		'''
		Registered with collected.
		Called once per interval on each instance of ConsulPlugin to dispatch metrics to collectd.
		'''
		for plugin in self.plugin_instances:
			plugin.read()

	def shutdown_callback(self):
		'''
		Registered with collectd.
		Called on each instance of ConsulPlugin on shutdown.
		'''
		for plugin in self.plugin_instances:
			plugin.shutdown()


class ConsulAgent(object):
	'''
	Helper class for interacting with consul's http api
	'''

	def __init__(self, api_host='localhost', api_port=8500, api_protocol='http', acl_token=None, sfx_token=None, ssl_certs=None):
		
		self.api_host = api_host
		self.api_port = api_port
		self.api_protocol = api_protocol
		self.sfx_token = sfx_token
		self.ssl_certs = ssl_certs

		# If acl_token provided, add to header
		self.acl_token = acl_token
		self.headers = {'X-Consul-Token': acl_token} if self.acl_token else {}

		self.base_url = '{0}://{1}:{2}/v1'.format(self.api_protocol, self.api_host, self.api_port)

		'''
		Endpoint to get config of consul instance
		'''
		self.local_config_url = '{0}/agent/self'.format(self.base_url)

		'''
		Catalog endpoints to get strongly consistent view of datacenters,
		nodes (for given dc) and services (for given dc) running in Consul cluster.
		'''
		self.list_nodes_url = '{0}/catalog/nodes'.format(self.base_url)
		self.list_services_for_node_url = '{0}/catalog/node'.format(self.base_url)

		'''
		Status endpoints to get leader and peers
		'''
		self.leader_status_url = '{0}/status/leader'.format(self.base_url)
		self.list_peers_url = '{0}/status/peers'.format(self.base_url)

		'''
		Coordinate endpoints to get node coordinates
		'''
		self.list_wan_coordinates_url = '{0}/coordinate/datacenters'.format(self.base_url)
		self.list_lan_coordinates_url = '{0}/coordinate/nodes'.format(self.base_url)
		'''
		Health check endpoint
		'''
		self.health_checks_url = '{0}/health/state/any'.format(self.base_url)
		'''
		endpoint to query telemetry data. Available in v 0.9.1
		'''
		self.list_metrics_url = '{0}/agent/metrics'.format(self.base_url)
		'''
		Ingest Url to send event
		'''
		self._event_url = 'http://lab-ingest.corp.signalfuse.com:8080/v2/event'
		# self._event_url = 'https://ingest.signalfx.com/v2/event'

		self.config = None
		self.metrics_enabled = False
		self.last_leader = self.get_dc_leader()
		self.update_local_config()

	def update_local_config(self):
		conf = self.get_local_config()
		self.config = conf['Config']
		self.metrics_enabled = self.check_metrics_endpoint_available()

	def check_metrics_endpoint_available(self):
		'''
		/agent/metrics endpoint is available from version 0.9.1
		'''
		major, minor, revision = map(lambda x : int(x), self.config['Version'].split('.'))
		
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
		return self._send_request(self.list_services_for_node_url + '/{0}'.format(node))

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
		Might return no leader, if consul is in between elections or majority is broken.
		'''
		curr_leader = self.get_dc_leader()
		
		agent_addr = '{0}:{1}'.format(self.config['AdvertiseAddr'], \
									self.config['Ports']['Server'])
		
		if curr_leader == agent_addr:
			if (self.last_leader != curr_leader) and self.sfx_token :
				LOGGER.info('Change in leader.')
				dimensions = {'old_leader': self.last_leader.split(':')[0],\
				'new_leader': curr_leader.split(':')[0], 'datacenter':self.config['Datacenter']}
				self._send_leader_change_event(dimensions)
				self.last_leader = curr_leader
			return True

		self.last_leader = curr_leader
		
		if not curr_leader:
			LOGGER.warn('Did not find any consul cluster leader.')
		
		return False

	def get_catalog_map(self):
		'''
		returns a dictionary with count of services on a node and the number of nodes providing a service.
		Ex - { 'Nodes': {
				'node_1': 2, 'nodes_2': 1 },
				'Services': {
				'web': 1, 'consul': 2, 'redis': 1
				}		
			} 
		This means node_1 is providing 2 services while the service 'web' is running on 1 node
		'''
		nodes = self.get_nodes_in_dc()
		node_to_services_map = {}
		service_to_nodes_map ={}
		catalog_map = {}

		for node in nodes:
			seen_services = set()
			node_id = node['Node'] or node['ID']
			services = self.get_services_for_node(node_id)
			node_to_services_map[node_id] = len(services['Services'])

			for k,v in services['Services'].iteritems():
				if v['Service'] not in seen_services:
					'''
					in case multiple instances of same service is running on a node
					'''
					seen_services.add(v['Service'])
					service_to_nodes_map[v['Service']] = service_to_nodes_map.get(v['Service'], 0) + 1

		catalog_map = {'Nodes' : node_to_services_map, 'Services': service_to_nodes_map}
		
		return catalog_map 

	def calculate_inter_dc_latency(self):
		'''
		Calculates latencies from every node in instance dc to every node in a destination dc.
		Returns an average, minimum and maximum of these latencies for every possible destination dc.
		'''
		inter_dc_coords = self.get_wan_coordinates()
		agent_dc_coords = {}
		dc_latency_map = {}
		agent_dc = self.config['Datacenter']

		for idx, dc in enumerate(inter_dc_coords):
			if dc['Datacenter'] == agent_dc:
				agent_dc_coords = dc['Coordinates']
				# found the instance dc, remove it from list
				inter_dc_coords.pop(idx)
				break
		# Skip if only one dc
		if agent_dc_coords:
			for dc in inter_dc_coords:
				latencies = []
				for node_a in dc['Coordinates']:
					for node_b in agent_dc_coords:
						latencies.append(compute_rtt(node_a['Coord'], node_b['Coord']))
				avg = reduce(lambda x,y: x + y, latencies)/len(latencies)
				dc_latency_map[dc['Datacenter']] = {'avg': avg, 'min': min(latencies), 'max': max(latencies)}

		return dc_latency_map

	def calculate_intra_dc_latency(self):
		'''
		Calculates latencies from instance node to every node within the instance dc.
		Returns an average, minimum and maximum of these latencies.
		'''		
		intra_dc_coords = self.get_lan_coordinates()
		agent_node_coords = {}
		latency_metrics = {}

		for idx, node in enumerate(intra_dc_coords):
			if node['Node'] == self.config['NodeName'] or node['Node'] == self.config['NodeID']:
				agent_node_coords = node['Coord']
				# found the instance, remove it from list
				intra_dc_coords.pop(idx)
				break
		# skip if single node in dc
		if len(intra_dc_coords):
			latencies = []
			for node in intra_dc_coords:
				latencies.append(compute_rtt(node['Coord'], agent_node_coords))
			latency_metrics['avg'] = reduce(lambda x,y: x + y, latencies)/len(latencies)
			latency_metrics['min'] = min(latencies)
			latency_metrics['max'] = max(latencies)
		
		return latency_metrics

	def get_health_check_stats(self):
		'''
		Count service and nodes in states - passing, warning, critical
		'''
		checks_map = {'service':{}, 'node': {}}
		checks = self.get_health_checks()
		
		for check in checks:
			status = check['Status']
			# if serviceID field is not empty, means the check is a service check
			if check['ServiceID']:
				checks_map['service'][status] = checks_map['service'].get(status, 0) + 1
			# check is a node check
			else:
				checks_map['node'][status] = checks_map['node'].get(status, 0) + 1

		return checks_map

	def get_global_dimensions(self):
		'''
		These dimensions are added to all metrics - 
		datacenter the consul node belongs to, node name, and node id.
		'''
		dimensions = {}
		dimensions['datacenter'] = self.config['Datacenter']
		dimensions['consul_node'] = self.config['NodeName'] or self.config['NodeID']
		if self.config['Server']:
			dimensions['consul_mode'] = 'server'
		else:
			dimensions['consul_mode'] = 'client'
		return dimensions

	def _send_leader_change_event(self, dimensions):
		'''
		send an event to signalfx on leader transition
		'''
		payload = json.dumps([{
		'category': 'USER_DEFINED',
		'eventType': 'consul leader changed',
		'dimensions': dimensions,
		'timestamp': int(time.time() * 1000)
		}])
		sfx_headers = {'Content-Type': 'application/json', 'X-SF-TOKEN': self.sfx_token}
		
		try:
			req = urllib2.Request(self._event_url, payload)
			for k,v in sfx_headers:
				req.add_header(k, v)
			response = urllib2.urlopen(req)
		except urllib2.HTTPError, e:
			LOGGER.error('HTTPError - status code: {0}, received from {1}'.format(e.code, url))
		except urllib2.URLError, e:
			LOGGER.error('URLError - {0}'.format(e.reason))
	
	def _send_request(self, url):
		'''
		Performs a GET against the given url.
		'''
		data = None

		try:

			opener = urllib2.build_opener(urllib_ssl_handler.HTTPSHandler(\
							ca_certs = self.ssl_certs['ca_cert'],\
							cert_file = self.ssl_certs['client_cert'],\
							key_file = self.ssl_certs['client_key']))
			opener.addheaders = []
			for k,v in self.headers.items():
				opener.addheaders.append((k, v))
			response = opener.open(url)
			data = response.read()
			data = json.loads(data)
		except urllib2.HTTPError, e:
			LOGGER.error('HTTPError - status code: {0}, received from {1}'.format(e.code, url))
		except urllib2.URLError, e:
			LOGGER.error('URLError - {0}'.format(e.reason))
		except ValueError, e:
			LOGGER.error('Error - {0} parsing JSON for url {1}'.format(e, url))
		
		return data

class CollectdMock(object):
    '''
    Mock of the collectd module.

    This is used when running the plugin locally.
    All log messages are printed to stdout.
    The Values() method will return an instance of CollectdValuesMock
    '''
    def __init__(self):
        self.value_mock = CollectdValuesMock

    def debug(self, msg):
        print msg

    def info(self, msg):
        print msg

    def notice(self, msg):
        print msg

    def warning(self, msg):
        print msg

    def error(self, msg):
        print msg

    def Values(self):
        return (self.value_mock)()


class CollectdValuesMock(object):
    '''
    Mock of the collectd Values class.

    Instanes of this class are returned by CollectdMock, which is used to mock
    collectd when running locally.
    The dispatch() method will print the emitted record to stdout.
    The code is copied from nginx_plus_collectd.py
    '''
    def dispatch(self):
        if not getattr(self, 'host', None):
            self.host = os.environ.get('COLLECTD_HOSTNAME', 'localhost')

        identifier = '%s/%s' % (self.host, self.plugin)
        if getattr(self, 'plugin_instance', None):
            identifier += '-' + self.plugin_instance
        identifier += '/' + self.type

        if getattr(self, 'type_instance', None):
            identifier += '-' + self.type_instance

        print '[PUTVAL]', identifier, ':'.join(map(str, [int(self.time)] + self.values))

    def __str__(self):
        attrs = []
        for name in dir(self):
            if not name.startswith('_') and name != 'dispatch':
                attrs.append("{}={}".format(name, getattr(self, name)))
        return "<CollectdValues {}>".format(' '.join(attrs))

class CollectdConfigMock(object):
    '''
    Mock of the collectd Config class.

    This class is used to configure the plugin when running locally.
    The children field is expected to be a list of CollectdConfigChildMock.
    The code is copied from nginx_plus_collectd.py
    '''
    def __init__(self, children=None):
        self.children = children or []

class CollectdConfigChildMock(object):
    '''
    Mock of the collectd Conf child class.

    This class is used to mock key:value pairs normally pulled from the plugin
    configuration file.
    The code is copied from nginx_plus_collectd.py
    '''
    def __init__(self, key, values):
        self.key = key
        self.values = values

# Set up logging
LOG_FILE_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
LOG_FILE_MESSAGE_FORMAT = '[%(levelname)s] [consul-collectd] [%(asctime)s UTC]: %(message)s'
formatter = logging.Formatter(fmt=LOG_FILE_MESSAGE_FORMAT, datefmt=LOG_FILE_DATE_FORMAT)
log_handler = CollectdLogHandler('consul-collectd', False)
log_handler.setFormatter(formatter)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
LOGGER.propagate = False
LOGGER.addHandler(log_handler)

if __name__ == '__main__':
    cli_api_host = sys.argv[1] if len(sys.argv) > 1 else '10.2.5.60'
    cli_api_port = sys.argv[2] if len(sys.argv) > 2 else 8500

    collectd = CollectdMock()
    mock_config_api_host_child = CollectdConfigChildMock(API_HOST, [cli_api_host])
    mock_config_api_port_child = CollectdConfigChildMock(API_PORT, [cli_api_port])
    mock_config_api_protocol_child = CollectdConfigChildMock(API_PROTOCOL, ['http'])
    mock_config_telemetry_server_child = CollectdConfigChildMock(TELEMETRY_SERVER, [False])
    mock_config_dimension_child = CollectdConfigChildMock(DIMENSIONS, ['foo=bar'])

    mock_config = CollectdConfigMock([mock_config_api_host_child,
                                      mock_config_api_port_child,
                                      mock_config_api_protocol_child,
                                      mock_config_telemetry_server_child,
                                      mock_config_dimension_child])

    plugin_manager = ConsulPluginManager()
    plugin_manager.configure_callback(mock_config)

    while True:
        plugin_manager.read_callback()
        time.sleep(5)

else:

	plugin_manager = ConsulPluginManager()
	collectd.register_config(plugin_manager.configure_callback)
	collectd.register_read(plugin_manager.read_callback)
	collectd.register_shutdown(plugin_manager.shutdown_callback)
