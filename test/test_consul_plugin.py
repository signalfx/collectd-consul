#!/usr/bin/env python
import os
import sys
import json
import unittest
from mock import Mock, MagicMock, patch
import re
sys.path.insert(0, os.path.dirname(__file__))
# Mock out the collectd module
sys.modules['collectd'] = Mock()
import consul_collectd


class MockMetricSink(object):
    def __init__(self):
        self.captured_records = []

    def emit(self, metric_record):
        self.captured_records.append(metric_record)


class TestConsulPlugin(unittest.TestCase):

    @patch('consul_collectd.MetricSink')
    def setUp(self, mock_sink):
        self.maxDiff = None
        mock_consul_agent = self._build_mock_consul_agent()
        mock_sink.side_effect = MockMetricSink
        custom_dimensions = {'foo': 'bar'}
        self.plugin_conf = {'api_host': 'example',
                            'api_port': 8500,
                            'api_protocol': 'http',
                            'telemetry_server': False,
                            'telemetry_host': None,
                            'telemetry_port': None,
                            'acl_token': None,
                            'sfx_token': None,
                            'ssl_certs': {'ca_cert': None,
                                          'client_cert': None,
                                          'client_key': None},
                            'exclude_metrics_regex': None,
                            'custom_dimensions': custom_dimensions,
                            'debug': False
                            }
        with patch('consul_collectd.ConsulAgent') as m_agent:
            m_agent.return_value = mock_consul_agent
            self.plugin = consul_collectd.ConsulPlugin(self.plugin_conf)
            self.plugin.global_dimensions.update(
                            self.plugin.consul_agent.get_global_dimensions())

    def test_custom_dimensions(self):

        actual = self.plugin.global_dimensions
        expected = {'foo': 'bar',
                    'datacenter': 'dc1',
                    'consul_mode': 'server',
                    'consul_node': 'server-3'}
        self.assertDictEqual(actual, expected)

    def test_enable_server(self):
        self.assertFalse(self.plugin.enable_server)
        self.assertIsNone(self.plugin.udp_server)

    def test_fetch_server_state(self):

        dimensions = {'consul_server_state': 'follower'}
        dimensions.update(self.plugin.global_dimensions)
        expected_record = consul_collectd.MetricRecord(
            'consul.is_leader',
            'gauge',
            0,
            dimensions)

        actual_records = self.plugin._fetch_server_state()

        self._validate_single_record(expected_record, actual_records[0])

    def test_fetch_server_state_leader(self):
        expected_dim = {'consul_server_state': 'leader'}
        expected_dim.update(self.plugin.global_dimensions)
        expected_metric = consul_collectd.MetricRecord('consul.is_leader',
                                                       'gauge',
                                                       1,
                                                       expected_dim)

        actual_metric = self.plugin._fetch_server_state_leader()
        self.assertEquals(1, len(actual_metric))
        self._validate_single_record(expected_metric, actual_metric[0])

    def test_fetch_peers(self):
        expected_dim = self.plugin.global_dimensions
        expected_metric = consul_collectd.MetricRecord('consul.peers',
                                                       'gauge',
                                                       3,
                                                       expected_dim)
        actual_metric = self.plugin._fetch_peers()
        self.assertEquals(1, len(actual_metric))
        self._validate_single_record(expected_metric, actual_metric[0])

    def test_fetch_catalog_metrics(self):

        expected_records = {'consul.catalog.services.total':
                            [{'type': 'gauge',
                              'value':  2,
                              'dimensions': self.plugin.global_dimensions}],
                            'consul.catalog.nodes.total':
                            [{'type': 'gauge',
                              'value': 5,
                              'dimensions': self.plugin.global_dimensions}],
                            'consul.catalog.nodes_by_service': [],
                            'consul.catalog.services_by_node': []
                            }

        service_to_nodes = [('search', 1), ('consul', 3)]
        for (service, val) in service_to_nodes:
            dim = {'consul_service': service}
            dim.update(self.plugin.global_dimensions)
            del dim['consul_node']
            metric = {'type': 'gauge',
                      'value': val,
                      'dimensions': dim}
            expected_records['consul.catalog.nodes_by_service'].append(metric)

        node_to_services = [('client-1', 0),
                            ('server-1', 1),
                            ('server-2', 1),
                            ('server-3', 1),
                            ('google', 1)]
        for (node, val) in node_to_services:
            dim = {}
            dim.update(self.plugin.global_dimensions)
            dim['consul_node'] = node
            metric = {'type': 'gauge',
                      'value': val,
                      'dimensions': dim}
            expected_records['consul.catalog.services_by_node'].append(metric)

        actual_records = self.plugin._fetch_catalog_metrics()

        self._validate_records_list(expected_records, actual_records)

    def test_fetch_dc_network_metrics(self):

        expected_records = []
        dimensions = {'destination_dc': 'dc2'}
        dimensions.update(self.plugin.global_dimensions)
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.dc.latency.avg',
                                         'gauge',
                                         0.41374872289293463,
                                         dimensions
                                         ))
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.dc.latency.min',
                                         'gauge',
                                         0.02,
                                         dimensions
                                         ))
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.dc.latency.max',
                                         'gauge',
                                         0.5702953069636298,
                                         dimensions
                                         ))
        actual_records = self.plugin._fetch_dc_network_metrics()

        self.assertEquals(len(expected_records), len(actual_records))

        for (idx, record) in enumerate(actual_records):
            self._validate_single_record(expected_records[idx], record)

    def test_fetch_node_network_metrics(self):

        expected_records = []
        dimensions = self.plugin.global_dimensions
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.node.latency.avg',
                                         'gauge',
                                         0.47051138518296787,
                                         dimensions
                                         ))
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.node.latency.min',
                                         'gauge',
                                         0.4154354798133847,
                                         dimensions
                                         ))
        expected_records.append(
            consul_collectd.MetricRecord('consul.network.node.latency.max',
                                         'gauge',
                                         0.5571355904035926,
                                         dimensions
                                         ))
        actual_records = self.plugin._fetch_node_network_metrics()

        self.assertEquals(len(expected_records), len(actual_records))

        for (idx, record) in enumerate(actual_records):
            self._validate_single_record(expected_records[idx], record)

    def test_fetch_health_ckecks(self):

        expected_records = []
        dimensions = self.plugin.global_dimensions
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.services.critical',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.services.warning',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.services.passing',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.nodes.critical',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.nodes.warning',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.health.nodes.passing',
            'gauge',
            4,
            dimensions))

        actual_records = self.plugin._fetch_health_ckecks()

        self.assertEquals(len(expected_records), len(actual_records))

        for (idx, record) in enumerate(actual_records):
            self._validate_single_record(expected_records[idx], record)

    def test_fetch_telemetry_metrics(self):

        expected_records = []
        dimensions = self.plugin.global_dimensions
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.fsm.coordinate.batch-update_mean',
            'gauge',
            0.05109499953687191,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.fsm.coordinate.batch-update_max',
            'gauge',
            0.05452900007367134,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.fsm.coordinate.batch-update_min',
            'gauge',
            0.04766099900007248,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.http.GET.v1.coordinate.nodes_mean',
            'gauge',
            0.7903540134429932,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.http.GET.v1.coordinate.nodes_max',
            'gauge',
            0.7903540134429932,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.consul.http.GET.v1.coordinate.nodes_min',
            'gauge',
            0.7903540134429932,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.gossip_mean',
            'gauge',
            0.007076957123354077,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.gossip_max',
            'gauge',
            0.015080999583005905,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.gossip_min',
            'gauge',
            0.0043750000186264515,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.raft.fsm.apply_mean',
            'gauge',
            0.10995149984955788,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.raft.fsm.apply_max',
            'gauge',
            0.11391499638557434,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.raft.fsm.apply_min',
            'gauge',
            0.10598800331354141,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.ip-10-2-2-84.ec2.internal.consul.session_ttl.active',
            'gauge',
            0,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.ip-10-2-2-84.ec2.internal.runtime.alloc_bytes',
            'gauge',
            4117728,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.ip-10-2-2-84.ec2.internal.runtime.free_count',
            'gauge',
            124627230,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.udp.received',
            'gauge',
            2174,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.udp.sent',
            'gauge',
            2174,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.tcp.sent',
            'gauge',
            1458,
            dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.tcp.accept',
            'gauge',
            1,
            dimensions))

        actual_records = self.plugin._fetch_telemetry_metrics()

        actual_records.sort(key=lambda x: x.name)
        expected_records.sort(key=lambda x: x.name)
        for (idx, record) in enumerate(actual_records):
            self._validate_single_record(expected_records[idx], record)

    def test_metrics_excluded(self):
        self.plugin.exclude_regex = re.compile(
            '(?:consul.memberlist.gossip)|'
            '(?:consul.ip-10-2-2-84.ec2)|'
            '(?:consul.http.GET)')

        actual_records = self.plugin._fetch_telemetry_metrics()

        not_expected_metrics = set([
            'consul.memberlist.gossip_mean',
            'consul.memberlist.gossip_min',
            'consul.memberlist.gossip_max',
            'consul.ip-10-2-2-84.ec2.internal.consul.session_ttl.active',
            'consul.ip-10-2-2-84.ec2.internal.runtime.alloc_bytes',
            'consul.ip-10-2-2-84.ec2.internal.runtime.free_count'])

        expected_metrics = set([
            'consul.consul.fsm.coordinate.batch-update_mean',
            'consul.consul.fsm.coordinate.batch-update_min',
            'consul.consul.fsm.coordinate.batch-update_max',
            'consul.consul.http.GET.v1.coordinate.nodes_mean',
            'consul.consul.http.GET.v1.coordinate.nodes_min',
            'consul.consul.http.GET.v1.coordinate.nodes_max',
            'consul.raft.fsm.apply_mean',
            'consul.raft.fsm.apply_min',
            'consul.raft.fsm.apply_max',
            'consul.memberlist.udp.received',
            'consul.memberlist.udp.sent',
            'consul.memberlist.tcp.sent',
            'consul.memberlist.tcp.accept'])

        actual_set = set()
        for record in actual_records:
            actual_set.add(record.name)

        self.assertEquals(len(expected_metrics), len(actual_set))
        self.assertTrue(actual_set.isdisjoint(not_expected_metrics))
        self.assertTrue(actual_set == expected_metrics)

    def test_telemetry_from_udp(self):
        mock_server = self._build_mock_udp_server()
        self.plugin.udp_server = mock_server
        self.plugin.enable_server = True
        actual_records = self.plugin._fetch_telemetry_metrics()

        expected_records = []
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.udp.received',
            'gauge',
            181,
            self.plugin.global_dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.server-3.dc1.consul.runtime.alloc_bytes',
            'gauge',
            4815376.0,
            self.plugin.global_dimensions
            ))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.memberlist.gossip_min',
            'gauge',
            0.006504,
            self.plugin.global_dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.raft.rpc.appendEntries_max',
            'gauge',
            0.014557,
            self.plugin.global_dimensions))
        expected_records.append(consul_collectd.MetricRecord(
            'consul.server-3.dc1.consul.runtime.sys_bytes',
            'gauge',
            13576440.0,
            self.plugin.global_dimensions))

        self.plugin.udp_server.read_complete.set.assert_called_once()
        self.assertEquals(len(expected_records), len(actual_records))

        actual_records.sort(key=lambda x: x.name)
        expected_records.sort(key=lambda x: x.name)
        for (idx, record) in enumerate(actual_records):
            self._validate_single_record(expected_records[idx], record)

    def test_shutdown(self):
        self.plugin.enable_server = True
        self.plugin.udp_server = self._build_mock_udp_server()
        self.plugin.shutdown()

        self.plugin.udp_server.terminate.set.assert_called_once()

    def test_read_for_leader(self):
        self.plugin.read()
        self.assertEquals(42, len(self.plugin.metric_sink.captured_records))

    def test_read_for_not_leader(self):
        self.plugin.consul_agent.is_leader.return_value = False
        self.plugin.read()
        self.assertEquals(23, len(self.plugin.metric_sink.captured_records))

    def test_read_with_udp(self):
        self.plugin.udp_server = self._build_mock_udp_server()
        self.plugin.enable_server = True
        self.plugin.read()
        self.assertEquals(28, len(self.plugin.metric_sink.captured_records))

    def _validate_records_list(self, expected_records, actual_records):
        actual_map = {}

        for record in actual_records:
            self.assertIsNotNone(record.timestamp)
            metric = {'value': record.value,
                      'type': record.type,
                      'dimensions': record.dimensions}
            actual_map[record.name] = actual_map.get(record.name, [])
            actual_map[record.name].append(metric)

        self.assertEquals(len(expected_records), len(actual_map))

        for k, v in actual_map.items():
            self.assertItemsEqual(expected_records[k], v)

    def _get_metrics_names_from_plugin(self):
        metric_names = []
        for emitter in self.plugin.emitters:
            metric_names.extend(
                self._extract_metic_names_from_emitter(emitter))
        return metric_names

    def _sample_response(self, path):

        dir_name = os.path.dirname(os.path.realpath(__file__))
        file = dir_name + '/resources' + path
        with open(file, 'r') as data:
            return json.load(data)

    def _mock_get_services_for_node(self, node):
        return self._sample_response('/catalog/node/{0}'.format(node))

    def _build_mock_consul_agent(self):

        mock_agent = Mock()
        api_host = 'example'
        api_port = 8500
        api_protocol = 'http'
        acl_token = None
        sfx_token = None
        ssl_certs = {'ca_cert': None,
                     'client_cert': None,
                     'client_key': None}
        agent = consul_collectd.ConsulAgent(api_host, api_port, api_protocol,
                                            acl_token, sfx_token, ssl_certs)
        agent.config = self._sample_response('/agent/self')['Config']

        mock_agent.config = self._sample_response('/agent/self')['Config']
        mock_agent.metrics_enabled = True

        mock_agent.is_leader = MagicMock(return_value=True)
        mock_agent.update_local_config = MagicMock()

        peers = self._sample_response('/status/peers')
        mock_agent.get_peers_in_dc = MagicMock(return_value=peers)

        metrics = self._sample_response('/agent/metrics')
        mock_agent.get_metrics = MagicMock(return_value=metrics)

        with patch('consul_collectd.ConsulAgent.get_nodes_in_dc') as n_call:
            with patch('consul_collectd.ConsulAgent.get_services_for_node') as s_call:
                n_call.return_value = self._sample_response('/catalog/nodes')
                s_call.side_effect = self._mock_get_services_for_node
                catalog_map = agent.get_catalog_map()
                mock_agent.get_catalog_map = \
                    MagicMock(return_value=catalog_map)

        with patch('consul_collectd.ConsulAgent.get_wan_coordinates') as mcall:
            mcall.return_value = self._sample_response(
                '/coordinate/datacenters')
            dc_latency_map = agent.calculate_inter_dc_latency()
            mock_agent.calculate_inter_dc_latency = \
                MagicMock(return_value=dc_latency_map)

        with patch('consul_collectd.ConsulAgent.get_lan_coordinates') as mcall:
            mcall.return_value = self._sample_response('/coordinate/nodes')
            node_latency_map = agent.calculate_intra_dc_latency()
            mock_agent.calculate_intra_dc_latency = \
                MagicMock(return_value=node_latency_map)

        with patch('consul_collectd.ConsulAgent.get_health_checks') as mcall:
            mcall.return_value = self._sample_response('/health/state/any')
            h_map = agent.get_health_check_stats()
            mock_agent.get_health_check_stats = MagicMock(return_value=h_map)

        dimensions = agent.get_global_dimensions()
        mock_agent.get_global_dimensions = MagicMock(return_value=dimensions)

        return mock_agent

    def _build_mock_udp_server(self):
        mock_server = MagicMock()
        mock_server.read_complete = MagicMock()
        mock_server.terminate = MagicMock()
        mock_server.lock = MagicMock()
        mock_server.stats = {'consul.memberlist.udp.received':
                             {'timestamp': 1503336692.968225,
                              'type': 'gauge',
                              'value': 181},
                             'consul.server-3.dc1.consul.runtime.alloc_bytes':
                             {'timestamp': 1503336692.96813,
                              'type': 'gauge',
                              'value': 4815376.0},
                             'consul.memberlist.gossip_min':
                             {'timestamp': 1503336692.968156,
                              'type': 'gauge',
                              'value': 0.006504},
                             'consul.raft.rpc.appendEntries_max':
                             {'timestamp': 1503336692.968211,
                              'type': 'gauge',
                              'value': 0.014557},
                             'consul.server-3.dc1.consul.runtime.sys_bytes':
                             {'timestamp': 1503336692.968135,
                              'type': 'gauge',
                              'value': 13576440.0}
                             }
        return mock_server

    def _validate_single_record(self, expected_record, actual_record):
        self.assertIsNotNone(actual_record)
        self.assertEquals(expected_record.name, actual_record.name)
        self.assertEquals(expected_record.type, actual_record.type)
        self.assertEquals(expected_record.value, actual_record.value)
        self.assertDictEqual(expected_record.dimensions,
                             actual_record.dimensions)
        self.assertIsNotNone(actual_record.timestamp)

    def _verify_records_captured(self, expected_records):
        for expected_record in expected_records:
            self.assertIsNotNone(next((
                record for record in self.mock_sink.captured_records
                if self._compare_records(expected_record, record)), None),
                'Captured record does not contain: {0} captured records: {1}'
                .format(expected_record.to_string(),
                        [record.to_string() for record in
                        self.mock_sink.captured_records]))

    def _compare_records(self, expected_record, actual_record):
        try:
            self._validate_single_record(expected_record, actual_record)
            return True
        except Exception:
            pass
        return False
