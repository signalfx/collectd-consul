#!/usr/bin/env python
import mock
import unittest
import sys
import json
import os
from urllib2 import HTTPError
from urllib2 import URLError
sys.path.insert(0, os.path.dirname(__file__))
sys.modules['collectd'] = mock.Mock()

from consul_plugin import ConsulAgent


class MockHTTPSHandler(object):

    def __init__(self, ca_cert, client_cert, client_key):
        self.ca_cert = ca_cert
        self.client_cert = client_cert
        self.client_key = client_key


def sample_response(path):

    dir_path = os.path.dirname(os.path.realpath(__file__))
    file = dir_path + '/resources' + path
    with open(file, 'r') as data:
        return json.load(data)


class ConsulAgentTest(unittest.TestCase):
    def setUp(self):
        self.base_url = 'http://example.com:8888/v1'
        self.api_host = 'example.com'
        self.api_port = '8888'
        self.api_protocol = 'http'
        self.acl_token = 'acl_token'
        self.sfx_token = 'sfx_token'
        self.ssl_certs = {'ca_cert': None,
                          'client_cert': None,
                          'client_key': None}
        self.agent = ConsulAgent(self.api_host, self.api_port,
                                 self.api_protocol, self.acl_token,
                                 self.sfx_token, self.ssl_certs)
        self.agent.config = sample_response('/agent/self')

    @mock.patch('urllib2.build_opener')
    def test_return_json_on_ok(self, mock_urllib_opener):

        mock_response = mock.Mock()
        mock_response.code = 200
        mock_response.read.return_value = '{"foo": "bar", "bat": "baz"}'

        mock_urllib_opener().open.return_value = mock_response

        url = 'http://example.com:8500'
        actual_response = self.agent._send_request(url)
        expected_response = {"foo": "bar", "bat": "baz"}

        expected_header = [('X-Consul-Token', self.agent.acl_token)]
        self.assertEqual(mock_urllib_opener.return_value.addheaders,
                         expected_header)
        mock_urllib_opener().open.assert_called_with(url)

        self.assertDictEqual(expected_response, actual_response)

    @mock.patch('urllib2.build_opener')
    def test_return_none_on_exception(self, mock_urllib_opener):

        mock_urllib_opener.side_effect = HTTPError(*[None] * 5)
        self.assertIsNone(self.agent._send_request('http://example.com:8500'))

        mock_urllib_opener.side_effect = URLError('Mock URLError')
        self.assertIsNone(self.agent._send_request('http://exampl.com:8500'))

    @mock.patch('urllib2.Request')
    @mock.patch('urllib2.urlopen')
    def test_send_leader_change_event_with_ok_status(self, mock_urlopen,
                                                     mock_request):
        dimensions = {'foo': 'bar'}

        mock_response = mock.Mock()
        mock_urlopen.return_value = mock_response

        self.agent._send_leader_change_event(dimensions)

        mock_request.return_value.add_header.assert_has_calls(
                            [mock.call('Content-Type', 'application/json')],
                            [mock.call('X-SF-TOKEN', self.agent.sfx_token)])

        mock_urlopen.assert_called_with(mock_request.return_value)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_local_config(self, mock_send_request):
        expected_url = '{0}/agent/self'.format(self.base_url)
        self.agent.get_local_config()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_dc_leader(self, mock_send_request):
        expected_url = '{0}/status/leader'.format(self.base_url)
        self.agent.get_dc_leader()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_nodes_in_dc(self, mock_send_request):
        expected_url = '{0}/catalog/nodes'.format(self.base_url)
        self.agent.get_nodes_in_dc()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_services_for_node(self, mock_send_request):
        expected_url = '{0}/catalog/node/example_node'.format(self.base_url)
        self.agent.get_services_for_node('example_node')
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_peers_in_dc(self, mock_send_request):
        expected_url = '{0}/status/peers'.format(self.base_url)
        self.agent.get_peers_in_dc()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_wan_coordinates(self, mock_send_request):
        expected_url = '{0}/coordinate/datacenters'.format(self.base_url)
        self.agent.get_wan_coordinates()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_lan_coordinates(self, mock_send_request):
        expected_url = '{0}/coordinate/nodes'.format(self.base_url)
        self.agent.get_lan_coordinates()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_health_checks(self, mock_send_request):
        expected_url = '{0}/health/state/any'.format(self.base_url)
        self.agent.get_health_checks()
        mock_send_request.assert_called_with(expected_url)

    @mock.patch('consul_plugin.ConsulAgent._send_request')
    def test_get_metrics(self, mock_send_request):
        expected_url = '{0}/agent/metrics'.format(self.base_url)
        self.agent.get_metrics()
        mock_send_request.assert_called_with(expected_url)

    def test_check_metrics_endpoint_available(self):
        self.assertTrue(self.agent.check_metrics_endpoint_available())

        self.agent.config['Config']['Version'] = '0.9.0'
        self.assertFalse(self.agent.check_metrics_endpoint_available())

    @mock.patch('consul_plugin.ConsulAgent.get_dc_leader',
                return_value=sample_response('/status/leader'))
    def test_is_leader(self, mock_call):

        self.assertFalse(self.agent.is_leader())

        self.agent.config['Config']['AdvertiseAddr'] = '10.2.5.60'
        self.assertTrue(self.agent.is_leader())

    def _mock_get_services_for_node(self, node):
        return sample_response('/catalog/node/{0}'.format(node))

    @mock.patch('consul_plugin.ConsulAgent.get_services_for_node')
    @mock.patch('consul_plugin.ConsulAgent.get_nodes_in_dc',
                return_value=sample_response('/catalog/nodes'))
    def test_get_catalog_map(self, mock_get_nodes, mock_get_services):
        mock_get_services.side_effect = self._mock_get_services_for_node
        expected_result = {'Nodes': {'client-1': 0,
                                     'server-1': 1,
                                     'server-2': 1,
                                     'server-3': 1,
                                     'google': 1},
                           'Services': {'search': 1,
                                        'consul': 3}}
        actual_result = self.agent.get_catalog_map()
        self.assertDictEqual(actual_result, expected_result)

    @mock.patch('consul_plugin.ConsulAgent.get_wan_coordinates',
                return_value=sample_response('/coordinate/datacenters'))
    def test_calculate_inter_dc_latency(self, mock_call):
        expected_dict_keys = {'dc2': {'avg': 0,
                                      'min': 0,
                                      'max': 0}}

        actual_response = self.agent.calculate_inter_dc_latency()
        self.assertIn('dc2', actual_response)
        self.assertEqual(set(expected_dict_keys['dc2'].keys()),
                         set(actual_response['dc2'].keys()))

    @mock.patch('consul_plugin.ConsulAgent.get_lan_coordinates',
                return_value=sample_response('/coordinate/nodes'))
    def test_calculate_intra_dc_latency(self, mock_call):
        expected_dict_keys = {'avg': 0,
                              'min': 0,
                              'max': 0}
        actual_response = self.agent.calculate_intra_dc_latency()
        self.assertEqual(set(expected_dict_keys.keys()),
                         set(actual_response.keys()))

    @mock.patch('consul_plugin.ConsulAgent.get_health_checks',
                return_value=sample_response('/health/state/any'))
    def test_get_health_check_stats(self, mock_call):
        expected_response = {'service': {},
                             'node': {'passing': 4}}

        actual_response = self.agent.get_health_check_stats()
        self.assertEqual(expected_response, actual_response)

    def test_get_global_dimensions(self):
        expected_response = {'datacenter': 'dc1',
                             'consul_mode': 'server',
                             'consul_node': 'server-3'}
        actual_response = self.agent.get_global_dimensions()
        self.assertDictEqual(actual_response, expected_response)
