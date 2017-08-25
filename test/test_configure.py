#!/usr/bin/env python
import mock
import unittest
import re
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
sys.modules['collectd'] = mock.MagicMock()
import consul_plugin


class TestConfiure(unittest.TestCase):

    @mock.patch('consul_plugin.ConsulPlugin')
    def test_default_config(self, mock_plugin):

        mock_conf = mock.Mock()
        mock_conf.children = []
        default_telemetry_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                             for x in 
                                             consul_plugin.default_telemetry))

        expected_conf = {'api_host': 'localhost',
                         'api_port': 8500,
                         'api_protocol': 'http',
                         'telemetry_server': False,
                         'telemetry_host': 'localhost',
                         'telemetry_port': 8125,
                         'acl_token': None,
                         'sfx_token': None,
                         'ssl_certs': {'ca_cert': None, 'client_cert': None,
                                       'client_key': None},
                         'default_telemetry_regex': default_telemetry_regex,
                         'enhanced_metrics': False,
                         'exclude_metrics_regex': None,
                         'include_metrics_regex': None,
                         'custom_dimensions': {},
                         'debug': False}
        consul_plugin.configure_callback(mock_conf)
        args, kwargs = mock_plugin.call_args
        for k, v in args[0].items():
            self.assertIn(k, expected_conf)
            self.assertEquals(v, expected_conf[k])

    @mock.patch('consul_plugin.ConsulPlugin')
    def test_all_config(self, mock_plugin):
        mock_plugin.read = mock.MagicMock()
        mock_plugin.shutdown = mock.MagicMock()

        mock_conf = mock.Mock()

        expected_host = '10.2.5.60'
        mock_host = _build_mock_config_child('ApiHost', expected_host)

        expected_port = 8080
        mock_port = _build_mock_config_child('ApiPort', str(expected_port))

        expected_protocol = 'https'
        mock_protocol = _build_mock_config_child('ApiProtocol',
                                                 expected_protocol)

        expected_tele_server = True
        mock_tele_server = _build_mock_config_child('TelemetryServer',
                                                    'true')

        expected_tele_host = '17.2.3.4'
        mock_tele_host = _build_mock_config_child('TelemetryHost',
                                                  expected_tele_host)

        expected_tele_port = 8200
        mock_tele_port = _build_mock_config_child('TelemetryPort',
                                                  expected_tele_port)

        expected_acl_token = 'acl_token'
        mock_acl_token = _build_mock_config_child('AclToken',
                                                  expected_acl_token)

        expected_sfx_token = 'sfx_token'
        mock_sfx_token = _build_mock_config_child('SfxToken',
                                                  expected_sfx_token)

        expected_ca_cert = '/path/to/ca/cert'
        mock_ca_cert = _build_mock_config_child('CaCertificate',
                                                expected_ca_cert)

        expected_client_cert = '/path/to/clien/cert'
        mock_client_cert = _build_mock_config_child('ClientCertificate',
                                                    expected_client_cert)

        expected_client_key = '/path/to/client/key'
        mock_client_key = _build_mock_config_child('ClientKey',
                                                   expected_client_key)

        expected_ssl_certs = {'ca_cert': expected_ca_cert,
                              'client_cert': expected_client_cert,
                              'client_key': expected_client_key}

        expected_enhanced_metrics = True
        mock_enchanced_metrics = _build_mock_config_child('EnhancedMetrics',
                                                          'true')

        expected_exclude_metric_1 = 'consul.http'
        mock_exclude_metric_1 = _build_mock_config_child(
            'ExcludeMetric',
            expected_exclude_metric_1)

        expected_exclude_metric_2 = 'consul.rpc'
        mock_exclude_metric_2 = _build_mock_config_child(
            'ExcludeMetric',
            expected_exclude_metric_2)

        expected_include_metric_1 = 'consul.serf'
        mock_include_metric_1 = _build_mock_config_child(
            'IncludeMetric',
            expected_include_metric_1)

        expected_include_metric_2 = 'consul.memberlist'
        mock_include_metric_2 = _build_mock_config_child(
            'IncludeMetric',
            expected_include_metric_2)

        expected_telemetry_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                              for x in 
                                              consul_plugin.default_telemetry))
        include = ['consul.serf',
                   'consul.memberlist']
        exclude = ['consul.http',
                   'consul.rpc']

        expected_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                    for x in exclude))

        expected_include_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                            for x in include))

        expected_dimension = 'foo=bar'
        mock_dimension = _build_mock_config_child('Dimension',
                                                  expected_dimension)

        expected_dimensions = 'bar=baz,key=val'
        mock_dimensions = _build_mock_config_child('Dimensions',
                                                   expected_dimensions)
        expected_custom_dimensions = {'foo': 'bar', 'bar': 'baz', 'key': 'val'}

        expected_debug = False
        mock_debug = _build_mock_config_child('Debug', expected_debug)

        mock_conf.children = [mock_host,
                              mock_port,
                              mock_protocol,
                              mock_tele_server,
                              mock_tele_host,
                              mock_tele_port,
                              mock_acl_token,
                              mock_sfx_token,
                              mock_ca_cert,
                              mock_client_cert,
                              mock_client_key,
                              mock_enchanced_metrics,
                              mock_exclude_metric_1,
                              mock_exclude_metric_2,
                              mock_include_metric_1,
                              mock_include_metric_2,
                              mock_dimension,
                              mock_dimensions,
                              mock_debug]

        expected_conf = {'api_host': expected_host,
                         'api_port': expected_port,
                         'api_protocol': expected_protocol,
                         'telemetry_server': expected_tele_server,
                         'telemetry_host': expected_tele_host,
                         'telemetry_port': expected_tele_port,
                         'acl_token': expected_acl_token,
                         'sfx_token': expected_sfx_token,
                         'ssl_certs': expected_ssl_certs,
                         'default_telemetry_regex': expected_telemetry_regex,
                         'enhanced_metrics': expected_enhanced_metrics,
                         'exclude_metrics_regex': expected_regex,
                         'include_metrics_regex': expected_include_regex,
                         'custom_dimensions': expected_custom_dimensions,
                         'debug': expected_debug}

        consul_plugin.configure_callback(mock_conf)
        args, kwargs = mock_plugin.call_args
        for k, v in args[0].items():
            if k not in ['exclude_metrics_regex',
                         'include_metrics_regex',
                         'default_telemetry_regex']:
                self.assertIn(k, expected_conf)
                self.assertEquals(v, expected_conf[k])
            else:
                self.assertEquals(v.pattern, expected_conf[k].pattern)

        consul_plugin.collectd.register_read.assert_called_once()
        consul_plugin.collectd.register_read.assert_called_with(
            mock_plugin().read, name='{}:{}'.format(expected_host,
                                                    expected_port))

        consul_plugin.collectd.register_shutdown.assert_called_once()
        consul_plugin.collectd.register_shutdown.assert_called_with(
            mock_plugin().shutdown)


def _build_mock_config_child(key, value):
    mock_config_child = mock.Mock()
    mock_config_child.key = key
    mock_config_child.values = [value]
    return mock_config_child
