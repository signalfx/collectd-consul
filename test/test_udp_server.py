#!/usr/bin/env python
import mock
import unittest
import sys
import os
from copy import deepcopy
import time
import re
from functools import reduce
sys.path.insert(0, os.path.dirname(__file__))

sys.modules['collectd'] = mock.Mock()
from consul_plugin import UDPServer, default_telemetry

dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path + '/sample_packet', 'r') as f:
    sample_data = f.read()


def wait_for_udp_server(mock_socket, host, port, timeout=120):
    timeout_ts = time.time() + timeout
    while time.time() < timeout_ts:
        try:
            mock_socket.return_value.bind.assert_called_with((host, port))
            return True
        except AssertionError:
            time.sleep(0.5)
    return False


def wait_for_data_to_be_processed(udp_server, expected_stats, timeout=120):
    """
    helper function for udp server tests to wait for metrics to be processed
    """
    timeout_ts = time.time() + timeout
    while time.time() < timeout_ts:
        with udp_server.lock:
            if len(udp_server.stats) >= len(expected_stats):
                return True
        time.sleep(1)
    return False


class TestUDPServer(unittest.TestCase):

    def test_udp_server(self):
        self.maxDiff = None
        rpc_list = [0.003464, 0.003024, 0.014557]
        rpc_mean = reduce(lambda x, y: x + y, [0.003464, 0.003024, 0.014557])
        rpc_mean = rpc_mean/3
        expected_stats = {'consul.server-3.dc1.consul.runtime.num_goroutines':
                          {'value': 66.0,
                           'type': 'gauge'
                           },
                          'consul.server-3.dc1.consul.runtime.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'
                           },
                          'consul.runtime.alloc_bytes.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'
                           },
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.avg':
                          {'value': rpc_mean,
                           'type': 'gauge'
                           },
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.min':
                          {'value': min(rpc_list),
                           'type': 'gauge'
                           },
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.max':
                          {'value': max(rpc_list),
                           'type': 'gauge'
                           }
                          }

        default_telemetry_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                             for x in default_telemetry))
        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recvfrom.return_value = ('',
                                                              'example.com')
            udp_server = UDPServer('example.com', 8125,
                                   default_telemetry_regex)

            if not wait_for_udp_server(mock_socket, 'example.com', 8125, timeout=120):
                self.fail('timed out waiting for the udp server to be active')

        udp_server.sanitize_data(sample_data)

        if not wait_for_data_to_be_processed(udp_server, expected_stats, 120):
            self.fail(msg="test timed out waiting for sanitized metrics")

        with udp_server.lock:
            actual_response = deepcopy(udp_server.stats)
        udp_server.terminate.set()

        while udp_server.isAlive():
            time.sleep(0.5)

        self.assertEqual(set(actual_response.keys()),
                          set(expected_stats.keys()))

        for k, v in list(expected_stats.items()):
            self.assertTrue(set(v.items()).issubset(
                set(actual_response[k].items())))

    def test_udp_server_with_enhanced_metrics(self):
        rpc_list = [0.003464, 0.003024, 0.014557]
        rpc_mean = reduce(lambda x, y: x + y, [0.003464, 0.003024, 0.014557])
        rpc_mean = rpc_mean/3
        expected_stats = {'consul.server-3.dc1.consul.runtime.num_goroutines':
                          {'value': 66.0,
                           'type': 'gauge'},
                          'consul.server-3.dc1.consul.runtime.sys_bytes':
                          {'value': 13576440.0,
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.avg':
                          {'value': rpc_mean,
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.min':
                          {'value': min(rpc_list),
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.max':
                          {'value': max(rpc_list),
                           'type': 'gauge'},
                          'consul.runtime.alloc_bytes.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'},
                          'consul.server-3.dc1.consul.runtime.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'}
                          }
        exclude_metrics = ['consul.memberlist']
        exclude_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                            for x in exclude_metrics))
        default_telemetry_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                             for x in default_telemetry))

        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recvfrom.return_value = ('',
                                                              'example.com')
            udp_server = UDPServer('example.com', 8125, default_telemetry_regex,
                                   enhanced_metrics=True, exclude_regex=exclude_regex)

            if not wait_for_udp_server(mock_socket, 'example.com', 8125, timeout=120):
                self.fail('timed out waiting for the udp server to be active')

        udp_server.sanitize_data(sample_data)

        if not wait_for_data_to_be_processed(udp_server, expected_stats, 120):
            self.fail(msg="test timed out waiting for sanitized metrics")

        with udp_server.lock:
            actual_response = deepcopy(udp_server.stats)
        udp_server.terminate.set()

        while udp_server.isAlive():
            time.sleep(0.5)

        self.assertEqual(set(actual_response.keys()),
                          set(expected_stats.keys()))

        for k, v in list(expected_stats.items()):
            self.assertTrue(set(v.items()).issubset(set(
                actual_response[k].items())))

    def test_udp_server_with_include_metrics(self):
        rpc_list = [0.003464, 0.003024, 0.014557]
        rpc_mean = reduce(lambda x, y: x + y, [0.003464, 0.003024, 0.014557])
        rpc_mean = rpc_mean/3
        expected_stats = {'consul.server-3.dc1.consul.runtime.num_goroutines':
                          {'value': 66.0,
                           'type': 'gauge'},
                          'consul.server-3.dc1.consul.runtime.sys_bytes':
                          {'value': 13576440.0,
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.avg':
                          {'value': rpc_mean,
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.min':
                          {'value': min(rpc_list),
                           'type': 'gauge'},
                          'consul.raft.replication.appendEntries.rpc.server-3.dc1.consul.max':
                          {'value': max(rpc_list),
                           'type': 'gauge'},
                          'consul.runtime.alloc_bytes.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'},
                          'consul.server-3.dc1.consul.runtime.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'},
                          'consul.memberlist.udp.received':
                          {'value': 154+27,
                           'type': 'gauge'},
                          'consul.memberlist.gossip.avg':
                          {'value': 0.006504,
                           'type': 'gauge'
                           },
                          'consul.memberlist.gossip.min':
                          {'value': 0.006504,
                           'type': 'gauge'
                           },
                          'consul.memberlist.gossip.max':
                          {'value': 0.006504,
                           'type': 'gauge'
                           }
                          }
        include_metrics = ['consul.memberlist.gossip',
                           'consul.memberlist.udp.received',
                           'consul.server-3.dc1.consul.runtime.sys_bytes']
        include_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                            for x in include_metrics))
        default_telemetry_regex = re.compile('|'.join('(?:{0})'.format(re.escape(x))
                                             for x in default_telemetry))

        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recvfrom.return_value = ('',
                                                              'example.com')
            udp_server = UDPServer('example.com', 8125, default_telemetry_regex,
                                   enhanced_metrics=False, exclude_regex=include_regex,
                                   include_regex=include_regex)
            
            if not wait_for_udp_server(mock_socket, 'example.com', 8125, timeout=120):
                self.fail('timed out waiting for the udp server to be active')

        udp_server.sanitize_data(sample_data)

        if not wait_for_data_to_be_processed(udp_server, expected_stats, 120):
            self.fail(msg="test timed out waiting for sanitized metrics")

        with udp_server.lock:
            actual_response = deepcopy(udp_server.stats)
        udp_server.terminate.set()

        while udp_server.isAlive():
            time.sleep(0.5)

        self.assertEqual(set(actual_response.keys()),
                          set(expected_stats.keys()))

        for k, v in list(expected_stats.items()):
            self.assertTrue(set(v.items()).issubset(set(
                actual_response[k].items())))

# if __name__ == "__main__":
#     logging.basicConfig( stream=sys.stderr )
#     logging.getLogger( "TestUDPServer.test_udp_server" )\
#      .setLevel( logging.DEBUG )

#     unittest.main()
