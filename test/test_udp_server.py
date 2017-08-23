#!/usr/bin/env python
import mock
import unittest
import sys
import os
from copy import deepcopy
import time
import re
sys.path.insert(0, os.path.dirname(__file__))

sys.modules['collectd'] = mock.Mock()
from consul_plugin import UDPServer

dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path + '/sample_packet', 'r') as f:
    sample_data = f.read()


class TestUDPServer(unittest.TestCase):

    def test_udp_server(self):
        self.maxDiff = None

        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recvfrom.return_value = ('',
                                                              'example.com')
            udp_server = UDPServer(host='example.com', port=8125)

            mock_socket.return_value.bind.assert_called_with(('example.com',
                                                              8125))
        udp_server.sanitize_data(sample_data)
        time.sleep(2)

        with udp_server.lock:
            actual_response = deepcopy(udp_server.stats)
        udp_server.terminate.set()

        rpc_list = [0.003464, 0.003024, 0.014557]
        rpc_mean = reduce(lambda x, y: x + y, [0.003464, 0.003024, 0.014557])
        rpc_mean = rpc_mean/3
        expected_stats = {'consul.memberlist.udp.sent': {'value': 154+27,
                                                         'type': 'gauge'
                                                         },
                          'consul.memberlist.udp.received': {'value': 154+27,
                                                             'type': 'gauge'
                                                             },
                          'consul.server-3.dc1.consul.runtime.num_goroutines':
                          {'value': 66.0,
                           'type': 'gauge'
                           },
                          'consul.server-3.dc1.consul.runtime.alloc_bytes':
                          {'value': 4815376.0,
                           'type': 'gauge'
                           },
                          'consul.server-3.dc1.consul.runtime.sys_bytes':
                          {'value': 13576440.0,
                           'type': 'gauge'
                           },
                          'consul.memberlist.gossip_mean':
                          {'value': 0.006504,
                           'type': 'gauge'
                           },
                          'consul.memberlist.gossip_min':
                          {'value': 0.006504,
                           'type': 'gauge'
                           },
                          'consul.memberlist.gossip_max':
                          {'value': 0.006504,
                           'type': 'gauge'
                           },
                          'consul.raft.rpc.appendEntries_mean':
                          {'value': rpc_mean,
                           'type': 'gauge'
                           },
                          'consul.raft.rpc.appendEntries_min':
                          {'value': min(rpc_list),
                           'type': 'gauge'
                           },
                          'consul.raft.rpc.appendEntries_max':
                          {'value': max(rpc_list),
                           'type': 'gauge'
                           },
                          }

        self.assertEquals(set(actual_response.keys()),
                          set(expected_stats.keys()))

        for k, v in expected_stats.items():
            self.assertTrue(set(v.items()).issubset(
                set(actual_response[k].items())))

    def test_udp_server_with_exclude_metric(self):

        exclude_metrics = ['consul.server-3.dc1.consul.runtime.alloc_bytes',
                           'consul.memberlist']
        exclude_regex = re.compile('|'.join('(?:{0})'.format(x)
                                            for x in exclude_metrics))
        with mock.patch('socket.socket') as mock_socket:
            mock_socket.return_value.recvfrom.return_value = ('',
                                                              'example.com')
            udp_server = UDPServer(host='example.com', port=8125,
                                   exclude_regex=exclude_regex)

            mock_socket.return_value.bind.assert_called_with(('example.com',
                                                              8125))
        udp_server.sanitize_data(sample_data)
        time.sleep(1)

        with udp_server.lock:
            actual_response = deepcopy(udp_server.stats)
        udp_server.terminate.set()

        rpc_list = [0.003464, 0.003024, 0.014557]
        rpc_mean = reduce(lambda x, y: x + y, [0.003464, 0.003024, 0.014557])
        rpc_mean = rpc_mean/3
        expected_stats = {'consul.server-3.dc1.consul.runtime.num_goroutines':
                          {'value': 66.0,
                           'type': 'gauge'},
                          'consul.server-3.dc1.consul.runtime.sys_bytes':
                          {'value': 13576440.0,
                           'type': 'gauge'},
                          'consul.raft.rpc.appendEntries_mean':
                          {'value': rpc_mean,
                           'type': 'gauge'},
                          'consul.raft.rpc.appendEntries_min':
                          {'value': min(rpc_list),
                           'type': 'gauge'},
                          'consul.raft.rpc.appendEntries_max':
                          {'value': max(rpc_list),
                           'type': 'gauge'}
                          }

        self.assertEquals(set(actual_response.keys()),
                          set(expected_stats.keys()))

        for k, v in expected_stats.items():
            self.assertTrue(set(v.items()).issubset(set(
                actual_response[k].items())))

# if __name__ == "__main__":
#     logging.basicConfig( stream=sys.stderr )
#     logging.getLogger( "TestUDPServer.test_udp_server" )\
#      .setLevel( logging.DEBUG )

#     unittest.main()
