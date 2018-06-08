"""
Basic integration tests of Consul that make sure the plugin loads in collectd
and sends something
"""
from functools import partial as p
import os
from textwrap import dedent

from collectdtesting import run_collectd, run_container, container_ip
from collectdtesting.assertions import wait_for, has_datapoint_with_dim
import pytest

PLUGIN_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.mark.parametrize("version", [
    "0.8.0",
    "1.1.0",
    "latest",
])
def test_basic_metrics(version):
    serverconfig = '{"telemetry": {"statsd_address": "%s:8125"}}'
    config = dedent("""
        LoadPlugin python

        <Plugin python>
        ModulePath "/opt/collectd-plugin"

        Import consul_plugin
        <Module consul_plugin>
            ApiHost "{server1}"
            ApiPort 8500
            ApiProtocol "http"
            Dimension "test_server=server1"
            TelemetryServer {telemetry_server}
            TelemetryHost {collectd}
            TelemetryPort 8125
            Debug true
        </Module>
        <Module consul_plugin>
            ApiHost "{server2}"
            ApiPort 8500
            ApiProtocol "http"
            Dimensions "foo=bar,bar=baz,test_server=server2"
            TelemetryServer False
            Debug True
        </Module>
        <Module consul_plugin>
            ApiHost "{server3}"
            ApiPort 8500
            ApiProtocol "http"
            TelemetryServer False
        </Module>
        <Module consul_plugin>
            ApiHost "{client}"
            ApiPort 8500
            ApiProtocol "http"
            TelemetryServer False
        </Module>
        </Plugin>
    """)
    with run_collectd(config.format(server1='0.0.0.0', server2='0.0.0.0',
                                    server3='0.0.0.0', client='0.0.0.0',
                                    telemetry_server=False, collectd='0.0.0.0'), PLUGIN_DIR) as (ingest, collectd):
        tempfile_path = '/tmp/%s' % collectd.ip.replace('.', '')
        if not os.path.exists(tempfile_path):
            os.makedirs(tempfile_path)
        tempfile_path = tempfile_path + '/server1.json'
        with open(tempfile_path, 'w') as conf_file:
            conf_file.write(serverconfig % collectd.ip)
            conf_file.flush()

        with run_container("consul:%s" % version,
                           command=['consul', 'agent', '-node=server-1', '-server', '-bootstrap',
                                    '-node-id=31de20e0-43fc-43b0-8bbf-436f1db7e16a',
                                    '-data-dir=/tmp/data', '-config-dir=/consul/config',
                                    '-client=0.0.0.0'],
                           volumes={tempfile_path: {'bind': '/consul/config/server1.json'}}) as server1:
            server1_ip = container_ip(server1)
            with run_container("consul:%s" % version,
                               command=['consul', 'agent', '-node=server-2',
                                        '-node-id=31de20e0-43fc-43b0-8bbf-436f1db7e16b',
                                        '-server', '-retry-join=%s' % server1_ip,
                                        '-data-dir=/tmp/data', '-client=0.0.0.0']) as server2:
                server2_ip = container_ip(server2)
                with run_container("consul:%s" % version,
                                   command=[
                                        'consul', 'agent', '-node=server-3',
                                        '-node-id=31de20e0-43fc-43b0-8bbf-436f1db7e16c',
                                        '-server', '-retry-join=%s' % server1_ip,
                                        '-data-dir=/tmp/data', '-client=0.0.0.0'
                                    ]) as server3:
                    server3_ip = container_ip(server3)
                    with run_container("consul:%s" % version,
                                       command=[
                                        'consul', 'agent', '-node=client',
                                        '-node-id=31de20e0-43fc-43b0-8bbf-436f1db7e16d',
                                        '-retry-join=%s' % server1_ip,
                                        '-data-dir=/tmp/data', '-client=0.0.0.0'
                                       ]) as client:
                        client_ip = container_ip(client)
                        collectd.reconfig(config.format(server1=server1_ip, server2=server2_ip,
                                          server3=server3_ip, client=client_ip,
                                          telemetry_server=True, collectd=collectd.ip))
                        assert wait_for(p(has_datapoint_with_dim, ingest, "plugin", "consul"), 60), \
                            "Didn't received a consul datapoint"
