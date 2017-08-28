# collectd Consul Plugin

A consul collectd plugin which users can use to send metrics from consul agent to SignalFx.

## Installation

Checkout this repository somewhere on your system accessible by collectd. The suggested location is /usr/share/collectd/
Install the Python requirements with sudo pip install -r requirements.txt
Configure the plugin (see below)
Restart collectd

## Requirements

* collectd 4.9 or later (for the Python plugin)
* Python 2.6 or later
* Consul 0.7.0 or later

## Configuration

Consul provides various runtime metrics about the performance of different libraries and subsystems. One way to access this data is to configure Consul to stream it to our plugin. Consul version 0.9.1 and above makes these metrics available at the API endpoint `/agent/metrics`.

If running Consul version below 0.9.1, configure the Consul agents that are to be monitored to send telemetry by adding the below configuration to Consul agents configuration file.
```
{"telemetry":
  {"statsd_address": "host:port"}
}
```
The plugin will start a UDP server listening at above host and port.

Next configure the collectd-consul plugin by using the below given example configuration file as a guide, provide values for the configuration options listed in the table that make sense for your environment.

**Configuration Option** | **Description** | **Default Value**
:------------------------|:----------------|:------------------
ApiHost	| IP address or DNS to which the Consul HTTP/HTTPS server binds to on the instance to be monitored | `localhost`
ApiPort |	Port to which the Consul HTTP/HTTPS server binds to on the instance to be monitored |	`8500`
ApiProtocol | Possible values - *http* or *https*	| `http`
AclToken | Consul ACL token. | None
TelemetryServer	| Possible values - *true* or *false*<br>Set to *true* to enable collecting Consul's internal metrics via UDP from Consul's telemetry.<br>If set to *false* and Consul version is 0.9.1 and above, the metrics will be collected from API.<br>If set to *false* and Consul version is less than 0.9.1, Consul's internal metrics will not be available. | `false`
TelemetryHost	| IP address or DNS to which consul is configured to send telemetry UDP packets. Relevant if TelemetryServer set to true. |	`localhost`
TelemetryPort	| Port to which consul is configured to send telemetry UDP packets. Relevant if TelemetryServer set to true. |	`8125`
EnhancedMetrics | Possible values - *true* or *false*<br>Set to *true* to enable collecting all metrics from Consul's runtime telemetry send via UDP or /agent/metrics endpoint. | `false`
ExcludeMetric | Blocks metrics by prefix matching, if *EnhancedMetrics* is true. This can be used to exclude metrics sent from `/agent/metrics` endpoint or from Consul's runtime telemetry send via UDP. | None
IncludeMetric | Allows metrics by prefix matching, if *EnhancedMetrics* is false. This can be used to include metrics sent from `/agent/metrics` endpoint or from Consul's runtime telemetry send via UDP. | None
SfxToken |	SignalFx org access token. If added to the config, an event is sent to SignalFx on leader transition and can be viewed on the Consul dashboard. |	None
Dimension | Add single custom global dimension to your metrics, formatted as "key=value" | None
Dimensions | Add multiple global dimensions, formatted as "key1=value1,key2=value2,..." | None
CaCertificate | If Consul server has https enabled for the API, provide the path to the CA Certificate. | None
ClientCertificate | If client-side authentication is enabled, provide the path to the certificate file. | None
ClientKey | If client-side authentication is enabled, provide the path to the key file. | None
Debug | Possible values - *true* or *false*<br> | `false`

Note that multiple Consul instances can be configured in the same file. If using Consul version 0.9.1 and above, it is recommended to not enable the TelemetryServer and let the plugin collect metrics from API for better performance.

```
LoadPlugin python

<Plugin python>
  ModulePath "/usr/share/collectd/collectd-consul"

  Import consul_plugin
  <Module consul_plugin>
    ApiHost "server-1"
    ApiPort 8500
    ApiProtocol "http"
    AclToken "token"
    SfxToken "SignalFX_token"
    TelemetryServer true
    TelemetryHost "17.2.3.4"
    TelemetryPort 8125
    EnhancedMetrics true
    ExcludeMetric "consul.consul.http"
    ExcludeMetric "consul.memberlist"
    Dimension "foo=bar"
    Debug true
  </Module>
  <Module consul_plugin>
    ApiHost "server-2"
    ApiPort 8500
    ApiProtocol "http"
    IncludeMetric "consul.fsm"
    Dimensions "foo=bar,bar=baz"
    TelemetryServer false
  </Module>
</Plugin>
```

**Example usage of the `ExcludeMetric` and `IncludeMetric` config option**

With the above example configuration values, for the *server-1* instance all metrics starting with the prefix **consul.consul.http** will be blocked.
So metrics *consul.consul.http.GET.v1.agent.self* and *consul.consul.http.POST.v1.kv.KEY* will be blocked. Similarly, for the *server-2* instance all metrics starting with the prefix **consul.fsm** will be allowed.
