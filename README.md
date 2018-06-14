# DIT Cloudfoundry autoscaler

A Cloudfoundry autoscaler that scales applications based on average CPU usage. It currently depends on a prometheus
exporter to expose the Cloudfoundry app metrics, which is provided by: https://github.com/alphagov/paas-metric-exporter

## Usage

The following environment variables can be set on a Cloudfoundry application to enable and configure autoscaling:

NOTE: the app does not need to be restaged for the autoscaler to pick up environment changes.

`AUTOSCALING` - can be set to on/off or test. 'test' means autoscaler will report potential autoscaling actions only.
`AUTOSCALING_MIN` - the minimum number of instances - default: 2
`AUTOSCALING_MAX` - the maximum number of instances - default: 10
