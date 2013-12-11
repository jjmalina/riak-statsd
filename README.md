# riak-statsd

Just a little program that polls the [riak http stats endpoint](http://docs.basho.com/riak/latest/ops/running/stats-and-monitoring/#Statistics-from-Riak) every 60s and sends the data to statsd. Nothing fancy.

Why Go? Because it compiles to a binary :)

## usage

* -nodename="riak": Riak node name
* -riak_host="127.0.0.1": Riak host
* -riak_http_port=8098: Riak HTTP port
* -statsd_host="127.0.0.1": Statsd host
* -statsd_port=8125: Statsd port
