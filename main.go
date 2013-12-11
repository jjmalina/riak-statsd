//
// riak-statsd
// Sends Riak stats to statsd every 60s.
//
// Usage:
//   -nodename="riak": Riak node name
//   -riak_host="127.0.0.1": Riak host
//   -riak_http_port=8098: Riak HTTP port
//   -statsd_host="127.0.0.1": Statsd host
//   -statsd_port=8125: Statsd host


package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// The interesting metric keys and their statsd types
var MetricTypes = map[string]string{
	"node_gets":                    "g",
	"node_puts":                    "g",
	"vnode_gets":                   "g",
	"vnode_puts":                   "g",
	"read_repairs":                 "g",
	"read_repairs_total":           "g",
	"node_get_fsm_objsize_mean":    "g",
	"node_get_fsm_objsize_median":  "g",
	"node_get_fsm_objsize_95":      "g",
	"node_get_fsm_objsize_100":     "g",
	"node_get_fsm_time_mean":       "ms",
	"node_get_fsm_time_median":     "ms",
	"node_get_fsm_time_95":         "ms",
	"node_get_fsm_time_100":        "ms",
	"node_put_fsm_time_mean":       "ms",
	"node_put_fsm_time_median":     "ms",
	"node_put_fsm_time_95":         "ms",
	"node_put_fsm_time_100":        "ms",
	"node_get_fsm_siblings_mean":   "g",
	"node_get_fsm_siblings_median": "g",
	"node_get_fsm_siblings_95":     "g",
	"node_get_fsm_siblings_100":    "g",
	"memory_processes_used":        "g",
	"node_get_fsm_active":          "g",
	"node_get_fsm_active_60s":      "g",
	"node_get_fsm_in_rate":         "g",
	"node_get_fsm_out_rate":        "g",
	"node_get_fsm_rejected":        "g",
	"node_get_fsm_rejected_60s":    "g",
	"node_get_fsm_rejected_total":  "g",
	"node_put_fsm_active":          "g",
	"node_put_fsm_active_60s":      "g",
	"node_put_fsm_in_rate":         "g",
	"node_put_fsm_out_rate":        "g",
	"node_put_fsm_rejected":        "g",
	"node_put_fsm_rejected_60s":    "g",
	"node_put_fsm_rejected_total":  "g",
	"index_fsm_create":             "g",
	"index_fsm_create_error":       "g",
	"index_fsm_active":             "g",
	"list_fsm_create":              "g",
	"list_fsm_create_error":        "g",
	"list_fsm_active":              "g",
	"sys_process_count":            "g",
	"coord_redirs_total":           "g",
	"pbc_connects":                 "g",
	"pbc_active":                   "g",
}

func getRiakStats(host string, port int) (*map[string]interface{}, error) {
	url := fmt.Sprintf("http://%s:%d/stats", host, port)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data := make(map[string]interface{})
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func pingRiak(host string, port int) error {
	url := fmt.Sprintf("http://%s:%d/ping", host, port)
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.New("Error reading response")
	}
	msg := string(body)
	if msg != "OK" {
		return errors.New(fmt.Sprintf("Not OK. Response was: %s", msg))
	}
	return nil
}

func prepareMetrics(nodename string, riakstats map[string]interface{}) *[]string {
	metrics := make([]string, len(MetricTypes))
	i := 0
	for key, st := range MetricTypes {
		value := riakstats[key]
		metrics[i] = fmt.Sprintf("%s.%s:%v|%s", nodename, key, value, st)
		i++
	}
	return &metrics
}

func sendRiakMetrics(conn *net.UDPConn, metrics *[]string) error {
	data := []byte(strings.Join(*metrics, "\n"))
	_, err := conn.Write(data)
	if err != nil {
		log.Println("Error sending metrics: %v", err)
	}
	return nil
}

func getAndSendRiakMetrics(conn *net.UDPConn, nodename string, host string, port int) {
	data, _ := getRiakStats(host, port)
	if data != nil {
		metrics := prepareMetrics(nodename, *data)
		sendRiakMetrics(conn, metrics)
	}
}

func main() {
	var statsdHost = flag.String("statsd_host", "127.0.0.1", "Statsd host")
	var statsdPort = flag.Int("statsd_port", 8125, "Statsd host")
	var nodename = flag.String("nodename", "riak", "Riak node name")
	var riakHost = flag.String("riak_host", "127.0.0.1", "Riak host")
	var riakHttpPort = flag.Int("riak_http_port", 8098, "Riak HTTP port")
	flag.Parse()

	// First ping to node to make sure it works
	err := pingRiak(*riakHost, *riakHttpPort)
	if err != nil {
		log.Fatalf("Error: %v", err)
		os.Exit(1)
	}
	statsd := fmt.Sprintf("%s:%d", *statsdHost, *statsdPort)
	addr, err := net.ResolveUDPAddr("udp", statsd)
	if err != nil {
		log.Fatalf("Couldn't resolve UDP addr: %v", err)
		os.Exit(1)
	}
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		log.Fatalf("Couldn't connect to statsd at %s", statsd)
		os.Exit(1)
	}

	// every 60s run hit the stats endpoint and then send to statsd
	interval := time.NewTicker(time.Second * 60)
	for _ = range interval.C {
		go getAndSendRiakMetrics(conn, *nodename, *riakHost, *riakHttpPort)
	}
}
