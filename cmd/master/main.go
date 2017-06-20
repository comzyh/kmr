package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/naturali/kmr/master"
)

var (
	port       = flag.String("port", ":50051", "port")
	jobName    = flag.String("jobname", "", "jobName")
	inputFile  = flag.String("file", "", "input file path")
	dataDir    = flag.String("intermediate-dir", "/tmp/", "directory of intermediate files")
	nReduce    = flag.Int("nReduce", 1, "number of reducers")
	local      = flag.Bool("local", false, "Wheter use local run")
	namespace  = flag.String("namespace", "kmr", "kubernetes namespace to run KMR task")
	configFile = flag.String("config", "", "MapReduce Job description JSON file, should be http(s) URL or a filepath")
)

func main() {
	flag.Parse()
	var err error

	// k8s client
	var config *rest.Config
	var clientset *kubernetes.Clientset
	if *local == false {
		k8sSchema := os.Getenv("KUBERNETES_API_SCHEMA")
		if k8sSchema != "http" { // InCluster
			config, err = rest.InClusterConfig()
			if err != nil {
				log.Fatalf("Can't get incluster config, %v", err)
			}
		} else { // For debug usage. > source dev_environment.sh
			host := os.Getenv("KUBERNETES_SERVICE_HOST")
			port := os.Getenv("KUBERNETES_SERVICE_PORT")

			config = &rest.Config{
				Host: fmt.Sprintf("%s://%s", k8sSchema, net.JoinHostPort(host, port)),
			}
			token := os.Getenv("KUBERNETES_API_ACCOUNT_TOKEN")
			if len(token) > 0 {
				config.BearerToken = token
			}
		}
		clientset, err = kubernetes.NewForConfig(config)
		if err != nil {
			log.Fatalf("Can't init kubernetes client , %v", err)
		}
	} else {
		clientset = nil
	}

	master.NewMapReduce(*port, *jobName, strings.Split(*inputFile, ","), *dataDir,
		*nReduce, clientset, *namespace)
}
