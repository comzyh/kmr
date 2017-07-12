package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/util/log"
)

var (
	configFile  = flag.String("config", "", "MapReduce Job description JSON file, should be http(s) URL or a filepath")
	objectsFile = flag.String("objects", "", "inputFile")
	indent      = flag.String("indent", "", "indent string for print json")
)

func main() {
	flag.Parse()

	var err error
	var raw []byte
	var jobDescription job.JobDescription

	if *configFile == "" {
		log.Fatalf("configFile is required")
	}

	if strings.HasPrefix(*configFile, "http") { // Fetch config over http
		var resp *http.Response
		log.Infof("Fetching job config from %s", *configFile)
		resp, err = http.Get(*configFile)
		if err != nil {
			log.Fatalf("Can't fetch %s: %v", *configFile, err)
		}
		if resp.StatusCode != 200 {
			log.Fatalf("Can't fetch %s: HTTP %v", *configFile, resp.StatusCode)
		}
		defer resp.Body.Close()
		raw, err = ioutil.ReadAll(resp.Body)
	} else {
		raw, err = ioutil.ReadFile(*configFile)
	}
	if err != nil {
		log.Fatalf("Can't read description file '%s': %v", *configFile, err)
	}
	err = json.Unmarshal(raw, &jobDescription)
	if err != nil {
		log.Fatalf("Can't parse description file: %v", err)
	}

	var inputFile *os.File
	if *objectsFile == "" {
		inputFile = os.Stdin
	} else {
		inputFile, err = os.Open(*objectsFile)
		if err != nil {
			log.Fatalf("Can't open %s: %v", *objectsFile, err)
		}
	}
	reader := bufio.NewReader(inputFile)
	objects := make([]string, 0)
	var line string
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}
		if strings.TrimSpace(line) != "" {
			objects = append(objects, strings.TrimSpace(line))
		}
	}
	jobDescription.Map.Objects = objects

	var rebuiltJSON []byte
	if *indent == "" {
		rebuiltJSON, _ = json.Marshal(jobDescription)
	} else {
		rebuiltJSON, _ = json.MarshalIndent(jobDescription, "", *indent)
	}
	fmt.Println(string(rebuiltJSON))
}
