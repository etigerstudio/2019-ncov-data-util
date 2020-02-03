package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"
	"time"
)

type DataRecord struct {
	ProvinceName string
	ConfirmedCount int
	DeadCount int
	Comment string
	UpdateTime int64
}

type DataPayload struct {
	Results []DataRecord
	Success bool
}

func TransformJSONToCSV(filename string, outputDir string) {
	dataset := ParseIsaaclinJSONFile(filename)
	GenerateGlobalcitizenCSVFile(dataset, outputDir)
}

func ParseIsaaclinJSONFile(filename string) []Data {
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatalln("cannot read json:", err)
	}

	var payload DataPayload
	err = json.Unmarshal(bytes, &payload)
	if err != nil {
		log.Fatalln("cannot unmarshal json:", err)
	}
	if !payload.Success {
		log.Fatalln("invalid payload")
	}

	dataset := []Data{}
	var prevTimestamp string
	var prevProvince string
	n := -1
	for i := len(payload.Results) - 1; i >= 0; i-- {
		timestamp := timestampFromUnixTime(payload.Results[i].UpdateTime)
		province := payload.Results[i].ProvinceName

		pushDataItem(timestamp, province, &prevTimestamp, &prevProvince, &n, &dataset,
			strconv.Itoa(payload.Results[i].ConfirmedCount),
			strconv.Itoa(payload.Results[i].DeadCount), payload.Results[i].Comment)
	}

	printDatasetDigest(dataset)
	return dataset
}

func timestampFromUnixTime(unix int64) string {
	return time.Unix(unix / 1000, unix % 1000 * 1000).Format("2006-01-02 15:04:05.000")
}