package main

import (
	"log"
	"time"
)

func TransformCSVToCSV(filename string, outputDir string) {
	dataset := ParseIsaaclinCSVFile(filename)
	GenerateGlobalcitizenCSVFile(dataset, outputDir)
}

func getFilename(timestamp time.Time) string {
	return timestamp.Format("20060102-150405") + "-dxy-2019ncov-data.csv"
}

func getOutputTimestamp(timestamp time.Time) string {
	return timestamp.Format("2006-01-02 15:04:05")
}

func ParseIsaaclinCSVFile(filename string) []Data {
	records := readCSV(filename, ',', 0)

	var prevTimestamp string
	var prevProvince string
	dataset := []Data{}
	n := -1
	for i := len(records) - 1; i > 0; i-- {
		record := records[i]

		timestamp := getTimestampI(record)
		province := getProvinceI(record)

		pushDataItem(timestamp, province, &prevTimestamp, &prevProvince,
			&n, &dataset, getConfirmedI(record), getDeadI(record), "")
	}

	printDatasetDigest(dataset)
	return dataset
}

func printDatasetDigest(dataset []Data) {
	log.Println("parsing finished:", len(dataset), "individual data generated")
	log.Println("first:", dataset[0].Timestamp)
	log.Println("last:", dataset[len(dataset) - 1].Timestamp)
}

func newDataVanilla(timestamp string) Data {
	return Data{
		Timestamp: timestamp,
		Provinces: make(map[string]DataItem),
	}
}

func newDataFromPrev(timestamp string, prevData *Data) Data {
	provinces := make(map[string]DataItem)
	for k, v := range prevData.Provinces {
		provinces[k] = v
	}

	return Data{
		Timestamp: timestamp,
		Provinces: provinces,
	}
}

// Extracting from Isaaclin

func getTimestampI(record []string) string {
	return record[10]
}

func getProvinceI(record []string) string {
	return record[0]
}

func getConfirmedI(record []string) string {
	return record[2]
}

func getDeadI(record []string) string {
	return record[5]
}
