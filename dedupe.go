package main

import (
	"io/ioutil"
	"log"
	"os"
	p "path"
	"path/filepath"
	"strings"
	"time"
)

func PerformDedupe(path string) {
	prevFile, curFile := getLastTwoCSVs(path)
	prevData := parseGlobalcitizenCSV(prevFile)
	curData := parseGlobalcitizenCSV(curFile)

	if compareData(prevData, curData) {
		purgeOneFetch(curFile)

		log.Println("dedupe: stale fetch: latest data were purged")
		return
	}

	log.Println("dedupe: fresh fetch: latest data were retained")
}

func purgeOneFetch(csvName string) {
	prefix := strings.TrimSuffix(csvName, ".csv")
	rmFilesIfExists([]string{
		prefix + ".csv",
		prefix + ".svg",
		prefix + ".json",
	})
}

func rmFilesIfExists(filenames []string) {
	for _, f := range filenames {
		info, err := os.Stat(f)
		if err == nil && !info.IsDir() {
			err := os.Remove(f)
			if err != nil {
				log.Println("warning: an error occurred when removing", f + " :", err)
			}
		}
	}
}

func parseGlobalcitizenCSV(filename string) *Data {
	records := readCSV(filename, '|', '#')

	data := Data{Provinces: map[string]DataItem{}}
	for _, record := range records {
		data.Provinces[getProvinceG(record)] = DataItem{
			Confirmed: getConfirmedG(record),
			Dead:      getDeadG(record),
		}
	}

	return &data
}

func readLastTwoCSVs(path string) (prev string, cur string) {
	prevFile, curFile := getLastTwoCSVs(path)

	prevBytes, err := ioutil.ReadFile(prevFile)
	if err != nil {
		log.Fatalln("cannot read prev csv:", err)
	}

	curBytes, err := ioutil.ReadFile(curFile)
	if err != nil {
		log.Fatalln("cannot read cur csv:", err)
	}

	return string(prevBytes), string(curBytes)
}

func getLastTwoCSVs(path string) (prev string, cur string) {
	var prevTime, curTime time.Time
	var prevFile, curFile string

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || p.Ext(path) != ".csv" {
			return nil
		}

		t := info.ModTime()
		if t.After(prevTime) {
			if t.After(curTime) {
				prevTime, curTime = curTime, t
				prevFile, curFile = curFile, path
			} else {
				prevTime = t
				prevFile = path
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalln("cannot get last two csv files:", err)
	}

	log.Println("prev:", prevTime, prevFile)
	log.Println("cur:", curTime, curFile)
	return prevFile, curFile
}

// Extracting from globalcitizen

func getProvinceG(record []string) string {
	return record[0]
}

func getConfirmedG(record []string) string {
	return record[1]
}

func getDeadG(record []string) string {
	return record[2]
}