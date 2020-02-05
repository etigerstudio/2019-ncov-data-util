package main

import (
	"log"
	"os"
	p "path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type RecordSet map[time.Time]string

// omittingComment: if omittingComment is set to true, 
// no comparison on 'comment' records will be made
func PerformDedupe(path string, usingModTime bool, omittingComment bool, maxCount int) {
	records := getRecordSet(path, usingModTime)
	times := []time.Time{}
	for t, _ := range records {
		times = append(times, t)
	}

	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	pendingPurge := []string{}
	var lastData *Data
	n := len(times)
	for i := n - 1; i > 0; i-- {
		var prevData, curData *Data

		// counting
		if maxCount > 1 && n - i > maxCount {
			break
		}

		// parsing
		if i == n - 1 {
			prevData = parseGlobalcitizenCSV(records[times[i - 1]])
			curData = parseGlobalcitizenCSV(records[times[i]])
		} else {
			prevData, curData = parseGlobalcitizenCSV(records[times[i - 1]]), lastData
		}

		// comparing
		if compareData(prevData, curData, omittingComment) {
			pendingPurge = append(pendingPurge, records[times[i]])
		}

		// passing
		lastData = prevData
	}

	for _, filename := range pendingPurge {
		purgeOneFetch(filename)
	}

	if len(pendingPurge) > 0 {
		logln("gdedupe:", len(pendingPurge), "data purged:")
		for _, filename := range pendingPurge {
			logln(" -", filename)
		}
	} else {
		logln("gdedupe: duplicated data not found")
	}
}

// usingModTime: use last time modified of files instead of time parsed from file names
func getRecordSet(path string, usingModTime bool) RecordSet {
	records := make(RecordSet)

	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || p.Ext(path) != ".csv" {
			return nil
		}

		var t time.Time
		if usingModTime {
			t = info.ModTime()
		} else {
			t = extractTimeFromFilename(info.Name())
		}

		records[t] = path
		return nil
	})
	if err != nil {
		log.Fatalln("cannot get all files with timestamp:", err)
	}

	return records
}

func extractTimeFromFilename(filename string) time.Time {
	prefix := filename[:15]
	timestamp, err := time.Parse("20060102-150405", prefix)
	if err != nil {
		log.Fatalln("cannot parse time from filename:", err)
	}

	return timestamp
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
			Comment:   getCommentG(record),
		}
	}

	return &data
}

//func readLastTwoCSVs(path string) (prev string, cur string) {
//	prevFile, curFile := getLastTwoCSVs(path)
//
//	prevBytes, err := ioutil.ReadFile(prevFile)
//	if err != nil {
//		log.Fatalln("cannot read prev csv:", err)
//	}
//
//	curBytes, err := ioutil.ReadFile(curFile)
//	if err != nil {
//		log.Fatalln("cannot read cur csv:", err)
//	}
//
//	return string(prevBytes), string(curBytes)
//}
//
//func getLastTwoCSVs(path string) (prev string, cur string) {
//	var prevTime, curTime time.Time
//	var prevFile, curFile string
//
//	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
//		if err != nil {
//			return err
//		}
//		if info.IsDir() || p.Ext(path) != ".csv" {
//			return nil
//		}
//
//		t := info.ModTime()
//		if t.After(prevTime) {
//			if t.After(curTime) {
//				prevTime, curTime = curTime, t
//				prevFile, curFile = curFile, path
//			} else {
//				prevTime = t
//				prevFile = path
//			}
//		}
//		return nil
//	})
//	if err != nil {
//		log.Fatalln("cannot get last two csv files:", err)
//	}
//
//	log.Println("prev:", prevTime, prevFile)
//	log.Println("cur:", curTime, curFile)
//	return prevFile, curFile
//}

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

func getCommentG(record []string) string {
	return record[3]
}