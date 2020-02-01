package main

import (
	"encoding/csv"
	"log"
	"os"
)

type Data struct {
	timestamp string
	provinces map[string]DataItem
}

type DataItem struct{
	Confirmed string
	Dead string
}

func main()  {
	_ = ProcessCSVFile("DXYArea.csv")
}

func ProcessCSVFile(filename string) []Data {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("Cannot open csv:", err)
	}

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalln("Cannot read records:", err)
	}

	var prevTimestamp string
	var prevProvince string
	dataset := []Data{}
	n := -1
	for i := len(records) - 1; i > 0; i-- {
		record := records[i]

		timestamp := getTimestamp(record)
		province := getProvince(record)

		if timestamp != prevTimestamp {
			prevTimestamp = timestamp
			prevProvince = ""
			var data Data
			if n == -1 {
				data = newDataVanilla(timestamp)
				dataset = append(dataset, data)
				n = 0
			} else {
				// merge data if necessary
				if n < 1 || !compareData(&dataset[n - 1], &dataset[n]) {
					data = newDataFromPrev(timestamp, &dataset[n])
					dataset = append(dataset, data)
					n++
				} else {
					dataset[n].timestamp = timestamp
				}
			}
		}
		if province != prevProvince {
			prevProvince = province
			dataset[n].provinces[province] = DataItem{
				Confirmed: getConfirmed(record),
				Dead:      getDead(record),
			}
		}
	}

	log.Println("processing finished:", len(dataset), " individual data generated")
	log.Println("first:", dataset[0].timestamp)
	log.Println("last:", dataset[len(dataset) - 1].timestamp)

	return dataset
}

func newDataVanilla(timestamp string) Data {
	return Data{
		timestamp: timestamp,
		provinces: make(map[string]DataItem),
	}
}

func newDataFromPrev(timestamp string, prevData *Data) Data {
	provinces := make(map[string]DataItem)
	for k, v := range prevData.provinces {
		provinces[k] = v
	}

	return Data{
		timestamp: timestamp,
		provinces: provinces,
	}
}

func compareData(prev *Data, next *Data) (equal bool) {
	for k, v1 := range next.provinces {
		v2, ok := prev.provinces[k]
		if !ok {
			return false
		}
		if v1 != v2 {
			return false
		}
	}

	return true
}

func getTimestamp(record []string) string {
	return record[10]
}

func getProvince(record []string) string {
	return record[0]
}

func getConfirmed(record []string) string {
	return record[2]
}

func getDead(record []string) string {
	return record[5]
}