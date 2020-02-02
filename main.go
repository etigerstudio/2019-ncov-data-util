package main

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"time"
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
	dataset := ParseIsaaclinCSVFile("DXYArea.csv")
	GenerateGlobalcitizenCSVFile(dataset, "dxy")
}

func GenerateGlobalcitizenCSVFile(dataset []Data, directory string) {
	const prefix = `# source: DXY @ https://3g.dxy.cn/newh5/view/pneumonia
# update: `
	const suffix = ` CST
# place|confirmed_cases|deaths|notes|sources
`
	provinceNameMap := map[string]string{
		"青海省": "Qinghai",
		"福建省": "Fujian",
		"辽宁省": "Liaoning",
		"四川省": "Sichuan",
		"江苏省": "Jiangsu",
		"江西省": "Jiangxi",
		"山东省": "Shandong",
		"广西壮族自治区": "Guangxi",
		"宁夏回族自治区": "Ningxia",
		"澳门": "Macau",
		"河南省": "Henan",
		"上海市": "Shanghai",
		"北京市": "Beijing",
		"安徽省": "Anhui",
		//"台湾": "Taiwan",
		"新疆维吾尔自治区": "Xinjiang",
		"湖南省": "Hunan",
		"吉林省": "Jilin",
		"甘肃省": "Gansu",
		"贵州省": "Guizhou",
		"重庆市": "Chongqing",
		"云南省": "Yunnan",
		"陕西省": "Shaanxi",
		"香港": "Hong Kong",
		"浙江省": "Zhejiang",
		"山西省": "Shanxi",
		"湖北省": "Hubei",
		"广东省": "Guangdong",
		"天津市": "Tianjin",
		"海南省": "Hainan",
		"河北省": "Hebei",
		"内蒙古自治区": "Inner Mongolia",
		"黑龙江省": "Heilongjiang",
		"西藏自治区": "Tibet",
	}

	for _, data := range dataset {
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", data.timestamp)
		if err != nil {
			log.Fatalln("cannot parse timestamp:", err)
		}

		file, err := os.Create(path.Join(directory, getFilename(timestamp)))
		if err != nil {
			log.Fatalln("cannot create file:", err)
		}

		_, err = file.WriteString(prefix + getUpdateTimestamp(timestamp) + suffix)
		if err != nil {
			log.Fatalln("cannot write string:", err)
		}

		writer := csv.NewWriter(file)
		writer.Comma = '|'
		for k, v := range data.provinces {
			err := writer.Write(extractRecord(k, &v, provinceNameMap))
			if err != nil {
				log.Fatalln("cannot write csv record:", err)
			}
		}

		writer.Flush()
		err = writer.Error()
		if err != nil {
			log.Fatalln("cannot flush csv records:", err)
		}
	}

	log.Println("converting finished:", len(dataset), "csv files generated")
}

func extractRecord(provinceName string, dataItem *DataItem, provinceNameMap map[string]string) []string {
	//if provinceNameMap[provinceName] == "" {
	//	log.Println("province name mismatch:", provinceName)
	//}
	return []string{
		provinceNameMap[provinceName],
		dataItem.Confirmed,
		dataItem.Dead,
		"",
		"",
	}
}

func getFilename(timestamp time.Time) string {
	return timestamp.Format("20060102-150405") + "-dxy-2019ncov-data.csv"
}

func getUpdateTimestamp(timestamp time.Time) string {
	return timestamp.Format("2006-01-02 15:04:05")
}

func ParseIsaaclinCSVFile(filename string) []Data {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("cannot open csv:", err)
	}

	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalln("cannot read records:", err)
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

	log.Println("parsing finished:", len(dataset), "individual data generated")
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