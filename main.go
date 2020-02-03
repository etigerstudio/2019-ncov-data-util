package main

import (
	"encoding/csv"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"time"
)

type Data struct {
	Timestamp string
	Provinces map[string]DataItem
}

type DataItem struct{
	Confirmed string
	Dead string
}

func main()  {
	//TransformCSVToCSV("data/DXYArea.csv", "dxy")
	TransformJSONToCSV("data/DXYArea.json", "dxy")
}

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
			strconv.Itoa(payload.Results[i].DeadCount))
	}

	printDatasetDigest(dataset)
	return dataset
}

func pushDataItem(timestamp string, province string, prevTimestamp *string,
	prevProvince *string, n *int, dataset *[]Data, confirmed string, dead string) {
	if timestamp != *prevTimestamp {
		*prevTimestamp = timestamp
		*prevProvince = ""
		if *n == -1 {
			data := newDataVanilla(timestamp)
			*dataset = append(*dataset, data)
			*n = 0
		} else {
			// merge data if necessary
			if *n < 1 || !compareData(&(*dataset)[*n - 1], &(*dataset)[*n]) {
				data := newDataFromPrev(timestamp, &(*dataset)[*n])
				*dataset = append(*dataset, data)
				*n++
			} else {
				(*dataset)[*n].Timestamp = timestamp
			}
		}
	}
	if province != *prevProvince {
		*prevProvince = province
		(*dataset)[*n].Provinces[province] = DataItem{
			Confirmed: confirmed,
			Dead:      dead,
		}
	}
}

func timestampFromUnixTime(unix int64) string {
	return time.Unix(unix / 1000, unix % 1000 * 1000).Format("2006-01-02 15:04:05.000")
}

func TransformCSVToCSV(filename string, outputDir string) {
	dataset := ParseIsaaclinCSVFile(filename)
	GenerateGlobalcitizenCSVFile(dataset, outputDir)
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
		"台湾": "Taiwan",
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
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", data.Timestamp)
		if err != nil {
			log.Fatalln("cannot parse timestamp:", err)
		}

		file, err := os.Create(path.Join(directory, getFilename(timestamp)))
		if err != nil {
			log.Fatalln("cannot create file:", err)
		}

		_, err = file.WriteString(prefix + getOutputTimestamp(timestamp) + suffix)
		if err != nil {
			log.Fatalln("cannot write string:", err)
		}

		writer := csv.NewWriter(file)
		writer.Comma = '|'
		for k, v := range data.Provinces {
			province, ok := provinceNameMap[k]
			if !ok {
				//log.Println("province name key not found:", provinceName)
				continue
			}
			err := writer.Write(extractRecord(province, &v))
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

func extractRecord(provinceName string, dataItem *DataItem) []string {
	return []string{
		provinceName,
		dataItem.Confirmed,
		dataItem.Dead,
		"",
		"",
	}
}

func getFilename(timestamp time.Time) string {
	return timestamp.Format("20060102-150405") + "-dxy-2019ncov-data.csv"
}

func getOutputTimestamp(timestamp time.Time) string {
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

		pushDataItem(timestamp, province, &prevTimestamp, &prevProvince,
			&n, &dataset, getConfirmed(record), getDead(record))
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

func compareData(prev *Data, next *Data) (equal bool) {
	for k, v1 := range next.Provinces {
		v2, ok := prev.Provinces[k]
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