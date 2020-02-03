package main

import (
	"encoding/csv"
	"log"
	"os"
	"path"
	"time"
)

type Data struct {
	Timestamp string
	Provinces map[string]DataItem
}

type DataItem struct{
	Confirmed string
	Dead string
	Comment string
}

func readCSV(filename string, comma rune, comment rune) [][]string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("cannot open csv:", err)
	}

	r := csv.NewReader(file)
	r.Comma = comma
	r.Comment = comment
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalln("cannot read records:", err)
	}

	return records
}

func extractRecord(provinceName string, dataItem *DataItem) []string {
	return []string{
		provinceName,
		dataItem.Confirmed,
		dataItem.Dead,
		dataItem.Comment,
		"",
	}
}

func pushDataItem(timestamp string, province string,
	prevTimestamp *string, prevProvince *string, n *int,
	dataset *[]Data, confirmed string, dead string, comment string) {
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
			Comment:   comment,
		}
	}
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

func compareData(prev *Data, next *Data) (equal bool) {
	for k, v1 := range next.Provinces {
		v2, ok := prev.Provinces[k]
		if !ok {
			return false
		}
		// Ignoring comment data
		if v1.Confirmed != v2.Confirmed || v1.Dead != v2.Dead {
			return false
		}
	}

	return true
}