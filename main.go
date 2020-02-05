package main

import (
	"flag"
	"fmt"
	"os"
)

const (
	usage = "usage: nutil <command> [<args>]"
	icsv2gcsvDesc = "\nicsv2gcsv Convert DXYArea CSV data from Isaac Lin to CSV records for globalcitizen"
	icsv2gcsvUsage = "nutil icsv2gcsv <-o <output-directory>> <input-csv>"
	ijson2gcsvDesc = "\nijson2gcsv Convert DXYArea JSON data from Isaac Lin to CSV records for globalcitizen"
	ijson2gcsvUsage = "nutil ijson2gcsv <-o <output-directory>> <input-json>"
	gdedupeDesc = "\ngdedupe Purge duplicate CSV records for globalcitizen"
	gdedupeUsage = "nutil gdedupe [-n <max-count>] [-omit-comment] <input-directory>"
)
// Commands
var (
	icsv2gcsv = flag.NewFlagSet("icsv2gcsv", flag.ExitOnError)
	ijson2gcsv = flag.NewFlagSet("ijson2gcsv", flag.ExitOnError)
	gdedupe = flag.NewFlagSet("gdedupe", flag.ExitOnError)
)

func main()  {
	icsvOutput := icsv2gcsv.String("o", "", "csv records output directory")
	ijsonOutput := ijson2gcsv.String("o", "", "csv records output directory")
	maxCount := gdedupe.Int("n", -1, "maximum csv records to read & process\ndefault: no record limit")
	omittingComment := gdedupe.Bool("omit-comment", false, "omit comment of records when comparing to determine duplication")

	if len(os.Args) < 2 {
		printCommandHelp()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "icsv2gcsv":
		_ = icsv2gcsv.Parse(os.Args[2:])
		if icsv2gcsv.NArg() == 0 {
			logln("No <input-csv> specified.")
			printArgumentHelp(icsv2gcsv, icsv2gcsvDesc, icsv2gcsvUsage)
			os.Exit(1)
		}
		if *icsvOutput == "" {
			logln("No <output-directory> specified.")
			printArgumentHelp(icsv2gcsv, icsv2gcsvDesc, icsv2gcsvUsage)
			os.Exit(1)
		}

		TransformCSVToCSV(icsv2gcsv.Arg(0), *icsvOutput)
	case "ijson2gcsv":
		_ = ijson2gcsv.Parse(os.Args[2:])
		if ijson2gcsv.NArg() == 0 {
			logln("No <input-json> specified.")
			printArgumentHelp(ijson2gcsv, ijson2gcsvDesc, ijson2gcsvUsage)
			os.Exit(1)
		}
		if *ijsonOutput == "" {
			logln("No <output-directory> specified.")
			printArgumentHelp(ijson2gcsv, ijson2gcsvDesc, ijson2gcsvUsage)
			os.Exit(1)
		}

		TransformJSONToCSV(ijson2gcsv.Arg(0), *ijsonOutput)
	case "gdedupe":
		_ = gdedupe.Parse(os.Args[2:])
		if gdedupe.NArg() == 0 {
			logln("No <input-directory> specified.")
			printArgumentHelp(gdedupe, gdedupeDesc, gdedupeUsage)
			os.Exit(1)
		}

		PerformDedupe(gdedupe.Arg(0), false, *omittingComment, *maxCount)
	default:
		printCommandHelp()
	}
	//TransformCSVToCSV("data/DXYArea.csv", "dxy")
	//TransformJSONToCSV("data/DXYArea.json", "dxy")
	//PerformDedupe("../2019-wuhan-coronavirus-data/data-sources/dxy/data", false, false, -1)
}

func printCommandHelp() {
	logln(usage)
	logln("'icsv2gcsv', 'ijson2gcsv' or 'gdedupe' command is required")
	printArgumentHelp(icsv2gcsv, icsv2gcsvDesc, icsv2gcsvUsage)
	printArgumentHelp(ijson2gcsv, ijson2gcsvDesc, ijson2gcsvUsage)
	printArgumentHelp(gdedupe, gdedupeDesc, gdedupeUsage)
}

func printArgumentHelp(f *flag.FlagSet, desc string, usage string) {
	logln(desc)
	logln(usage)
	f.PrintDefaults()
}

func logln(v ...interface{})  {
	_, _ = fmt.Fprintln(os.Stderr, v...)
}
