package main

import (
	"encoding/csv"
	"flag"
	"github.com/gustavoluvizotto/ldap-sequel/ldap-identification/result"
	"os"
	"strconv"

	"github.com/gustavoluvizotto/ldap-sequel/ldap-identification/match"

	"github.com/RumbleDiscovery/recog-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	var inputCsv string
	flag.StringVar(&inputCsv,
		"input-csv",
		"",
		"The input file in CSV format. Must contain the columns: id,ip,port,raw_response. id: int, ip: str, port: int, raw_response: str (base64).")

	var logFile string
	flag.StringVar(&logFile,
		"log-file",
		"",
		"The log file in JSON format")

	var output string
	flag.StringVar(&output,
		"output",
		"",
		"The output file in CSV format (provide extension)")

	// default value is no verbosity
	var verbosity int
	flag.IntVar(&verbosity,
		"v",
		0,
		"Verbosity level (1 or 2)")

	flag.Parse()

	log.Logger = log.Output(zerolog.NewConsoleWriter())

	if verbosity >= 2 {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else if verbosity == 1 {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	}
	if logFile != "" {
		fh, err := os.Create(logFile)
		if err != nil {
			log.Fatal().Err(err).Str("file", logFile).Msg("Error creating log file")
		}
		log.Logger = log.Output(fh)
	}

	if inputCsv == "" {
		log.Fatal().Msg("Input CSV file is required")
	}
	if output == "" {
		log.Fatal().Msg("Output file is required")
	}

	resultChan := matchResponse(inputCsv)
	result.ConsumeResult(*resultChan, len(*resultChan), output)
	log.Info().Msg("Finished")
}

func matchResponse(inputCsv string) *chan result.Result {
	fps, err := recog.LoadFingerprintsDir("recog/xml")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to load fingerprints")
	}
	file, err := os.Open(inputCsv)
	if err != nil {
		log.Fatal().Err(err).Str("file", inputCsv).Msg("Failed to open input file")
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read CSV file")
	}

	log.Info().Msg("Starting LDAP server identification...")
	resultChan := make(chan result.Result, len(records)-1)
	for _, record := range records[1:] {
		input := match.Input{}
		input.Id, err = strconv.Atoi(record[0])
		if err != nil {
			log.Fatal().Err(err).Str("id", record[0]).Msg("Failed to convert id to int")
		}
		input.Ip = record[1]
		input.Port, err = strconv.Atoi(record[2])
		if err != nil {
			log.Fatal().Err(err).Str("port", record[2]).Msg("Failed to convert port to int")
		}
		input.RawResponseB64 = record[3]
		match.Match(fps, input, resultChan)
	}

	return &resultChan
}
