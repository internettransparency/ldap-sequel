package main

import (
	"encoding/csv"
	"errors"
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

	log.Info().Msg("Starting LDAP server identification...")
	resultChan, nrResults, err := matchResponse(inputCsv)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to match responses")
	}
	result.ConsumeResult(*resultChan, nrResults, output)
	log.Info().Msg("Finished")
}

func matchResponse(inputCsv string) (*chan result.Result, int, error) {
	fps, err := recog.LoadFingerprintsDir("recog/xml")
	if err != nil {
		return nil, 0, err
	}
	log.Info().Msg("Fingerprints loaded")

	file, err := os.Open(inputCsv)
	if err != nil {
		return nil, 0, err
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, 0, err
	}

	nrRecords := len(records) - 1
	resultChan := make(chan result.Result, nrRecords)
	for i, record := range records[1:] { // skip header
		input := match.Input{}
		input.Id, err = strconv.Atoi(record[0])
		if err != nil {
			return nil, i, errors.New(err.Error() + " id: " + record[0])
		}
		input.Ip = record[1]
		input.Port, err = strconv.Atoi(record[2])
		if err != nil {
			return nil, i, errors.New(err.Error() + " port: " + record[2])
		}
		input.RawResponseB64 = record[3]
		go match.Match(fps, input, resultChan)
	}

	return &resultChan, nrRecords, nil
}
