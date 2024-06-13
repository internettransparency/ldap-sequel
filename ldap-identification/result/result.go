package result

import (
	"encoding/csv"
	"github.com/rs/zerolog/log"
	"os"
	"strconv"
	"strings"
)

// Result channels the results of the match function
type Result struct {
	Id      int
	Ip      string
	Port    int
	Matches []map[string]string
	Name    string
}

// ConsumeResult consumes the results from the match function
func ConsumeResult(resultChan chan Result, nrResults int, fileName string) {
	allLinesResults := make([]Result, nrResults)
	for i := 0; i < nrResults; i++ {
		allLinesResults[i] = <-resultChan
	}

	for i, result := range allLinesResults {
		certainty := 0.0
		name := ""
		for _, match := range result.Matches {
			currentCertainty, err := strconv.ParseFloat(strings.TrimSpace(match["fp.certainty"]), 32)
			if err != nil {
				continue
			}
			// only the most certain match is considered
			if currentCertainty > certainty {
				if currentName, ok := match["service.product"]; ok {
					name = currentName
				} else {
					name = match["os.product"]
				}
			}

		}
		allLinesResults[i].Name = name
	}
	store(allLinesResults, fileName)
}

func store(results []Result, fileName string) {
	// store results in a CSV file
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal().Err(err).Str("file", fileName).Msg("Failed to create output file")
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the header
	header := []string{"id", "ip", "port", "server_name"}
	if err := writer.Write(header); err != nil {
		log.Fatal().Err(err).Msg("Failed to write header to file")
	}

	// Write the records
	for _, record := range results {
		row := []string{strconv.Itoa(record.Id), record.Ip, strconv.Itoa(record.Port), record.Name}
		if err := writer.Write(row); err != nil {
			log.Fatal().Err(err).Msg("Failed to write record to file")
		}
	}
}
