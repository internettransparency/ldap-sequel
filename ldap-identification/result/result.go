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
	Id        int
	Ip        string
	Port      int
	Matches   []map[string]string
	Name      string
	Certainty float64
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
				name = getServerName(match)
				certainty = currentCertainty
			}

		}
		allLinesResults[i].Name = name
		allLinesResults[i].Certainty = certainty
	}
	store(allLinesResults, fileName)
}

func getServerName(match map[string]string) string {
	// build the server name similarly to the description field of recog/xml/ldap_searchresult.xml
	// service.vendor service.product on os.product
	// service.product on os.product
	// os.product
	name := ""
	osProduct := match["os.product"]
	vendor := match["service.vendor"]
	product, ok := match["service.product"]
	if ok {
		if strings.Contains(product, vendor) {
			name = product
		} else {
			name = vendor + " " + product
		}
		if osProduct != "" {
			name += " on " + osProduct
		}

	} else {
		name = osProduct
	}

	return name
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
	header := []string{"id", "ip", "port", "server_name", "certainty"}
	if err := writer.Write(header); err != nil {
		log.Fatal().Err(err).Msg("Failed to write header to file")
	}

	// Write the records
	for _, record := range results {
		row := []string{
			strconv.Itoa(record.Id),
			record.Ip,
			strconv.Itoa(record.Port),
			record.Name,
			strconv.FormatFloat(record.Certainty, 'f', 1, 64),
		}
		if err := writer.Write(row); err != nil {
			log.Fatal().Err(err).Msg("Failed to write record to file")
		}
	}
}
