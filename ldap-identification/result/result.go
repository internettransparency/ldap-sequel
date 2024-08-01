package result

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// Result channels the results of the match function
type Result struct {
	Id        int
	Ip        string
	Port      int
	Matches   []map[string]string
	Name      string
	Certainty float64
	Version   string
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
		version := ""
		for _, match := range result.Matches {
			currentCertainty, err := strconv.ParseFloat(strings.TrimSpace(match["fp.certainty"]), 32)
			if err != nil {
				continue
			}
			// only the most certain match is considered
			if currentCertainty > certainty {
				name = getServerName(match)
				version = getServerVersion(match)
				certainty = currentCertainty
			}

		}
		allLinesResults[i].Name = name
		allLinesResults[i].Version = version
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

func getServerVersion(match map[string]string) string {
	// build the server version similarly to the version field of recog/xml/ldap_searchresult.xml
	// service.version
	// os.version
	version := ""
	if version, ok := match["service.version"]; ok {
		return version
	}
	if version, ok := match["os.version"]; ok {
		return version
	}
	return version
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
	header := []string{"id", "ip", "port", "server_name", "version", "certainty"}
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
			record.Version,
			strconv.FormatFloat(record.Certainty, 'f', 1, 64),
		}
		if err := writer.Write(row); err != nil {
			log.Fatal().Err(err).Msg("Failed to write record to file")
		}
	}
}
