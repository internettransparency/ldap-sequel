package match

import (
	"encoding/base64"
	"github.com/gustavoluvizotto/ldap-sequel/ldap-identification/result"

	"github.com/rs/zerolog/log"

	"github.com/RumbleDiscovery/recog-go"
)

// Input map of the input CSV file to be used in the match function
type Input struct {
	Id             int
	Ip             string
	Port           int
	RawResponseB64 string
}

// Match compares the raw response with the fingerprints
func Match(fps *recog.FingerprintSet, input Input, resultChan chan result.Result) {
	res := result.Result{
		Id:   input.Id,
		Ip:   input.Ip,
		Port: input.Port,
	}
	rawResponse, err := base64.RawStdEncoding.DecodeString(input.RawResponseB64)
	if err != nil {
		resultChan <- res
		log.Fatal().Int("id", input.Id).Err(err).Msg("Failed to decode base64")
	}

	matches := fps.MatchAll("ldap.search_result", string(rawResponse))
	matchedResults := make([]map[string]string, 0)
	for _, match := range matches {
		if match.Matched {
			matchedResults = append(matchedResults, match.Values)
		}
	}

	res.Matches = matchedResults
	resultChan <- res
}
