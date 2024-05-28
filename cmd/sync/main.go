package main

import (
	"bufio"
	"encoding/csv"
	"github.com/schollz/progressbar/v3"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	FileTitulosV1   = "dataset/v1_titulos.csv"
	FileContractsV2 = "dataset/v2_contracts.csv"
	FileTitulosV2   = "dataset/v2_titulos.csv"
	FileOutput      = "output.csv"
)

var progressBar *progressbar.ProgressBar

/*
*
Mapa files
contract idx0 v2, idx1 v1
titulos idx0 v2, idx2 total
*/
func main() {
	lineCount := 0
	found := 0
	generated := 0

	time.Sleep(1 * time.Second)
	start := time.Now()
	// SCANNERS
	slog.Info("Starting combine data")

	scannerContractsV2, fileCV2 := ReadFileByLine(FileContractsV2)

	var combinedContracts [][]string

	titulosV1, err := ReadAllFile(FileTitulosV1)
	if err != nil {
		log.Fatal(err)
	}
	progressBar = progressbar.Default(-1, "Reading Contracts...")
	for scannerContractsV2.Scan() {
		progressBar.Add(1)
		lineCount++
		line := scannerContractsV2.Text()
		rowV2 := strings.Split(line, ",")
		//if found == 50000 {
		//	break
		//}

		if rowV2[2] == "CANCELED" {
			continue
		}

		for _, rowV1 := range titulosV1 {

			// uuid | uuid
			if rowV2[1] == rowV1[0] {
				found++
				tempCombined := []string{rowV1[0], rowV2[0], rowV1[1]} // uuid, cuid, number
				combinedContracts = append(combinedContracts, tempCombined)
			}
		}
	}

	fileCV2.Close()

	var toAnalysis [][]string

	if len(combinedContracts) <= 0 {
		log.Fatal("No contracts found")
	}

	slog.Info("Starting generate data to save")
	progressBar.Reset()
	progressBar.Describe("Processing Combination...")
	progressBar.Clear()
	allTitulosV2, err := ReadAllFile(FileTitulosV2)
	if err != nil {
		log.Fatal(err)
	}

	for i := range combinedContracts {
		progressBar.Add(1)
		rowCombined := combinedContracts[i] // uuid, cuid, total

		for _, current := range allTitulosV2 {
			// current [internal_id, total]
			isSameId := current[0] == rowCombined[1] // cuid
			if !isSameId {
				continue
			}

			isDiffTotal := current[1] != rowCombined[2] // total is diff
			if !isDiffTotal {
				continue
			}

			generated++
			// external_id, total, internal_id, total
			toAnalysis = append(toAnalysis, []string{rowCombined[0], rowCombined[2], current[0], current[1]})
		}
	}

	/// WRITE FILE
	progressBar.Reset()
	progressBar.Describe("Writing file...")
	progressBar.Clear()
	progressBar.ChangeMax(len(toAnalysis))
	err = WriteFile(FileOutput, toAnalysis)
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("Total records: ", lineCount)
	slog.Info("Total records generated: ", generated)
	slog.Info("Process finished!", time.Since(start))
}

func ReadFileByLine(filename string) (*bufio.Scanner, *os.File) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	// skip first line
	scanner.Scan()

	return scanner, file
}

func ReadAllFile(filename string) ([][]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	reader := csv.NewReader(file)

	return reader.ReadAll()
}

func WriteFile(filename string, records [][]string) error {
	fileHeader := []string{"external_id", "total", "contract_id", "total"}
	fileWrite, err := os.Create(filename)

	if err != nil {
		return err
	}
	defer fileWrite.Close()

	writer := csv.NewWriter(fileWrite)
	defer writer.Flush()
	writer.Write(fileHeader)
	for _, record := range records {
		progressBar.Add(1)
		writer.Write(record)
	}

	return nil
}
