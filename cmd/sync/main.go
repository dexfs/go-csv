package main

import (
	"bufio"
	"encoding/csv"
	"github.com/schollz/progressbar/v3"
	"log"
	"log/slog"
	"os"
	"time"
)

const (
	File01     = "dataset/file_01.csv"
	File02     = "dataset/file_02.csv"
	File03     = "dataset/file_03.csv"
	FileOutput = "output.csv"
)

var progressBar *progressbar.ProgressBar

func main() {
	lineCount := 0
	found := 0
	generated := 0

	time.Sleep(1 * time.Second)
	start := time.Now()

	slog.Info("Starting combine data")
	var err error
	allDataFile01, err := ReadAllFile(File01)

	if err != nil {
		log.Fatal(err)
	}

	allDataFile02, err := ReadAllFile(File02)
	if err != nil {
		log.Fatal(err)
	}

	var combinedData [][]string

	progressBar = progressbar.Default(int64(len(allDataFile02)), "Reading data of File 02...")
	for _, itemFile02 := range allDataFile02 {
		progressBar.Add(1)
		lineCount++

		if itemFile02[2] == "CANCELED" {
			continue
		}

		for _, itemFile01 := range allDataFile01 {
			if itemFile02[1] == itemFile01[0] {
				found++
				tempCombined := []string{itemFile01[0], itemFile02[0], itemFile01[1]}
				combinedData = append(combinedData, tempCombined)
			}
		}
	}

	var toAnalysis [][]string

	if len(combinedData) <= 0 {
		log.Fatal("No contracts found")
	}

	slog.Info("Starting generate data to save")
	progressBar.Reset()
	progressBar.Describe("Processing Combination...")
	progressBar.Clear()
	progressBar.ChangeMax(len(combinedData))

	allDataFile03, err := ReadAllFile(File03)
	if err != nil {
		log.Fatal(err)
	}

	for i := range combinedData {
		progressBar.Add(1)
		rowCombined := combinedData[i]

		for _, current := range allDataFile03 {
			isSameId := current[0] == rowCombined[1]
			if !isSameId {
				continue
			}

			isDiffTotal := current[1] != rowCombined[2]
			if !isDiffTotal {
				continue
			}

			generated++
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
