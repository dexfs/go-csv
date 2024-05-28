package main

import (
	"github.com/dexfs/golang-csv/cmd/async/helpers"
	"github.com/schollz/progressbar/v3"
	"log"
	"log/slog"
	"time"
)

const (
	File01 = "dataset/file_01.csv"
	File02 = "dataset/file_02.csv"
	File03 = "dataset/file_03.csv"
)
const ContractLogTitle = "ContractsV2"

var lineCount = 0
var found = 0
var generated = 0
var outputFileHeader = []string{"external_id", "total", "contract_id", "total"}
var countCalls = 0
var progressBar *progressbar.ProgressBar

func main() {
	start := time.Now()
	// input
	data01, _ := helpers.ReadAllFile(File01)
	progressBar = progressbar.Default(int64(len(data01)), "Processing!")
	// stage 1 -> Envia scanner para método
	allDataFromFile01 := readInfoOfFile01(data01)
	// stage 2 -> Ler contratos v1 e combinar com os dados do contrato v2
	combined := readInfoFile01AndCombine(allDataFromFile01)

	// stage 3 -> from combined to transform
	transformed := fromCombinedToCsv(combined)
	csvWriter, err := helpers.NewCsvWriter("output.csv")
	if err != nil {
		log.Fatal(err)
	}

	csvWriter.AppendHeader(outputFileHeader)
	for c := range transformed {
		countCalls++
		err := csvWriter.Append(c)
		if err != nil {
			slog.Error("Error writing to csv:", err)
		}
		progressBar.Add(1)
	}
	csvWriter.End()
	progressBar.Close()
	slog.Info("Process finished!", time.Since(start))
	slog.Info("Total found: ", found)
	slog.Info("Total records generated: ", generated)
}

func readInfoOfFile01(data [][]string) <-chan []string {
	out := make(chan []string)
	go func() {
		defer close(out)
		for _, row := range data {
			progressBar.Add(1)
			lineCount++
			if row[2] == "CANCELED" {
				continue
			}
			out <- row
		}
	}()
	return out
}

func readInfoFile01AndCombine(in <-chan []string) <-chan []string {
	out := make(chan []string)
	go func() {
		defer close(out)
		fileDataV1, _ := helpers.ReadAllFile(File02)
		for row := range in {
			for _, rowV1 := range fileDataV1 {
				countCalls++
				if row[1] == rowV1[0] {
					progressBar.Add(1)
					found++
					out <- []string{rowV1[0], row[0], rowV1[1]}
					break
				}
			}
		}
	}()
	return out
}

func fromCombinedToCsv(in <-chan []string) <-chan []string {
	out := make(chan []string)
	go func() {
		defer close(out)
		fileDataV2, _ := helpers.ReadAllFile(File03)

		for combined := range in {
			for _, rowV2 := range fileDataV2 {
				countCalls++
				isSameID := rowV2[0] == combined[1]
				if !isSameID {
					continue
				}
				isDiff := rowV2[1] != combined[2]
				if !isDiff {
					continue
				}
				progressBar.Add(1)
				generated++
				out <- []string{combined[0], combined[2], rowV2[0], rowV2[1]}
			}
		}

	}()
	return out
}
