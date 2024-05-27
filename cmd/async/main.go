package main

import (
	"bufio"
	"github.com/dexfs/golang-csv/cmd/async/helpers"
	"github.com/schollz/progressbar/v3"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	FileContractsV2 = "dataset/v2_contracts.csv"
	FileTitulosV1   = "dataset/v1_titulos.csv"
	FileTitulosV2   = "dataset/v2_titulos.csv"
)
const ContractLogTitle = "ContractsV2"

var lineCount = 0
var found = 0
var generated = 0
var outputFileHeader = []string{"external_id", "total", "contract_id", "total"}
var totalDebtsV2 = 0
var totalDebtsV1 = 0
var countCalls = 0
var progressBar *progressbar.ProgressBar

func main() {
	start := time.Now()
	// input
	progressBar = progressbar.Default(-1, "Processing!")
	contractsV2, fileV2 := helpers.ReadFileByLine(FileContractsV2)
	// stage 1 -> Envia scanner para método
	activeContractsV2 := readInfoContractV2(contractsV2, fileV2)
	// stage 2 -> Ler contratos v1 e combinar com os dados do contrato v2
	combined := readInfoContractV1AndCombine(activeContractsV2)

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
	}
	csvWriter.End()
	slog.Info("Process finished!", time.Since(start))
	//slog.Info("Total records: ", lineCount)
	//slog.Info("Total Debts V1: ", totalDebtsV1)
	//slog.Info("Total Debts V2: ", totalDebtsV2)
	//slog.Info("Calls: ", countCalls)
	slog.Info("Total found: ", found)
	slog.Info("Total records generated: ", generated)
}

func readInfoContractV2(data *bufio.Scanner, file *os.File) <-chan []string {
	out := make(chan []string)
	go func() {
		for data.Scan() {
			progressBar.Add(1)
			lineCount++
			line := data.Text()
			rowV2 := strings.Split(line, ",")
			if rowV2[2] == "CANCELED" {
				continue
			}
			out <- rowV2
		}
		close(out)
		file.Close()
	}()
	return out
}

func readInfoContractV1AndCombine(in <-chan []string) <-chan []string {
	out := make(chan []string)
	go func() {
		dataV1, file := helpers.ReadFileByLine(FileTitulosV1)
		defer file.Close()
		var fileDataV1 [][]string
		for dataV1.Scan() {
			line := dataV1.Text()
			rowDataV1 := strings.Split(line, ",")
			fileDataV1 = append(fileDataV1, rowDataV1)
		}
		totalDebtsV1 = len(fileDataV1)
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
		close(out)
	}()
	return out
}

func fromCombinedToCsv(in <-chan []string) <-chan []string {
	out := make(chan []string)
	go func() {
		dataV2, file := helpers.ReadFileByLine(FileTitulosV2)
		defer file.Close()
		var fileDataV2 [][]string
		for dataV2.Scan() {
			line := dataV2.Text()
			rowDataV2 := strings.Split(line, ",")
			fileDataV2 = append(fileDataV2, rowDataV2)
		}
		totalDebtsV2 = len(fileDataV2)
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
				//helpers.WriteFile("teste", [][]string{})
				out <- []string{combined[0], combined[2], rowV2[0], rowV2[1]}
			}
		}
		close(out)
	}()
	return out
}
