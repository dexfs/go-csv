package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
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
	fmt.Println("Starting combine data")
	scannerContractsV2, fileCV2 := ReadFileByLine(FileContractsV2)

	var combinedContracts [][]string

	for scannerContractsV2.Scan() {
		lineCount++
		line := scannerContractsV2.Text()
		rowV2 := strings.Split(line, ",")
		//if found == 50000 {
		//	break
		//}

		if rowV2[2] == "CANCELED" {
			continue
		}

		scannerTitulosV1, fileCV1 := ReadFileByLine(FileTitulosV1)
		for scannerTitulosV1.Scan() {
			lineV1 := scannerTitulosV1.Text()
			rowV1 := strings.Split(lineV1, ",")
			// uuid | uuid
			if rowV2[1] == rowV1[0] {
				found++
				tempCombined := []string{rowV1[0], rowV2[0], rowV1[1]} // uuid, cuid, number
				combinedContracts = append(combinedContracts, tempCombined)
			}
		}
		fileCV1.Close()
	}
	fileCV2.Close()

	var toAnalysis [][]string

	if len(combinedContracts) <= 0 {
		log.Fatal("No contracts found")
	}

	fmt.Println("Starting generate data to save")

	for i := range combinedContracts {
		rowCombined := combinedContracts[i] // uuid, cuid, total
		scannerTitulosV2, fileTituloV2 := ReadFileByLine(FileTitulosV2)
		for scannerTitulosV2.Scan() {
			// current [internal_id, total]
			line := scannerTitulosV2.Text()
			current := strings.Split(line, ",")      // cuid, number
			isSameId := current[0] == rowCombined[1] // cuid
			if !isSameId {
				continue
			}

			isDiffTotal := current[1] != rowCombined[2] // total is diff

			if isSameId && isDiffTotal {
				generated++
				// external_id, total, internal_id, total
				toAnalysis = append(toAnalysis, []string{rowCombined[0], rowCombined[2], current[0], current[1]})
			}
		}
		fileTituloV2.Close()
	}

	/// WRITE FILE
	err := WriteFile(FileOutput, toAnalysis)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Total records: ", lineCount)
	fmt.Println("Total records generated: ", generated)
	fmt.Println("Process finished!", time.Since(start))
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
		writer.Write(record)
	}

	return nil
}
