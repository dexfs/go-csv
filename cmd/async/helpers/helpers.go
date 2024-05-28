package helpers

import (
	"bufio"
	"encoding/csv"
	"log"
	"os"
)

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
