package helpers

import (
	"encoding/csv"
	"os"
	"sync"
)

type CSVAdapter struct {
	file   *os.File
	writer *csv.Writer
	reader *csv.Reader
	once   sync.Once
}

func NewCSVAdapter(filename string) (*CSVAdapter, error) {
	fileWrite, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	writer := csv.NewWriter(fileWrite)
	reader := csv.NewReader(fileWrite)
	return &CSVAdapter{
		file:   fileWrite,
		writer: writer,
		reader: reader,
	}, nil
}

func (w *CSVAdapter) AppendHeader(header []string) {
	w.once.Do(func() {
		w.writer.Write(header)
	})
}

func (w *CSVAdapter) Append(body []string) error {
	return w.writer.Write(body)
}

func (w *CSVAdapter) ReadAll() ([][]string, error) {
	return w.reader.ReadAll()
}

func (w *CSVAdapter) End() {
	w.writer.Flush()
	w.file.Close()
}
