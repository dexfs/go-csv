package helpers

import (
	"encoding/csv"
	"os"
)

type FileWriter interface {
	AppendHeader(header []string)
	Append(body string) error
	End()
}

type FileCSVWriter struct {
	file   *os.File
	writer *csv.Writer
}

func NewCsvWriter(filename string) (*FileCSVWriter, error) {
	fileWrite, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	writer := csv.NewWriter(fileWrite)
	return &FileCSVWriter{
		file:   fileWrite,
		writer: writer,
	}, nil
}

func (w *FileCSVWriter) AppendHeader(header []string) {
	w.writer.Write(header)
}

func (w *FileCSVWriter) Append(body []string) error {
	return w.writer.Write(body)
}

func (w *FileCSVWriter) End() {
	w.writer.Flush()
	w.file.Close()
}
