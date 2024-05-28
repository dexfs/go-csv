package main

import (
	"github.com/dexfs/golang-csv/cmd/seeder/seed"
	"github.com/dexfs/golang-csv/pkg/helpers"
	"github.com/schollz/progressbar/v3"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var pb *progressbar.ProgressBar
var wg sync.WaitGroup

func main() {
	start := time.Now()
	amount := 298_000
	pb = progressbar.Default(int64(amount), "Processing!")
	slog.Info("Starting seed for", amount, " items")

	var schema01 []*seed.DataSchema01
	for i := 0; i < amount; i++ {
		pb.Add(1)
		schema01 = append(schema01, seed.NewSchema01())
	}

	data01 := GenDataset01(schema01, &wg)
	data02 := GenDataset02(data01, &wg)
	data03 := GenDataset03(data02, &wg)

	go func() {
		wg.Wait()
	}()

	for {
		select {
		case _, ok := <-data03:
			if !ok {
				slog.Info("Process finished!", time.Since(start))
				return
			}
		}
	}
}

func GenDataset03(in <-chan *seed.DataSchema02, wg *sync.WaitGroup) <-chan *seed.DataSchema03 {
	wg.Add(1)
	out := make(chan *seed.DataSchema03, 6)
	go func() {
		csvAdapter, err := helpers.NewCSVAdapter("dataset/out_v2_titulos.csv")
		defer close(out)
		defer csvAdapter.End()

		if err != nil {
			panic(err)
		}
		for item := range in {
			schema := seed.NewSchema03(item.ID)
			csvAdapter.AppendHeader([]string{GetTag(*schema, 0), GetTag(*schema, 1)})
			fromStructToCSV := []string{schema.ContractID, strconv.Itoa(schema.Total)}
			csvAdapter.Append(fromStructToCSV)
			out <- schema
		}
	}()
	return out
}

func GenDataset02(in <-chan *seed.DataSchema01, wg *sync.WaitGroup) <-chan *seed.DataSchema02 {
	wg.Add(1)
	out := make(chan *seed.DataSchema02, 6)
	go func() {
		csvAdapter, err := helpers.NewCSVAdapter("dataset/out_v2_contracts.csv")
		defer close(out)
		defer csvAdapter.End()

		if err != nil {
			panic(err)
		}
		for item := range in {
			schema := seed.NewSchema02(item.ContractID)
			csvAdapter.AppendHeader([]string{GetTag(*schema, 0), GetTag(*schema, 1), GetTag(*schema, 2)})
			fromStructToCSV := []string{schema.ID, schema.ExternalID, schema.Status}
			csvAdapter.Append(fromStructToCSV)
			out <- schema
		}
		wg.Done()
	}()
	return out
}

func GenDataset01(items []*seed.DataSchema01, wg *sync.WaitGroup) <-chan *seed.DataSchema01 {
	wg.Add(1)
	out := make(chan *seed.DataSchema01, 7)
	pb.Reset()
	pb.Describe("Adicionando dados no arquivo...")
	go func() {
		defer close(out)
		csvAdapter, err := helpers.NewCSVAdapter("dataset/data_01.csv")
		if err != nil {
			panic(err)
		}
		for _, item := range items {
			csvAdapter.AppendHeader([]string{GetTag(*item, 0), GetTag(*item, 1)})
			fromStructToCSV := []string{item.ContractID, strconv.Itoa(item.Total)}
			csvAdapter.Append(fromStructToCSV)
			pb.Add(1)
			out <- item
		}
		csvAdapter.End()
		wg.Done()
	}()

	return out
}

func GetTag(s interface{}, key int) string {
	typeC := reflect.TypeOf(s)
	field := typeC.Field(key)
	return field.Tag.Get("json")
}
