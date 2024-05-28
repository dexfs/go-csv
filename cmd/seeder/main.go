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
	amount := 100
	pb = progressbar.Default(int64(amount), "Processing!")
	slog.Info("Starting seed for", amount, " items")

	var v1Contracts []*seed.V1Contract
	for i := 0; i < amount; i++ {
		pb.Add(1)
		v1Contracts = append(v1Contracts, seed.NewV1Contract())
	}

	genContractsV1Channel := GenContractsV1DataChannel(v1Contracts, &wg)
	genContractsV2Channel := GenContractsV2DataChannel(genContractsV1Channel, &wg)
	genTitulosV2Channel := GenTitulosV2DataChannel(genContractsV2Channel, &wg)

	go func() {
		wg.Wait()
	}()

	for {
		select {
		case _, ok := <-genTitulosV2Channel:
			if !ok {
				slog.Info("Process finished!", time.Since(start))
				return
			}
		}
	}

	//typeC := reflect.TypeOf(contract)
	//field := typeC.Field(0)
	//fmt.Println(field.Tag.Get("json"))
}

func GenTitulosV2DataChannel(in <-chan *seed.V2Contract, wg *sync.WaitGroup) <-chan *seed.V2Titulos {
	wg.Add(1)
	out := make(chan *seed.V2Titulos)
	go func() {
		csvAdapter, err := helpers.NewCSVAdapter("dataset/out_v2_titulos.csv")
		defer close(out)
		defer csvAdapter.End()

		if err != nil {
			panic(err)
		}
		for cv2 := range in {
			t := seed.NewV2Titulos(cv2.ID)
			fromStructToCSV := []string{t.ContractID, strconv.Itoa(t.Total)}
			csvAdapter.Append(fromStructToCSV)
			out <- t
		}
	}()
	return out
}

func GenContractsV2DataChannel(in <-chan *seed.V1Contract, wg *sync.WaitGroup) <-chan *seed.V2Contract {
	wg.Add(1)
	out := make(chan *seed.V2Contract)
	go func() {
		csvAdapter, err := helpers.NewCSVAdapter("dataset/out_v2_contracts.csv")
		defer close(out)
		defer csvAdapter.End()

		if err != nil {
			panic(err)
		}
		for v := range in {
			c := seed.NewV2Contract(v.ContractID)
			fromStructToCSV := []string{c.ID, c.ExternalID, c.Status}
			csvAdapter.Append(fromStructToCSV)
			out <- c
		}
		wg.Done()
	}()
	return out
}

func GenContractsV1DataChannel(items []*seed.V1Contract, wg *sync.WaitGroup) <-chan *seed.V1Contract {
	wg.Add(1)
	out := make(chan *seed.V1Contract)
	pb.Reset()
	pb.Describe("Adicionando dados no arquivo...")
	go func() {
		defer close(out)
		csvAdapter, err := helpers.NewCSVAdapter("dataset/out_v1_contracts.csv")
		if err != nil {
			panic(err)
		}
		for _, contract := range items {
			fromStructToCSV := []string{contract.ContractID, strconv.Itoa(contract.Total)}
			csvAdapter.Append(fromStructToCSV)
			pb.Add(1)
			out <- contract
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
