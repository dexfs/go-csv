package seed

import (
	"github.com/gofrs/uuid"
	"github.com/nrednav/cuid2"
	"math/rand"
)

func NewSchema01() *DataSchema01 {
	id := uuid.Must(uuid.NewV4())
	return &DataSchema01{
		ContractID: id.String(),
		Total:      rand.Intn(50),
	}
}

func NewSchema02(externalID string) *DataSchema02 {
	id := cuid2.Generate()
	return &DataSchema02{
		ID:         id,
		ExternalID: externalID,
		Status:     RandomState(),
	}
}

func NewSchema03(contractID string) *DataSchema03 {
	return &DataSchema03{
		ContractID: contractID,
		Total:      rand.Intn(50),
	}
}

func RandomState() string {
	randomNumber := rand.Intn(100)
	if randomNumber < 40 {
		return "CANCELED"
	}
	return "ACTIVE"
}
