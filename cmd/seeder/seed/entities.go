package seed

import (
	"github.com/gofrs/uuid"
	"github.com/nrednav/cuid2"
	"math/rand"
)

func NewV1Contract() *V1Contract {
	id := uuid.Must(uuid.NewV4())
	return &V1Contract{
		ContractID: id.String(),
		Total:      rand.Intn(50),
	}
}

func NewV2Contract(externalID string) *V2Contract {
	id := cuid2.Generate()
	return &V2Contract{
		ID:         id,
		ExternalID: externalID,
		Status:     RandomState(),
	}
}

func NewV2Titulos(contractID string) *V2Titulos {
	return &V2Titulos{
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
