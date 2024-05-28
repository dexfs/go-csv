package seed

type V1Contract struct {
	ContractID string `json:"contract_id"`
	Total      int    `json:"total"`
}

type V2Contract struct {
	ID         string `json:"id"`
	ExternalID string `json:"external_id"`
	Status     string `json:"status"`
}

type V2Titulos struct {
	ContractID string `json:"contract_id"`
	Total      int    `json:"total"`
}
