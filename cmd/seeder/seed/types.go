package seed

type DataSchema01 struct {
	ContractID string `json:"contract_id"`
	Total      int    `json:"total"`
}

type DataSchema02 struct {
	ID         string `json:"id"`
	ExternalID string `json:"external_id"`
	Status     string `json:"status"`
}

type DataSchema03 struct {
	ContractID string `json:"contract_id"`
	Total      int    `json:"total"`
}
