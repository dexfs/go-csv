package helpers

type FileService interface {
	AppendHeader(header []string)
	Append(body string) error
	ReadAll() ([][]string, error)
	End()
}
