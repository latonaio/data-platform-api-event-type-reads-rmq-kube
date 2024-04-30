package requests

type Text struct {
	EventType     		string  `json:"EventType"`
	Language          	string  `json:"Language"`
	EventTypeName		string  `json:"EventTypeName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
