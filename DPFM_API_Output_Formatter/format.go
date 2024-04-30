package dpfm_api_output_formatter

import (
	"data-platform-api-event-type-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToEventType(rows *sql.Rows) (*[]EventType, error) {
	defer rows.Close()
	eventType := make([]EventType, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.EventType{}

		err := rows.Scan(
			&pm.EventType,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &eventType, nil
		}

		data := pm
		eventType = append(eventType, EventType{
			EventType:				data.EventType,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &eventType, nil
}

func ConvertToText(rows *sql.Rows) (*[]Text, error) {
	defer rows.Close()
	text := make([]Text, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Text{}

		err := rows.Scan(
			&pm.EventType,
			&pm.Language,
			&pm.EventTypeName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &text, err
		}

		data := pm
		text = append(text, Text{
			EventType:     			data.EventType,
			Language:          		data.Language,
			EventTypeName:			data.EventTypeName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &text, nil
}
