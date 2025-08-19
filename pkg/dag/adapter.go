package dag

import (
	"encoding/json"
	"fmt"
)

type AdapterType string

const (
	Adapter_Http      AdapterType = "http"
	Adapter_Schedular AdapterType = "schedular"
)

type Adapter[T HttpAdapter | SchedularAdapter | any] struct {
	Type     AdapterType            `json:"type"  bson:"type"`
	InputMap map[string]interface{} `json:"input" bson:"input"`
	Meta     T                      `bson:"meta" json:"-"`
	MetaRaw  json.RawMessage        `json:"meta" bson:"-"`
	GraphID  string                 `json:"graphId" bson:"graphId"`
	ID       string                 `json:"id" bson:"id"`
	graph    *Graph[*Action]
}

type HttpAdapter struct {
	Path        string     `json:"path"         bson:"path"`
	Method      HTTPMethod `json:"method"       bson:"method"`
	ResposeNode string     `json:"responseNode" bson:"responseNode"`

	BodySchema  map[string]interface{} `json:"bodySchema"  bson:"bodySchema"`
	QuerySchema map[string]interface{} `json:"querySchema" bson:"querySchema"`
}

type SchedularAdapter struct {
	Cron string `json:"cron" bson:"cron"`
}

func (a *Adapter[T]) UnmarshalJSON(b []byte) error {
	type AdapterAlias Adapter[T]
	var temp AdapterAlias
	if err := json.Unmarshal(b, &temp); err != nil {
		return err
	}
	a.Type = temp.Type
	a.InputMap = temp.InputMap
	a.GraphID = temp.GraphID
	a.MetaRaw = temp.MetaRaw

	switch a.Type {
	case Adapter_Http:
		adapter := HttpAdapter{}
		if err := json.Unmarshal(temp.MetaRaw, &adapter); err != nil {
			return err
		}
		a.Meta = any(adapter).(T)
	case Adapter_Schedular:
		adapter := SchedularAdapter{}
		if err := json.Unmarshal(temp.MetaRaw, &adapter); err != nil {
			return err
		}
		a.Meta = any(adapter).(T)
	default:
		return fmt.Errorf("unknown adapter type: %s", a.Type)
	}
	return nil
}
