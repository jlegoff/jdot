package nrinfrareceiver

type RawEvent map[string]interface{}

type Event struct {
	EventType  string
	Timestamp  int
	EntityKey  string
	Attributes map[string]interface{}
	Metrics    map[string]float64
}

type Entities struct {
	EntityID int64
	IsAgent  bool
	Events   []Event
}

type RawEntities struct {
	EntityID  int64      `json:"EntityID"`
	IsAgent   bool       `json:"IsAgent"`
	RawEvents []RawEvent `json:"Events"`
}
