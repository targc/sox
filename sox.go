package sox

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ChangeEvent struct {
	OperationType string `bson:"operationType"`

	DocumentKey struct {
		ID bson.ObjectID `bson:"_id"`
	} `bson:"documentKey"`

	FullDocument map[string]interface{} `bson:"fullDocument"`

	UpdateDescription struct {
		UpdatedFields   map[string]interface{} `bson:"updatedFields"`
		RemovedFields   []string               `bson:"removedFields"`
		TruncatedArrays []interface{}          `bson:"truncatedArrays"`
	} `bson:"updateDescription"`
}

type SoxHandlers struct {
	Insert func(ctx context.Context, id string, doc map[string]interface{}) error
	Update func(ctx context.Context, id string, updatedFields map[string]interface{}, removedFields []string) error
	Delete func(ctx context.Context, id string) error
}

type Sox struct {
	stream   *mongo.ChangeStream
	handlers SoxHandlers
}

func InitSox(ctx context.Context, col *mongo.Collection) (*Sox, error) {

	stream, err := col.Watch(ctx, mongo.Pipeline{})

	if err != nil {
		return nil, err
	}

	s := Sox{
		stream: stream,
	}

	return &s, nil
}

func (s *Sox) SetHandlers(hls SoxHandlers) *Sox {
	s.handlers = hls
	return s
}

func (s *Sox) handle(ctx context.Context) error {
	raw := s.stream.Current

	var event ChangeEvent

	err := bson.Unmarshal(raw, &event)

	if err != nil {
		return err
	}

	switch event.OperationType {
	case "insert":
		return s.handlers.Insert(ctx, event.DocumentKey.ID.Hex(), event.FullDocument)
	case "update":
		return s.handlers.Update(ctx, event.DocumentKey.ID.Hex(), event.UpdateDescription.UpdatedFields, event.UpdateDescription.RemovedFields)
	case "delete":
		return s.handlers.Delete(ctx, event.DocumentKey.ID.Hex())
	}

	return nil
}

func (s *Sox) Stream(ctx context.Context) {
	for s.stream.Next(ctx) {
		appctx := context.Background()
		s.handle(appctx)
	}
}
