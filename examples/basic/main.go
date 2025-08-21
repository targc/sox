package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/gookit/validate"
	"github.com/targc/sox"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {

	ctx := context.Background()

	type Env struct {
		MongoDBURI  string
		MongoDBName string
		PostgresURI string
	}

	env := Env{
		// TODO:
	}

	// PostgresQL connection

	pgDB, err := gorm.Open(postgres.Open(env.PostgresURI), &gorm.Config{})

	if err != nil {
		panic(err)
	}

	defer func() {
		sdb, err := pgDB.DB()

		if err != nil {
			return
		}

		sdb.Close()
	}()

	// MongoDB connection

	serverAPI := options.ServerAPI(options.ServerAPIVersion1)

	opts := options.
		Client().
		ApplyURI(env.MongoDBURI).
		SetServerAPIOptions(serverAPI)

	client, err := mongo.Connect(opts)

	if err != nil {
		panic(err)
	}

	err = client.Ping(ctx, readpref.Primary())

	if err != nil {
		panic(err)
	}

	defer client.Disconnect(ctx)

	mdb := client.Database(env.MongoDBName)
	col := mdb.Collection("points")

	slog.Info("listening mongodb change stream")

	type Rec struct {
		UserID    string    `json:"userId" validate:"required"`
		Point     int       `json:"point" validate:"required"`
		CreatedAt time.Time `json:"createdAt" validate:"required"`
	}

	sadapter := sox.NewSumAggSoxAdapter(sox.SumAggSoxAdapterOpt[int, Rec]{
		DB:        pgDB,
		DestTable: "daily_given_points",
		ValName:   "points",
		KeysTransformer: func(id string, rec Rec) sox.Keys {

			ok := validate.Struct(rec).Validate()

			if !ok {
				return nil
			}

			loc, err := time.LoadLocation("Asia/Bangkok")

			if err != nil {
				return nil
			}

			return map[string]interface{}{
				"date":    rec.CreatedAt.In(loc).Format(time.DateOnly),
				"user_id": rec.UserID,
			}
		},
		ValueTransformer: func(id string, rec Rec) int {
			return rec.Point
		},
	})

	s, err := sox.InitSox(ctx, col)

	if err != nil {
		panic(err)
	}

	s.
		SetHandlers(sadapter.Build()).
		Stream(ctx)
}
