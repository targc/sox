package sox

import (
	"context"
	"encoding/json"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type number interface {
	int | float64
}

type Keys map[string]interface{}

type sumAggSoxAdapter[TVal number, TRec any] struct {
	db               *gorm.DB
	destTable        string
	valName          string
	keysTransformer  func(id string, doc TRec) Keys
	valueTransformer func(id string, doc TRec) TVal
}

type SumAggSoxAdapterOpt[TVal number, TRec any] struct {
	DB               *gorm.DB
	DestTable        string
	ValName          string
	KeysTransformer  func(id string, doc TRec) Keys
	ValueTransformer func(id string, doc TRec) TVal
}

func NewSumAggSoxAdapter[TVal number, TRec any](opt SumAggSoxAdapterOpt[TVal, TRec]) *sumAggSoxAdapter[TVal, TRec] {
	return &sumAggSoxAdapter[TVal, TRec]{
		db:               opt.DB,
		destTable:        opt.DestTable,
		valName:          opt.ValName,
		keysTransformer:  opt.KeysTransformer,
		valueTransformer: opt.ValueTransformer,
	}
}

func (a *sumAggSoxAdapter[TVal, TRec]) processer(ctx context.Context, keys Keys, val TVal) error {

	cols := make([]clause.Column, 0, len(keys))

	rec := make(map[string]interface{})

	for k, v := range keys {
		cols = append(cols, clause.Column{Name: k})
		rec[k] = v
	}

	rec[a.valName] = val

	err := a.db.
		WithContext(ctx).
		Table(a.destTable).
		Clauses(clause.OnConflict{
			Columns: cols,
			DoUpdates: clause.Assignments(map[string]interface{}{
				a.valName: gorm.Expr(a.destTable+"."+a.valName+" + ?", val),
			}),
		}).
		Debug().
		Create(rec).
		Error

	if err != nil {
		return err
	}

	return nil
}

func (a *sumAggSoxAdapter[TVal, TRec]) Build() SoxHandlers {
	return SoxHandlers{
		Insert: func(ctx context.Context, id string, doc map[string]interface{}) error {
			b, err := json.Marshal(doc)

			if err != nil {
				return err
			}

			var rec TRec

			err = json.Unmarshal(b, &rec)

			if err != nil {
				return err
			}

			keys := a.keysTransformer(id, rec)

			if keys == nil {
				return nil
			}

			val := a.valueTransformer(id, rec)

			err = a.processer(ctx, keys, val)

			if err != nil {
				return err
			}

			return nil
		},
		Update: func(ctx context.Context, id string, updatedFields map[string]interface{}, removedFields []string) error {
			return nil
		},
		Delete: func(ctx context.Context, id string) error {
			return nil
		},
	}
}
