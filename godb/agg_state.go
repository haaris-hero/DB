package godb

import (
	"fmt"
)

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	alias string
	expr  Expr
	sum   any // Can be int or float64
}

func (a *SumAggState) Copy() AggState {
	newState := &SumAggState{
		alias: a.alias,
		expr:  a.expr,
		sum:   a.sum,
	}
	return newState
}

func intAggGetter(v DBValue) any {
	val := v.(IntField)
	return val.Value
}

func stringAggGetter(v DBValue) any {
	val := v.(StringField)
	return val.Value
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = int64(0)
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		fmt.Println("evaluation error")
		return
	}
	if current, ok := a.sum.(int64); ok {
		a.sum = current + intAggGetter(val).(int64)
	}
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: a.alias, Ftype: a.expr.GetExprType().Ftype},
		},
	}
}

func (a *SumAggState) Finalize() *Tuple {
	return &Tuple{
		Fields: []DBValue{
			IntField{int64(a.sum.(int64))},
		},
		Desc: *a.GetTupleDesc(),
	}
}

// Implements the aggregation state for AVG
type AvgAggState struct {
	alias string
	expr  Expr
	sum   int64
	count int64
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{
		alias: a.alias,
		expr:  a.expr,
		sum:   a.sum,
		count: a.count,
	}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		fmt.Println("error evaluating")
		return
	}
	a.sum += intAggGetter(val).(int64)
	a.count++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: a.alias, Ftype: a.expr.GetExprType().Ftype},
		},
	}
}

func (a *AvgAggState) Finalize() *Tuple {
	avg := float64(a.sum) / float64(a.count)
	return &Tuple{
		Fields: []DBValue{
			IntField{int64(avg)},
		},
		Desc: *a.GetTupleDesc(),
	}
}

// Implements the aggregation state for MAX
type MaxAggState struct {
	alias string
	expr  Expr
	max   int64
	first bool
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{
		alias: a.alias,
		expr:  a.expr,
		max:   a.max,
		first: a.first,
	}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.first = true
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		fmt.Println("evalutation error")
		return
	}
	if a.first {
		a.max = intAggGetter(val).(int64)
		a.first = false
		return
	}

	current := a.max

	if newVal := intAggGetter(val).(int64); newVal > current {
		a.max = newVal
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: a.alias, Ftype: a.expr.GetExprType().Ftype},
		},
	}
}

func (a *MaxAggState) Finalize() *Tuple {

	return &Tuple{
		Fields: []DBValue{IntField{a.max}},
		Desc:   *a.GetTupleDesc(),
	}
}

// Implements the aggregation state for MIN
type MinAggState struct {
	alias string
	expr  Expr
	min   int64
	first bool
}

func (a *MinAggState) Copy() AggState {
	return &MinAggState{
		alias: a.alias,
		expr:  a.expr,
		min:   a.min,
		first: a.first,
	}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.first = true
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		fmt.Println("evalutation error")
		return
	}
	if a.first {
		a.min = intAggGetter(val).(int64)
		a.first = false
		return
	}

	current := a.min

	if newVal := intAggGetter(val).(int64); newVal < current {
		a.min = newVal
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: a.alias, Ftype: a.expr.GetExprType().Ftype},
		},
	}
}

func (a *MinAggState) Finalize() *Tuple {
	return &Tuple{
		Fields: []DBValue{IntField{a.min}},
		Desc:   *a.GetTupleDesc(),
	}
}
