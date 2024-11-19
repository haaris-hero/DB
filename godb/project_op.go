package godb

import (
	"fmt"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
	// You may want to add additional fields here
	// TODO: some code goes here
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, fmt.Errorf("length of selectFields and outputNames must match")
	}

	return &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		child:        child,
		distinct:     distinct,
		// Add additional fields if required (e.g., for handling distinct)
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fields := make([]FieldType, len(p.selectFields))
	for i, expr := range p.selectFields {
		fieldType := expr.GetExprType().Ftype // Assume GetExprType() is a valid method
		fields[i] = FieldType{
			Fname: p.outputNames[i],
			Ftype: fieldType,
		}
	}
	return &TupleDesc{Fields: fields}
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	seenTuples := make(map[any]struct{}) // For handling distinct tuples, if needed

	return func() (*Tuple, error) {
		for {
			tuple, err := childIter()
			if err != nil {
				return nil, err
			}
			if tuple == nil && err == nil {
				return nil, nil
			}

			// Project the fields
			projectedFields := make([]DBValue, len(p.selectFields))
			for i, expr := range p.selectFields {
				value, err := expr.EvalExpr(tuple) // Assume Evaluate processes an expression
				if err != nil {
					return nil, err
				}
				projectedFields[i] = value
			}

			// Create a new tuple with projected fields
			projectedTuple := &Tuple{
				Fields: projectedFields,
				Desc:   *p.Descriptor(),
			}

			// Handle distinct logic (if enabled)
			if p.distinct {
				key := projectedTuple.tupleKey() // Serialize tuple to a string as a key
				if _, exists := seenTuples[key]; exists {
					continue // Skip duplicates
				}
				seenTuples[key] = struct{}{}
			}

			return projectedTuple, nil
		}
	}, nil
}
