package godb

import (
	"fmt"
)

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group.
	newAggState []AggState

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Construct an aggregator with a group-by.
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Construct an aggregator with no group-by.
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

// Return a TupleDescriptor for this aggregation.
//
// If the aggregator has no group-by, the returned descriptor should contain the
// union of the fields in the descriptors of the aggregation states. If the
// aggregator has a group-by, the returned descriptor will additionally start
// with the group-by fields, and then the aggregation states descriptors like
// that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType.
//
// HINT: use [TupleDesc.merge] to merge the two [TupleDesc]s.
func (a *Aggregator) Descriptor() *TupleDesc {
	// If no group by fields, simply merge all aggregation state descriptors
	if a.groupByFields == nil {
		var desc *TupleDesc
		for _, aggState := range a.newAggState {
			aggDesc := aggState.GetTupleDesc()
			if desc == nil {
				desc = aggDesc
			} else {
				desc = desc.merge(aggDesc)
			}
		}
		return desc
	}

	// With group by fields, first create a tuple desc for group by fields
	groupByDesc := &TupleDesc{
		Fields: make([]FieldType, len(a.groupByFields)),
	}
	for i, expr := range a.groupByFields {
		groupByDesc.Fields[i] = FieldType{
			Fname: fmt.Sprintf("groupby_%d", i),
			Ftype: expr.GetExprType().Ftype,
		}
	}

	// Then merge with aggregation state descriptors
	var aggDesc *TupleDesc
	for _, aggState := range a.newAggState {
		stateDesc := aggState.GetTupleDesc()
		if aggDesc == nil {
			aggDesc = stateDesc
		} else {
			aggDesc = aggDesc.merge(stateDesc)
		}
	}

	// Merge group by descriptor with aggregation descriptor
	return groupByDesc.merge(aggDesc)
}

// Returns an iterator over the results of the aggregate. The aggregate should
// be the result of aggregating each group's tuples and the iterator should
// iterate through each group's result. In the case where there is no group-by,
// the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
func (a *Aggregator) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// the child iterator
	childIter, err := a.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil"}
			}
			newAggState = append(newAggState, copy)
		}

		aggState[DefaultGroup] = &newAggState
	}

	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() (*Tuple, error)

	return func() (*Tuple, error) {
		// iterates thru all child tuples
		for t, err := childIter(); t != nil || err != nil; t, err = childIter() {
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}

			if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
				for i := 0; i < len(a.newAggState); i++ {
					(*aggState[DefaultGroup])[i].AddTuple(t)
				}
			} else { // adds tuple to the aggregation with grouping
				keygenTup, err := extractGroupByKeyTuple(a, t)
				if err != nil {
					return nil, err
				}

				key := keygenTup.tupleKey()
				if aggState[key] == nil {
					asNew := make([]AggState, len(a.newAggState))
					aggState[key] = &asNew
					groupByList = append(groupByList, keygenTup)
				}

				addTupleToGrpAggState(a, t, aggState[key])
			}
		}

		if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
			if a.groupByFields == nil {
				var tup *Tuple
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					tup = joinTuples(tup, newTup)
				}
				finalizedIter = func() (*Tuple, error) { return nil, nil }
				return tup, nil
			} else {
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iterator, return a tuple that identifies t's
// group. The returned tuple should contain the fields from the groupByFields
// list passed into the aggregator constructor. The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
//
// If there is any error during expression evaluation, return the error.
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	if a.groupByFields == nil {
		return nil, fmt.Errorf("no group by fields defined")
	}

	keyFields := make([]DBValue, len(a.groupByFields))
	newDesc := make([]FieldType, len(a.groupByFields))

	for i, expr := range a.groupByFields {
		val, err := expr.EvalExpr(t)
		if err != nil {
			return nil, err
		}
		keyFields[i] = val
		newDesc[i] = FieldType{Fname: expr.GetExprType().Fname, Ftype: expr.GetExprType().Ftype}
	}

	return &Tuple{
		Fields: keyFields,
		Desc: TupleDesc{
			newDesc,
		},
	}, nil
}

// Given a tuple t from child and (a pointer to) the array of partially computed
// aggregates grpAggState, add t into all partial aggregations using
// [AggState.AddTuple]. If any of the array elements is of grpAggState is null
// (i.e., because this is the first invocation of this method, create a new
// aggState using [aggState.Copy] on appropriate element of the a.newAggState
// field and add the new aggState to grpAggState.
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	for i := range *grpAggState {
		// If the aggregation state is nil, create a new one by copying from template
		if (*grpAggState)[i] == nil {
			(*grpAggState)[i] = a.newAggState[i].Copy()
		}

		// Add tuple to this aggregation state
		(*grpAggState)[i].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned
// tuples should be structured according to the TupleDesc returned from the
// Descriptor() method.
//
// HINT: you can call [aggState.Finalize] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState
// tuples using the joinTuples function in tuple.go you wrote in lab 1.
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() (*Tuple, error) {
	currentIndex := 0

	return func() (*Tuple, error) {
		// If we've processed all groups, return nil
		if currentIndex >= len(groupByList) {
			return nil, nil
		}

		// Get the current group's key tuple
		groupByTuple := groupByList[currentIndex]

		// Get the current group's aggregation states
		groupAggStates := aggState[groupByTuple.tupleKey()]

		// Finalize each aggregation state
		var resultTuple *Tuple
		for _, aggState := range *groupAggStates {
			finalizedStateTuple := aggState.Finalize()

			// Join the group by tuple with the finalized state tuple
			if resultTuple == nil {
				resultTuple = joinTuples(groupByTuple, finalizedStateTuple)
			} else {
				resultTuple = joinTuples(resultTuple, finalizedStateTuple)
			}
		}

		// Move to next group
		currentIndex++

		return resultTuple, nil
	}
}