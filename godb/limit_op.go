package godb

import (
	"fmt"
)

type LimitOp struct {
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{
		child:     child,
		limitTups: lim,
	}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	// The descriptor of the LimitOp is the same as its child operator.
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// Fetch the limit value
	limitValue, err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, fmt.Errorf("error evaluating limit expression: %v", err)
	}

	// Ensure the limit is an integer value
	limit, ok := limitValue.(IntField)
	if !ok {
		return nil, fmt.Errorf("limit expression did not evaluate to an integer")
	}
	if limit.Value < 0 {
		return nil, fmt.Errorf("limit value cannot be negative")
	}

	// Get the child iterator
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	// Track the number of tuples returned
	count := 0

	// Iterator function for limiting tuples
	return func() (*Tuple, error) {
		if count >= int(limit.Value) {
			return nil, nil // Stop when the limit is reached
		}
		tuple, err := childIter()
		if err != nil {
			return nil, err // Propagate errors from the child iterator
		}
		count++
		return tuple, nil
	}, nil
}
