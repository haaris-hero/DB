package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
	// TODO: You may want to add additional fields here
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	if len(orderByFields) != len(ascending) {
		return nil, fmt.Errorf("length of orderByFields and ascending must match")
	}

	return &OrderBy{
		orderBy:   orderByFields,
		child:     child,
		ascending: ascending,
		// Add additional fields if needed
	}, nil
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	return o.child.Descriptor()
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	// Fetch all tuples from the child
	var tuples []*Tuple
	for {
		tuple, err := childIter()
		if err != nil {
			return nil, err
		}
		if tuple == nil && err == nil {
			break
		}
		tuples = append(tuples, tuple)
	}

	// Sort tuples using the sort package
	sort.SliceStable(tuples, func(i, j int) bool {
		for ind := 0; ind < len(o.orderBy); ind++ {
			expr := o.orderBy[ind]
			val1, err1 := expr.EvalExpr(tuples[i])
			val2, err2 := expr.EvalExpr(tuples[j])
			if err1 != nil || err2 != nil {
				panic(fmt.Sprintf("Error evaluating expression: %v, %v", err1, err2))
			}
			switch expr.GetExprType().Ftype {
			case IntType:
				if val1.(IntField).Value != val2.(IntField).Value {
					return (o.ascending[ind] && val1.(IntField).Value < val2.(IntField).Value) ||
						(!o.ascending[ind] && val1.(IntField).Value > val2.(IntField).Value)
				}
			case StringType:
				if val1.(StringField).Value != val2.(StringField).Value {
					return (o.ascending[ind] && val1.(StringField).Value < val2.(StringField).Value) ||
						(!o.ascending[ind] && val1.(StringField).Value > val2.(StringField).Value)
				}
			default:
				panic(fmt.Sprintf("Unsupported field type: %v", expr.GetExprType().Ftype))
			}
		}
		return false
	})

	// Iterator function for sorted tuples
	index := 0
	return func() (*Tuple, error) {
		if index >= len(tuples) {
			return nil, nil
		}
		tuple := tuples[index]
		index++
		return tuple, nil
	}, nil
}
