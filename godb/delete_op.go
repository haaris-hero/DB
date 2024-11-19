package godb

import (
	"fmt"
)

type DeleteOp struct {
	deleteFile DBFile
	child      Operator
}

// Construct a delete operator that deletes the records in the child Operator
// from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{
		deleteFile: deleteFile,
		child:      child,
	}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: "count", Ftype: IntType},
		},
	}
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile and then returns a one-field tuple with a "count"
// field indicating the number of tuples that were deleted.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// Get iterator from child operator
	childIter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	// Create closure to track state
	var returned bool
	var count int64 = 0

	return func() (*Tuple, error) {
		// If we've already returned the count, we're done
		if returned {
			return nil, nil
		}

		// Delete all tuples from child iterator
		for {
			// Get next tuple from child
			tuple, err := childIter()
			if err != nil {
				return nil, err
			}
			// If no more tuples, break
			if tuple == nil {
				break
			}

			// Delete the tuple from the file
			err = dop.deleteFile.deleteTuple(tuple, tid)
			if err != nil {
				return nil, fmt.Errorf("failed to delete tuple: %v", err)
			}
			count++
		}

		// Create return tuple with count of deleted records
		countTuple := &Tuple{
			Fields: []DBValue{
				IntField{Value: count},
			},
			Desc: *dop.Descriptor(),
		}

		// Mark as returned so next iteration returns nil
		returned = true

		return countTuple, nil
	}, nil
}
