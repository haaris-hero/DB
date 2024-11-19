package godb

import "fmt"

type InsertOp struct {
	insertFile DBFile
	child      Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{
		insertFile: insertFile,
		child:      child,
	}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{Fname: "count", Ftype: IntType},
		},
	}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile and then returns a one-field tuple with a "count"
// field indicating the number of tuples that were inserted.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// Get iterator from child operator
	childIter, err := iop.child.Iterator(tid)
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

		// Insert all tuples from child iterator
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

			// Insert the tuple into the file
			err = iop.insertFile.insertTuple(tuple, tid)
			if err != nil {
				return nil, fmt.Errorf("failed to insert tuple: %v", err)
			}
			count++
		}

		// Create return tuple with count
		countTuple := &Tuple{
			Fields: []DBValue{
				IntField{Value: count},
			},
			Desc: *iop.Descriptor(),
		}

		// Mark as returned so next iteration returns nil
		returned = true

		return countTuple, nil
	}, nil
}
