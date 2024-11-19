package godb

import (
	"fmt"
	"io/ioutil"
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {

	tempHeapFile, err := ioutil.TempFile("", "heapfile_*.db")
	if err != nil {
		return 0, fmt.Errorf("failed to create temporary heap file: %w", err)
	}
	defer os.Remove(tempHeapFile.Name())
	defer tempHeapFile.Close()

	hf, err := NewHeapFile(tempHeapFile.Name(), &td, bp)
	if err != nil {
		return 0, fmt.Errorf("failed to create heap file: %w", err)
	}

	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open or create file: %w", err)
	}
	defer file.Close()

	err = hf.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return 0, fmt.Errorf("failed to load CSV file: %w", err)
	}

	fieldIndex := -1
	for i, field := range td.Fields {
		if field.Fname == sumField {
			fieldIndex = i
			break
		}
	}

	if fieldIndex == -1 {
		return 0, fmt.Errorf("field %s does not exist in the tuple descriptor", sumField)
	}

	nextTuple, err := hf.Iterator(0)
	if err != nil {
		return 0, fmt.Errorf("failed to create iterator: %w", err)
	}

	sum := 0
	for {
		tuple, err := nextTuple()
		if err != nil {
			return 0, fmt.Errorf("iterator error: %w", err)
		}
		if tuple == nil {
			break
		}

		fieldValue := tuple.Fields[fieldIndex]
		switch v := fieldValue.(type) {
		case IntField:
			sum += int(v.Value)
		default:
			return 0, fmt.Errorf("unsupported field type for field %s", sumField)
		}
	}

	return sum, nil
}
