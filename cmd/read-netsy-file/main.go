// Copyright 2025 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/nadrama-com/netsy/internal/datafile"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <filename>\n", os.Args[0])
		os.Exit(1)
	}

	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	reader, err := datafile.NewReader(bufio.NewReader(file), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	for i := int64(0); i < reader.Count(); i++ {
		record, err := reader.Read()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

		data, _ := protojson.Marshal(record)
		fmt.Println(string(data))
	}

	reader.Close()
}
