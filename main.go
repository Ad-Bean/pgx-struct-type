package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type ColumnSpec struct {
	DataType string
	Name     string
}

func main() {
	// Set up the connection configuration
	config, err := pgx.ParseConfig("postgresql://qwe123:qwe123@prod-aws-uswe2-eks-a.risingwave.cloud:4566/dev?sslmode=verify-full&options=--tenant%3Drwc-g1hth0nmj6fecap7cctj4fmc4b-adbean-webconsole-type")
	if err != nil {
		fmt.Println("Failed to parse connection config:", err)
		return
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
	defer cancel()

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		fmt.Println("Failed to connect to the database:", err)
		return
	}
	defer conn.Close(context.TODO())
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}

		query := scanner.Text()
		ctx, cancel := context.WithTimeout(context.TODO(), time.Minute)
		defer cancel()

		rows, err := conn.Query(ctx, query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to query, %v\n", err)
			continue
		}
		defer rows.Close()

		// extract the columns names and types.
		// when the queries are comments or have no results the rows.FieldDescriptions() will be nil.
		if rows.Next() {
			extractedCols := rows.FieldDescriptions()
			cntColumn := len(extractedCols)
			for _, col := range extractedCols {
				fmt.Printf("column type %v\n\n", col)
				// Name                 string
				// TableOID             uint32
				// TableAttributeNumber uint16
				// DataTypeOID          uint32
				// DataTypeSize         int16
				// TypeModifier         int32
				// Format               int16

				/*
					create table t (v1 integer, v2 struct<a struct<a1 integer, a2 integer>, b int[]>);
					{v1 0 0   23  4 -1 0} DataTypeOID 23   DataTypeSize  4
					{v2 0 0 1043 -1 -1 0} DataTypeOID 1043 DataTypeSize -1
				*/
				// RisingWave will get {a 0 0 1043 -1 -1 0}

				// create type person as (name text, age integer, address text)
				// create table employees (id serial primary key, name text, details person);
				// Postgres will get {details 17329 3 17327 -1 -1 0} and pgx will throw error

				_, ok := pgtype.NewMap().TypeForOID(col.DataTypeOID)
				if !ok {
					fmt.Fprintf(os.Stderr, "failed to extract the column type of %s\n", col.Name)
					// continue
				}
			}

			// scan rows since the rows.Next() will prepare the next row.
			colValues := make([]interface{}, cntColumn)
			for i := 0; i < cntColumn; i++ {
				colValues[i] = new(interface{})
			}
			if err := rows.Scan(colValues...); err != nil {
				fmt.Fprintf(os.Stderr, "failed to scan rows %v\n", err)
				continue
			}
			for _, col := range colValues {
				fmt.Printf("%v    ", *(col.(*interface{})))
			}
			fmt.Println("")

			for rows.Next() {
				colValues = make([]interface{}, cntColumn)
				for i := 0; i < cntColumn; i++ {
					colValues[i] = new(interface{})
				}
				if err := rows.Scan(colValues...); err != nil {
					fmt.Fprintf(os.Stderr, "failed to scan rows %v\n", err)
					continue
				}
				for _, col := range colValues {
					fmt.Printf("%v    ", *(col.(*interface{})))
				}
				fmt.Println("")
			}
		}
	}
}
