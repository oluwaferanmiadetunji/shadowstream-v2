package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/joho/godotenv"
)

func main() {
	ctx := context.Background()
	_ = godotenv.Load()

	connStr := os.Getenv("PG_REPL_DSN")

	if connStr == "" {
		panic(fmt.Errorf("Invalid pg replicator dsn"))
	}

	conn, err := pgconn.Connect(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		panic(err)
	}

	slot := "shadowstream_v2_test"
	startLSN := sys.XLogPos

	err = pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{})
	if err != nil {
		panic(err)
	}

	fmt.Println("replication started at", startLSN)

	standbyTimeout := 10 * time.Second
	nextStandby := time.Now().Add(standbyTimeout)

	var lastReceivedLSN pglogrepl.LSN

	for {
		if time.Now().After(nextStandby) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lastReceivedLSN,
				WALFlushPosition: lastReceivedLSN,
				WALApplyPosition: lastReceivedLSN,
				ClientTime:       time.Now(),
			})
			if err != nil {
				panic(err)
			}
			nextStandby = time.Now().Add(standbyTimeout)
		}

		ctxRecv, cancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := conn.ReceiveMessage(ctxRecv)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			panic(err)
		}

		switch m := msg.(type) {
		case *pgproto3.ErrorResponse:
			panic(fmt.Errorf("postgres error: %s (%s)", m.Message, m.Code))
		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				continue
			}

			t := m.Data[0]

			if t == 'k' {
				continue
			}

			if t != 'w' {
				continue
			}

			if len(m.Data) < 1+8+8+8 {
				continue
			}

			walStart := binary.BigEndian.Uint64(m.Data[1:9])
			walData := m.Data[1+8+8+8:]

			lastReceivedLSN = pglogrepl.LSN(walStart) + pglogrepl.LSN(len(walData))
			fmt.Println(string(walData))

		default:
			continue
		}
	}

}
