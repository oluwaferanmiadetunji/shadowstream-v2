package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

type Column struct {
	Name     string `json:"name"`
	TypeOID  uint32 `json:"type_oid"`
	Nullable bool   `json:"nullable"`
}

type Relation struct {
	ID        uint32   `json:"id"`
	Namespace string   `json:"namespace"`
	Name      string   `json:"name"`
	Columns   []Column `json:"columns"`
}

type Change struct {
	Kind     string                 `json:"kind"`
	Relation string                 `json:"relation"`
	RelID    uint32                 `json:"rel_id"`
	New      map[string]any         `json:"new,omitempty"`
	Old      map[string]any         `json:"old,omitempty"`
	RawNew   map[string]string      `json:"raw_new,omitempty"`
	RawOld   map[string]string      `json:"raw_old,omitempty"`
	Meta     map[string]interface{} `json:"meta,omitempty"`
	Received string                 `json:"received"`
	WALStart string                 `json:"wal_start,omitempty"`
	EventID  string                 `json:"event_id"`
}

type Tx struct {
	Xid       uint32   `json:"xid"`
	BeginLSN  string   `json:"begin_lsn"`
	CommitLSN string   `json:"commit_lsn"`
	CommitTS  string   `json:"commit_ts"`
	Changes   []Change `json:"changes"`
}

type Checkpoint struct {
	LastCommitLSN string `json:"last_commit_lsn"`
	LastXid       uint32 `json:"last_xid"`
	UpdatedAt     string `json:"updated_at"`
}

type SeenStore struct {
	MaxCommits int                 `json:"-"`
	Order      []string            `json:"order"`
	ByCommit   map[string][]string `json:"by_commit"`
	set        map[string]struct{} `json:"-"`
}

func loadCheckpoint(path string) (*Checkpoint, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return nil, err
	}

	if cp.LastCommitLSN == "" {
		return nil, nil
	}

	return &cp, nil
}

func saveCheckpoint(path string, cp Checkpoint) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func newSeenStore(maxCommits int) *SeenStore {
	return &SeenStore{
		MaxCommits: maxCommits,
		Order:      []string{},
		ByCommit:   map[string][]string{},
		set:        map[string]struct{}{},
	}
}

func loadSeenStore(path string, maxCommits int) (*SeenStore, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newSeenStore(maxCommits), nil
		}
		return nil, err
	}

	var s SeenStore
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}

	s.MaxCommits = maxCommits
	s.set = map[string]struct{}{}

	start := 0
	if len(s.Order) > maxCommits {
		start = len(s.Order) - maxCommits
	}

	keptOrder := append([]string{}, s.Order[start:]...)
	keptByCommit := map[string][]string{}

	for _, c := range keptOrder {
		ids := s.ByCommit[c]
		keptByCommit[c] = ids
		for _, id := range ids {
			s.set[id] = struct{}{}
		}
	}

	s.Order = keptOrder
	s.ByCommit = keptByCommit

	return &s, nil
}

func (s *SeenStore) Seen(id string) bool {
	_, ok := s.set[id]
	return ok
}

func (s *SeenStore) MarkCommit(commitLSN string, ids []string) {
	if len(ids) == 0 {
		if _, ok := s.ByCommit[commitLSN]; ok {
			return
		}
		s.Order = append(s.Order, commitLSN)
		s.ByCommit[commitLSN] = []string{}
		s.trim()
		return
	}

	if _, ok := s.ByCommit[commitLSN]; ok {
		return
	}

	s.Order = append(s.Order, commitLSN)
	s.ByCommit[commitLSN] = ids
	for _, id := range ids {
		s.set[id] = struct{}{}
	}
	s.trim()
}

func (s *SeenStore) trim() {
	if len(s.Order) <= s.MaxCommits {
		return
	}
	excess := len(s.Order) - s.MaxCommits
	toDrop := append([]string{}, s.Order[:excess]...)
	s.Order = s.Order[excess:]

	for _, c := range toDrop {
		ids := s.ByCommit[c]
		delete(s.ByCommit, c)
		for _, id := range ids {
			delete(s.set, id)
		}
	}
}

func (s *SeenStore) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(struct {
		Order    []string            `json:"order"`
		ByCommit map[string][]string `json:"by_commit"`
	}{
		Order:    s.Order,
		ByCommit: s.ByCommit,
	}, "", "  ")
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

func stableKV(m map[string]string) string {
	if len(m) == 0 {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteString("=")
		sb.WriteString(m[k])
		sb.WriteString(";")
	}
	return sb.String()
}

func makeEventID(commitLSN string, xid uint32, rel string, kind string, oldRaw, newRaw map[string]string) string {
	h := sha256.New()
	h.Write([]byte(commitLSN))
	h.Write([]byte("|"))
	h.Write([]byte(fmt.Sprintf("%d", xid)))
	h.Write([]byte("|"))
	h.Write([]byte(rel))
	h.Write([]byte("|"))
	h.Write([]byte(kind))
	h.Write([]byte("|old:"))
	h.Write([]byte(stableKV(oldRaw)))
	h.Write([]byte("|new:"))
	h.Write([]byte(stableKV(newRaw)))
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)
}

func main() {
	ctx := context.Background()
	_ = godotenv.Load()

	dsn := os.Getenv("PG_REPL_DSN")
	if dsn == "" {
		panic(fmt.Errorf("PG_REPL_DSN is required (e.g. postgres://shadow:shadow@localhost:5433/shadowdb?replication=database)"))
	}

	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer conn.Close(ctx)

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		panic(fmt.Errorf("REDIS_ADDR is required (e.g. localhost:6379)"))
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer rdb.Close()

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		panic(err)
	}

	slot := "shadowstream_v2"
	publication := "shadowstream_pub"
	checkpointPath := "./data/checkpoint.json"
	startLSN := sys.XLogPos

	var lastCommittedLSN pglogrepl.LSN

	cp, err := loadCheckpoint(checkpointPath)
	if err != nil {
		panic(err)
	}

	if cp != nil {
		lsn, err := pglogrepl.ParseLSN(cp.LastCommitLSN)
		if err != nil {
			panic(err)
		}
		startLSN = lsn
		lastCommittedLSN = lsn
		fmt.Println("loaded checkpoint:", cp.LastCommitLSN, "xid:", cp.LastXid)
	}

	pluginArgs := []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publication),
	}

	err = pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("pgoutput replication started at", startLSN)

	relations := map[uint32]Relation{}
	var currentTx *Tx

	standbyTimeout := 10 * time.Second
	nextStandby := time.Now().Add(standbyTimeout)
	seenPath := "./data/seen.json"

	seen, err := loadSeenStore(seenPath, 1000)
	if err != nil {
		panic(err)
	}

	for {
		if time.Now().After(nextStandby) {
			err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: lastCommittedLSN,
				WALFlushPosition: lastCommittedLSN,
				WALApplyPosition: lastCommittedLSN,
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

			msgType := m.Data[0]

			if msgType == 'k' {
				continue
			}

			if msgType != 'w' {
				continue
			}

			if len(m.Data) < 1+8+8+8 {
				continue
			}

			walStart := binary.BigEndian.Uint64(m.Data[1:9])
			walData := m.Data[1+8+8+8:]

			ld, err := pglogrepl.Parse(walData)
			if err != nil {
				panic(err)
			}

			switch v := ld.(type) {
			case *pglogrepl.BeginMessage:
				currentTx = &Tx{
					Xid:      v.Xid,
					BeginLSN: v.FinalLSN.String(),
					Changes:  []Change{},
				}

			case *pglogrepl.CommitMessage:
				if currentTx == nil {
					continue
				}

				commitLSNStr := v.CommitLSN.String()

				currentTx.CommitLSN = commitLSNStr
				currentTx.CommitTS = v.CommitTime.UTC().Format(time.RFC3339Nano)

				filtered := make([]Change, 0, len(currentTx.Changes))
				marked := make([]string, 0, len(currentTx.Changes))

				for i := range currentTx.Changes {
					ch := currentTx.Changes[i]

					id := makeEventID(
						commitLSNStr,
						currentTx.Xid,
						ch.Relation,
						ch.Kind,
						ch.RawOld,
						ch.RawNew,
					)

					ch.EventID = id

					if seen.Seen(id) {
						continue
					}

					filtered = append(filtered, ch)
					marked = append(marked, id)
				}

				currentTx.Changes = filtered

				if len(currentTx.Changes) > 0 {
					payload, err := json.Marshal(currentTx)
					if err != nil {
						panic(err)
					}

					txID := commitLSNStr

					_, err = rdb.XAdd(ctx, &redis.XAddArgs{
						Stream: "shadowstream:tx",
						Values: map[string]any{
							"commit_lsn": commitLSNStr,
							"xid":        fmt.Sprintf("%d", currentTx.Xid),
							"commit_ts":  currentTx.CommitTS,
							"tx_id":      txID,
							"payload":    string(payload),
						},
					}).Result()
					if err != nil {
						panic(err)
					}

				}

				lastCommittedLSN = v.CommitLSN

				seen.MarkCommit(commitLSNStr, marked)
				if err := seen.Save(seenPath); err != nil {
					panic(err)
				}

				if err := saveCheckpoint(checkpointPath, Checkpoint{
					LastCommitLSN: commitLSNStr,
					LastXid:       currentTx.Xid,
					UpdatedAt:     time.Now().UTC().Format(time.RFC3339Nano),
				}); err != nil {
					panic(err)
				}

				currentTx = nil

			case *pglogrepl.RelationMessage:
				cols := make([]Column, 0, len(v.Columns))
				for _, c := range v.Columns {
					cols = append(cols, Column{
						Name:     c.Name,
						TypeOID:  c.DataType,
						Nullable: (c.Flags & 0x01) == 0,
					})
				}

				relations[v.RelationID] = Relation{
					ID:        v.RelationID,
					Namespace: v.Namespace,
					Name:      v.RelationName,
					Columns:   cols,
				}

			case *pglogrepl.InsertMessage:
				if currentTx == nil {
					continue
				}
				rel, ok := relations[v.RelationID]
				if !ok {
					continue
				}

				newMap, raw := tupleToMaps(rel, *v.Tuple)

				currentTx.Changes = append(currentTx.Changes, Change{
					Kind:     "INSERT",
					Relation: rel.Namespace + "." + rel.Name,
					RelID:    v.RelationID,
					New:      newMap,
					RawNew:   raw,
					Received: time.Now().UTC().Format(time.RFC3339Nano),
					WALStart: pglogrepl.LSN(walStart).String(),
				})

			case *pglogrepl.UpdateMessage:
				if currentTx == nil {
					continue
				}
				rel, ok := relations[v.RelationID]
				if !ok {
					continue
				}

				var oldMap map[string]any
				var rawOld map[string]string
				if v.OldTuple != nil {
					oldMap, rawOld = tupleToMaps(rel, *v.OldTuple)
				}

				newMap, rawNew := tupleToMaps(rel, *v.NewTuple)

				currentTx.Changes = append(currentTx.Changes, Change{
					Kind:     "UPDATE",
					Relation: rel.Namespace + "." + rel.Name,
					RelID:    v.RelationID,
					Old:      oldMap,
					New:      newMap,
					RawOld:   rawOld,
					RawNew:   rawNew,
					Received: time.Now().UTC().Format(time.RFC3339Nano),
					WALStart: pglogrepl.LSN(walStart).String(),
				})

			case *pglogrepl.DeleteMessage:
				if currentTx == nil {
					continue
				}
				rel, ok := relations[v.RelationID]
				if !ok {
					continue
				}

				var oldMap map[string]any
				var rawOld map[string]string
				if v.OldTuple != nil {
					oldMap, rawOld = tupleToMaps(rel, *v.OldTuple)
				}

				currentTx.Changes = append(currentTx.Changes, Change{
					Kind:     "DELETE",
					Relation: rel.Namespace + "." + rel.Name,
					RelID:    v.RelationID,
					Old:      oldMap,
					RawOld:   rawOld,
					Received: time.Now().UTC().Format(time.RFC3339Nano),
					WALStart: pglogrepl.LSN(walStart).String(),
				})

			default:
				continue
			}

		default:
			continue
		}
	}
}

func tupleToMaps(rel Relation, tup pglogrepl.TupleData) (map[string]any, map[string]string) {
	out := map[string]any{}
	raw := map[string]string{}

	for i, col := range rel.Columns {
		if i >= len(tup.Columns) {
			break
		}

		td := tup.Columns[i]

		switch td.DataType {
		case 'n':
			out[col.Name] = nil
			raw[col.Name] = ""
		case 'u':
			out[col.Name] = "__unchanged__"
			raw[col.Name] = "__unchanged__"
		case 't':
			s := string(td.Data)
			raw[col.Name] = s
			out[col.Name] = s
		default:
			s := string(td.Data)
			raw[col.Name] = s
			out[col.Name] = s
		}
	}

	return out, raw
}
