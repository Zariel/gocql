package gocql

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sync"
	"testing"
	"time"
)

type schema struct {
	keyspace_name               string
	columnfamily_name           string
	bloom_filter_fp_chance      float64
	caching                     string
	cf_id                       UUID
	comment                     string
	compaction_strategy_class   string
	compaction_strategy_options string
	comparator                  string
	compression_parameters      string
	default_time_to_live        int
	default_validator           string
	dropped_columns             map[string]int64
	gc_grace_seconds            int
	is_dense                    bool
	key_validator               string
	local_read_repair_chance    float64
	max_compaction_threshold    int
	max_index_interval          int
	memtable_flush_period_in_ms int
	min_compaction_threshold    int
	min_index_interval          int
	read_repair_chance          float64
	speculative_retry           string
	subcomparator               string
	typ                         string
}

func BenchmarkSession_ReadPerf(b *testing.B) {
	prepareFrame, err := readGzipData("testdata/frames/large-frames/prepare.gz")
	if err != nil {
		b.Fatal(err)
	}
	resultFrame, err := readGzipData("testdata/frames/large-frames/result.gz")
	if err != nil {
		b.Fatal(err)
	}

	var conns struct {
		mu     sync.Mutex
		closed bool
		c      []net.Conn
	}

	isClosed := func() bool {
		conns.mu.Lock()
		defer conns.mu.Unlock()
		return conns.closed
	}

	var readLoop = func(conn net.Conn) {
		// need to change the stream for each req
		resultFrame := copyBytes(resultFrame)
		prepareFrame := copyBytes(prepareFrame)

		br := bufio.NewReader(conn)
		header := make([]byte, 9)
		for {
			_, err := io.ReadFull(br, header)
			if err != nil {
				if !isClosed() {
					b.Error(err)
				}
				return
			}

			op := frameOp(header[4])
			size := int64(readInt(header[5:]))

			_, err = io.CopyN(ioutil.Discard, br, size)
			if err != nil {
				if !isClosed() {
					b.Error(err)
				}
				return
			}

			switch op {
			case opStartup:
				// everyday im framing
				header[0] |= protoDirectionMask
				header[4] = byte(opReady)
				header[5] = 0
				header[6] = 0
				header[7] = 0
				header[8] = 0

				if _, err = conn.Write(header); err != nil {
					if !isClosed() {
						b.Error(err)
					}
					return
				}
			case opPrepare:
				prepareFrame[2] = header[2]
				prepareFrame[3] = header[3]

				_, err = conn.Write(prepareFrame)
				if err != nil {
					if !isClosed() {
						b.Error(err)
					}
					return
				}
			case opExecute:
				resultFrame[2] = header[2]
				resultFrame[3] = header[3]

				_, err = conn.Write(resultFrame)
				if err != nil {
					if !isClosed() {
						b.Error(err)
					}
					return
				}
			default:
			}

		}
	}

	createConn = func(addr string) (net.Conn, error) {
		a, conn := net.Pipe()

		conns.mu.Lock()
		conns.c = append(conns.c, conn)
		conns.mu.Unlock()

		go readLoop(conn)

		return a, nil
	}

	cfg := NewCluster()
	cfg.ProtoVersion = 4
	cfg.Timeout = 1 * time.Second
	cfg.disableControlConn = true
	cfg.NumConns = 1

	const hosts = 4
	for i := 0; i < hosts; i++ {
		cfg.Hosts = append(cfg.Hosts, fmt.Sprintf("127.0.0.%d", i))
	}

	session, err := cfg.CreateSession()
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conns.mu.Lock()
		defer conns.mu.Unlock()

		conns.closed = true
		for _, conn := range conns.c {
			conn.Close()
		}

		conns.c = nil

		session.Close()
	}()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var row schema
		for pb.Next() {
			iter := session.Query("SELECT tokens FROM system.local").Iter()
			for iter.Scan(
				&row.keyspace_name,
				&row.columnfamily_name,
				&row.bloom_filter_fp_chance,
				&row.caching,
				&row.cf_id,
				&row.comment,
				&row.compaction_strategy_class,
				&row.compaction_strategy_options,
				&row.comparator,
				&row.compression_parameters,
				&row.default_time_to_live,
				&row.default_validator,
				&row.dropped_columns,
				&row.gc_grace_seconds,
				&row.is_dense,
				&row.key_validator,
				&row.local_read_repair_chance,
				&row.max_compaction_threshold,
				&row.max_index_interval,
				&row.memtable_flush_period_in_ms,
				&row.min_compaction_threshold,
				&row.min_index_interval,
				&row.read_repair_chance,
				&row.speculative_retry,
				&row.subcomparator,
				&row.typ,
			) {
			}

			if err := iter.Close(); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
