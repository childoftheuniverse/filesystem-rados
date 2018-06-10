package rados

import (
	"context"
	"github.com/ceph/go-ceph/rados"
	"github.com/childoftheuniverse/filesystem"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"time"
)

var radosAppenderLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "rados",
	Name:      "append_latency",
	Help:      "Latency of Rados Append requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 5, 20),
}, []string{"pool"})
var radosAppenderErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "append_errors",
	Help:      "Number of errors received when appending to Rados files",
}, []string{"pool"})
var radosAppenderBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "append_bytes",
	Help:      "Number of bytes sent when appending to Rados files",
}, []string{"pool"})

func init() {
	prometheus.MustRegister(radosAppenderLatencies)
	prometheus.MustRegister(radosAppenderErrors)
}

/*
Appender provides a WriteCloser API for appending data to Rados objects.
Data passed to Write() will be appended to the end of the Rados object demarked
by its oid.
Seeks are supported, but only as a means to determine the current position.
*/
type Appender struct {
	rctx *rados.IOContext
	pool string
	oid  string
	pos  int64
}

/*
NewAppender creates a new Appender for the Rados object described with
the specified oid.
*/
func NewAppender(rctx *rados.IOContext, oid string) (*Appender, error) {
	var stat rados.ObjectStat
	var pool string
	var pos int64
	var err error

	/*
	   Determine the name of the pool the object resides in, for prometheus.
	*/
	if pool, err = rctx.GetPoolName(); err != nil {
		return nil, err
	}

	/*
	   Determine the size of the object. If this fails, assume the object doesn't
	   exist and we start from offset 0.
	*/
	if stat, err = rctx.Stat(oid); err == nil {
		pos = int64(stat.Size)
	}

	return &Appender{
		rctx: rctx,
		pool: pool,
		oid:  oid,
		pos:  pos,
	}, nil
}

/*
Write appends the specified input bytes to the end of the Rados object.
Parallel Write() calls from different callers will cause data to be interleaved
as complete Write() calls.
TODO: does not respect contexts yet.
*/
func (w *Appender) Write(ctx context.Context, p []byte) (int, error) {
	var start = time.Now()
	var err error

	if err = w.rctx.Append(w.oid, p); err != nil {
		radosAppenderErrors.With(prometheus.Labels{"pool": w.pool}).Inc()
		return 0, err
	}

	radosAppenderLatencies.With(prometheus.Labels{"pool": w.pool}).Observe(
		time.Now().Sub(start).Seconds())
	radosAppenderBytes.With(prometheus.Labels{"pool": w.pool}).Add(
		float64(len(p)))

	w.pos += int64(len(p))
	return len(p), nil
}

/*
Seek can be called with 0, os.SEEK_CUR to determine the current position in the
Rados object. Any other calls to Seek are not supported.
*/
func (w *Appender) Seek(ctx context.Context, offset int64, whence int) (
	int64, error) {
	if offset == 0 && whence == os.SEEK_CUR {
		return w.pos, nil
	}

	return w.pos, filesystem.EUNSUPP
}

/*
Tell is fully supported and returns the current offset into the object.
*/
func (w *Appender) Tell(ctx context.Context) (int64, error) {
	return w.pos, nil
}

/*
Close is a no-op since Rados operations are quasi-synchronous and stateless.
*/
func (*Appender) Close(ctx context.Context) error {
	return nil
}
