package rados

import (
	"context"
	"github.com/ceph/go-ceph/rados"
	"github.com/prometheus/client_golang/prometheus"
	"io"
	"os"
	"time"
)

var radosReadLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "rados",
	Name:      "read_latency",
	Help:      "Latency of Rados Read requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 5, 20),
}, []string{"pool"})
var radosWriteLatencies = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Subsystem: "rados",
	Name:      "write_latency",
	Help:      "Latency of Rados Write requests",
	Buckets:   prometheus.ExponentialBuckets(0.001, 5, 20),
}, []string{"pool"})
var radosReadErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "read_errors",
	Help:      "Number of errors received when reading from Rados files",
}, []string{"pool"})
var radosWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "write_errors",
	Help:      "Number of errors received when writing to Rados files",
}, []string{"pool"})
var radosReadBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "read_bytes",
	Help:      "Number of bytes received when reading from Rados files",
}, []string{"pool"})
var radosWriteBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Subsystem: "rados",
	Name:      "write_bytes",
	Help:      "Number of bytes sent when writing to Rados files",
}, []string{"pool"})

func init() {
	prometheus.MustRegister(radosReadLatencies)
	prometheus.MustRegister(radosWriteLatencies)
	prometheus.MustRegister(radosReadErrors)
	prometheus.MustRegister(radosWriteErrors)
	prometheus.MustRegister(radosReadBytes)
	prometheus.MustRegister(radosWriteBytes)
}

/*
ReadWriteCloser provides both a ReadCloser and a WriteCloser for Rados objects.
A virtual position within the object is maintained by this class to provide
a regular filesystem API.
*/
type ReadWriteCloser struct {
	rctx *rados.IOContext
	pool string
	oid  string
	pos  int64
}

/*
NewReadWriteCloser provides a ReadWriteCloser object for the Rados object
designated as "oid" in the given I/O context. The initial position will be set
to the beginning of the object.

This function itself only constructs the ReadWriteCloser object, it does not
guarantee that the object can actually be accessed properly. This will only
be determined on the first call to Read() or Write().
*/
func NewReadWriteCloser(rctx *rados.IOContext, oid string) *ReadWriteCloser {
	var pool string

	/*
	   Determine the name of the pool the object resides in, for prometheus.
	*/
	pool, _ = rctx.GetPoolName()

	return &ReadWriteCloser{
		rctx: rctx,
		pool: pool,
		oid:  oid,
		pos:  0,
	}
}

/*
Read fetches up to len(p) bytes from the Rados object pointed to into the
specified buffer. Returns the number of bytes actually read.
TODO: does not respect contexts yet.
*/
func (r *ReadWriteCloser) Read(ctx context.Context, p []byte) (n int, err error) {
	var start = time.Now()
	n, err = r.rctx.Read(r.oid, p, uint64(r.pos))
	if n > 0 {
		r.pos += int64(n)
	} else if n == 0 && err == nil {
		/* TODO: find some way to check this is actually the end of the file. */
		err = io.EOF
	}
	if err == nil {
		radosReadLatencies.With(prometheus.Labels{"pool": r.pool}).Observe(
			time.Now().Sub(start).Seconds())
		radosReadBytes.With(prometheus.Labels{"pool": r.pool}).Add(
			float64(n))
	} else {
		radosReadErrors.With(prometheus.Labels{"pool": r.pool}).Inc()
	}
	return
}

/*
Write emplaces the bytes contained in p into the current position of the Rados
object specified by oid.
TODO: does not respect contexts yet.
*/
func (r *ReadWriteCloser) Write(ctx context.Context, p []byte) (int, error) {
	var start = time.Now()
	var err = r.rctx.Write(r.oid, p, uint64(r.pos))
	if err != nil {
		radosWriteErrors.With(prometheus.Labels{"pool": r.pool}).Inc()
		return 0, err
	}

	radosWriteLatencies.With(prometheus.Labels{"pool": r.pool}).Observe(
		time.Now().Sub(start).Seconds())
	radosWriteBytes.With(prometheus.Labels{"pool": r.pool}).Add(
		float64(len(p)))
	r.pos += int64(len(p))
	return len(p), nil
}

/*
Seek modifies the position of the ReadWriteCloser in the Rados object as
outlined in the io.Seeker API.
TODO: does not respect contexts yet.
*/
func (r *ReadWriteCloser) Seek(
	ctx context.Context, offset int64, whence int) (int64, error) {
	var stat rados.ObjectStat
	var newpos int64
	var err error

	stat, err = r.rctx.Stat(r.oid)
	if err != nil {
		return r.pos, err
	}

	if whence == os.SEEK_SET {
		// Seeking relative to the beginning of the file.
		newpos = offset
	} else if whence == os.SEEK_CUR {
		// Seeking relative to the current offset.
		newpos = r.pos + offset
	} else if whence == os.SEEK_END {
		// Seeking relative to the end of the file.
		newpos = int64(stat.Size) + offset
	}

	if newpos < 0 || newpos > int64(stat.Size) {
		return r.pos, os.ErrInvalid
	}

	r.pos = newpos
	return newpos, nil
}

/*
Tell determines the current position of the ReadWriteCloser in the Rados
object as outlined in the io.Seeker API.
*/
func (r *ReadWriteCloser) Tell(ctx context.Context) (int64, error) {
	return r.pos, nil
}

/*
Close is a no-op since Rados operations are quasi-synchronous and stateless.
*/
func (r *ReadWriteCloser) Close(ctx context.Context) error {
	return nil
}
