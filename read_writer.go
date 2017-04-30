package rados

import (
	"github.com/ceph/go-ceph/rados"
	"golang.org/x/net/context"
	"os"
)

/*
RadosReadCloser provides both a ReadCloser and a WriteCloser for Rados objects.
A virtual position within the object is maintained by this class to provide
a regular filesystem API.
*/
type RadosReadWriteCloser struct {
	rctx *rados.IOContext
	oid  string
	pos  int64
}

/*
NewRadosReadCloser provides a RadosReadCloser object for the Rados object
designated as "oid" in the given I/O context. The initial position will be set
to the beginning of the object.

This function itself only constructs the ReadCloser object, it does not
guarantee that the object can actually be accessed properly. This will only
be determined on the first call to Read() or Write().
*/
func NewRadosReadWriteCloser(rctx *rados.IOContext, oid string) *RadosReadWriteCloser {
	return &RadosReadWriteCloser{
		rctx: rctx,
		oid:  oid,
		pos:  0,
	}
}

/*
Read fetches up to len(p) bytes from the Rados object pointed to into the
specified buffer. Returns the number of bytes actually read.
TODO: does not respect contexts yet.
*/
func (r *RadosReadWriteCloser) Read(ctx context.Context, p []byte) (n int, err error) {
	n, err = r.rctx.Read(r.oid, p, uint64(r.pos))
	if n > 0 {
		r.pos += int64(n)
	}
	return
}

/*
Write emplaces the bytes contained in p into the current position of the Rados
object specified by oid.
TODO: does not respect contexts yet.
*/
func (r *RadosReadWriteCloser) Write(ctx context.Context, p []byte) (int, error) {
	var err = r.rctx.Write(r.oid, p, uint64(r.pos))
	if err == nil {
		r.pos += int64(len(p))
		return len(p), nil
	}
	return 0, err
}

/*
Seek modifies the position of the ReadWriteCloser in the Rados object as
outlined in the io.Seeker API.
TODO: does not respect contexts yet.
*/
func (r *RadosReadWriteCloser) Seek(
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
func (r *RadosReadWriteCloser) Tell(ctx context.Context) (int64, error) {
	return r.pos, nil
}

/*
Close is a no-op since Rados operations are quasi-synchronous and stateless.
*/
func (r *RadosReadWriteCloser) Close(ctx context.Context) error {
	return nil
}
