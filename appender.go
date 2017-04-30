package rados

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/childoftheuniverse/filesystem"
	"golang.org/x/net/context"
	"os"
)

/*
RadosAppender provides a WriteCloser API for appending data to Rados objects.
Data passed to Write() will be appended to the end of the Rados object demarked
by its oid.
Seeks are supported, but only as a means to determine the current position.
*/
type RadosAppender struct {
	rctx *rados.IOContext
	oid  string
	pos  int64
}

/*
NewRadosAppender creates a new RadosAppender for the Rados object described with
the specified oid.
*/
func NewRadosAppender(rctx *rados.IOContext, oid string) (*RadosAppender, error) {
	var stat rados.ObjectStat
	var pos int64
	var err error

	/*
	   Determine the size of the object. If this fails, assume the object doesn't
	   exist and we start from offset 0.
	*/
	if stat, err = rctx.Stat(oid); err == nil {
		pos = int64(stat.Size)
	}

	return &RadosAppender{
		rctx: rctx,
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
func (w *RadosAppender) Write(ctx context.Context, p []byte) (int, error) {
	var err error
	if err = w.rctx.Append(w.oid, p); err != nil {
		return 0, err
	}
	w.pos += int64(len(p))
	return len(p), nil
}

/*
Seek can be called with 0, os.SEEK_CUR to determine the current position in the
Rados object. Any other calls to Seek are not supported.
*/
func (w *RadosAppender) Seek(ctx context.Context, offset int64, whence int) (
	int64, error) {
	if offset == 0 && whence == os.SEEK_CUR {
		return w.pos, nil
	}

	return w.pos, filesystem.EUNSUPP
}

/*
Tell is fully supported and returns the current offset into the object.
*/
func (w *RadosAppender) Tell(ctx context.Context) (int64, error) {
	return w.pos, nil
}

/*
Close is a no-op since Rados operations are quasi-synchronous and stateless.
*/
func (*RadosAppender) Close(ctx context.Context) error {
	return nil
}
