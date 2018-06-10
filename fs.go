package rados

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/ceph/go-ceph/rados"
	"github.com/childoftheuniverse/filesystem"
)

var configPath = flag.String("rados-config", "",
	"Path to the Rados configuration file (usually /etc/ceph/ceph.conf)")
var user = flag.String("rados-user", "",
	"cephx user to use for talking to ceph/rados")
var cluster = flag.String("rados-cluster", "",
	"Ceph cluster name to connect to for rados. Defaults to ceph")

/*
radosFileSystem provides a filesystem-like interface for Rados object stores.
All operations except WatchFile are supported.
*/
type radosFileSystem struct {
	rfs *rados.Conn

	/*
		openContexts holds a mapping of rados pool names to the corresponding
		currently open I/O contexts to avoid recreating them every time a file is
		accessed.
	*/
	openContexts    map[string]*rados.IOContext
	openContextsMtx sync.Mutex
}

/*
InitRados attempts to create a new Rados connection using the parameters passed
in via flags and, if successful, registers a rados:// URL handler with the
filesystem API.
*/
func InitRados() error {
	var rfs *rados.Conn
	var err error

	if user != nil && *user != "" {
		if cluster != nil && *cluster != "" {
			if rfs, err = rados.NewConnWithClusterAndUser(*cluster, *user); err != nil {
				return fmt.Errorf("NewConnWithClusterAndUser(%s, %s) -> %s",
					*cluster, *user, err.Error())
			}
		} else {
			if rfs, err = rados.NewConnWithUser(*user); err != nil {
				return fmt.Errorf("NewConnWithUser(%s) -> %s", *user, err.Error())
			}
		}
	} else {
		if rfs, err = rados.NewConn(); err != nil {
			return fmt.Errorf("NewConn() -> %s", err.Error())
		}
	}
	return initRadosConnection(rfs, *configPath)
}

/*
RegisterRadosConfig creates a new Rados client based on the configuration file
specified as configPath, and registers it for handling rados:// URLs.

If configPath is left empty, the default configuration path will be used, so
this will have the same effect as the init() initializer.
*/
func RegisterRadosConfig(configPath string) error {
	var rfs *rados.Conn
	var err error

	if rfs, err = rados.NewConn(); err != nil {
		return err
	}

	return initRadosConnection(rfs, configPath)
}

/*
RegisterRadosConfigWithUser creates a new Rados client based on the
configuration file specified as configPath and using the specified user, and
registers it for handling rados:// URLs.

If configPath is left empty, the default configuration path will be used.
*/
func RegisterRadosConfigWithUser(configPath, user string) error {
	var rfs *rados.Conn
	var err error

	if rfs, err = rados.NewConnWithUser(user); err != nil {
		return err
	}

	return initRadosConnection(rfs, configPath)
}

/*
RegisterRadosConfigWithClusterAndUser creates a new Rados client based on the
configuration file specified as configPath and using the specified cluster name
and user, and registers it for handling rados:// URLs.

If configPath is left empty, the default configuration path will be used.
*/
func RegisterRadosConfigWithClusterAndUser(configPath, cluster, user string) error {
	var rfs *rados.Conn
	var err error

	if rfs, err = rados.NewConnWithClusterAndUser(cluster, user); err != nil {
		return err
	}

	return initRadosConnection(rfs, configPath)
}

/*
initRadosConnection does the "lower part" of the Rados Initialization: it parses
the specified configuration file (or the default configuration in case the path
is left empty), reads environment variables, reads command line flags and
attempts to connect to Rados. Upon success, the Rados handler will be
registered.
*/
func initRadosConnection(rfs *rados.Conn, configPath string) error {
	var err error

	if len(configPath) > 0 {
		if err = rfs.ReadConfigFile(configPath); err != nil {
			return fmt.Errorf("ReadConfigFile(%s) -> %s", configPath, err.Error())
		}
	} else {
		if err = rfs.ReadDefaultConfigFile(); err != nil {
			log.Print("Error reading default rados configuration file: ", err)
		}
	}
	if err = rfs.ParseDefaultConfigEnv(); err != nil {
		log.Print("Error parsing default rados configuration environment: ", err)
	}
	if err = rfs.ParseCmdLineArgs(os.Args[1:]); err != nil {
		log.Print("Error parsing rados command line arguments: ", err)
	}
	if err = rfs.Connect(); err != nil {
		log.Print("Error connecting to rados: ", err)
		return err
	}

	filesystem.AddImplementation("rados", &radosFileSystem{
		openContexts: make(map[string]*rados.IOContext),
		rfs:          rfs,
	})
	return nil
}

/*
getContext finds an open Rados I/O context for the specified pool name and
returns it. If no context can be found, it will
*/
func (r *radosFileSystem) getContext(pool string) (*rados.IOContext, error) {
	var ret *rados.IOContext
	var ok bool
	var err error

	r.openContextsMtx.Lock()
	defer r.openContextsMtx.Unlock()

	if ret, ok = r.openContexts[pool]; ok && ret != nil {
		return ret, nil
	}

	if ret, err = r.rfs.OpenIOContext(pool); err != nil {
		return nil, err
	}

	r.openContexts[pool] = ret
	return ret, err
}

/*
OpenReader opens the specified Rados object (u.Path) in the specified pool
(u.Host) for reading starting from offset 0.
TODO: does not respect contexts yet.
*/
func (r *radosFileSystem) OpenReader(ctx context.Context, u *url.URL) (
	filesystem.ReadCloser, error) {
	var rctx *rados.IOContext
	var err error

	if rctx, err = r.getContext(u.Host); err != nil {
		return nil, err
	}

	return NewReadWriteCloser(rctx, u.Path), nil
}

/*
OpenWriter opens the specified Rados object (u.Path) in the specified pool
(u.Host), truncates it to 0 bytes and creates a writer object to write data
to the resulting object.
TODO: does not respect contexts yet.
*/
func (r *radosFileSystem) OpenWriter(ctx context.Context, u *url.URL) (
	filesystem.WriteCloser, error) {
	var rctx *rados.IOContext
	var err error

	rctx, err = r.getContext(u.Host)
	if err != nil {
		return nil, err
	}

	err = rctx.Truncate(u.Path, 0)
	if err != nil {
		return nil, err
	}

	return NewReadWriteCloser(rctx, u.Path), nil
}

/*
OpenAppender opens the specified Rados object (u.Path) in the specified pool
(u.Host) for appending. If the object does not exist yet, it will be created.
TODO: does not respect contexts yet.
*/
func (r *radosFileSystem) OpenAppender(ctx context.Context, u *url.URL) (
	filesystem.WriteCloser, error) {
	var rctx *rados.IOContext
	var err error

	rctx, err = r.getContext(u.Host)
	if err != nil {
		return nil, err
	}

	return NewAppender(rctx, u.Path)
}

/*
ListEntries will find all entries in the Rados pool designated by u.Host which
have the prefix of u.Path. The object ID will be broken up into parts separated
by slashes. Only the part before the next slash is returned.
TODO: does not respect contexts yet.
*/
func (r *radosFileSystem) ListEntries(ctx context.Context, u *url.URL) (
	[]string, error) {
	var rctx *rados.IOContext
	var iter *rados.Iter
	var set = make(map[string]bool)
	var objs = make([]string, 0)
	var prefix = u.Path
	var path string
	var isset bool
	var err error

	rctx, err = r.getContext(u.Host)
	if err != nil {
		return nil, err
	}

	iter, err = rctx.Iter()
	if err != nil {
		return nil, err
	}

	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	for iter.Next() {
		path = iter.Value()
		if path == u.Path {
			var basename = path[strings.LastIndex(path, "/")+1:]
			if len(basename) > 0 {
				set[basename] = true
			}
		}
		if strings.HasPrefix(path, prefix) {
			var fragments []string
			path = path[len(prefix)+1:]
			fragments = strings.SplitN(path, "/", 2)
			if len(fragments) > 0 && len(fragments[0]) > 0 {
				set[fragments[0]] = true
			}
		}
	}

	iter.Close()

	for path, isset = range set {
		if isset {
			objs = append(objs, path)
		}
	}

	return objs, nil
}

/*
WatchFile returns an error because Rados does not provide any functionality for
watching files and cannot do so by design.
*/
func (*radosFileSystem) WatchFile(
	context.Context, *url.URL, filesystem.FileWatchFunc) (
	filesystem.CancelWatchFunc, chan error, error) {
	return nil, nil, filesystem.EUNSUPP
}

/*
Remove deletes the Rados object named u.Path in the pool pointed at by u.Host.
*/
func (r *radosFileSystem) Remove(ctx context.Context, u *url.URL) error {
	var rctx *rados.IOContext
	var err error

	rctx, err = r.getContext(u.Host)
	if err != nil {
		return err
	}

	return rctx.Delete(u.Path)
}
