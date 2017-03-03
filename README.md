Rados implementation for the filesystem API
===========================================

filesystem-rados implements Rados support for the filesystem API (as laid out
in <https://github.com/childoftheuniverse/filesystem>). This should allow
transparent access to some of the more basic Rados functions using the
filesystem API.

Underlying this implementation is the golang Rados API as provided by the
Ceph project in <https://github.com/ceph/go-ceph/rados>.

Initialization
--------------

There are 2 different ways to initialize the Rados implementation of the
filesystem API. The first way is to anonymously load the filesystem-rados
code and use the auto-registered default Rados client:

> import (
>   _ "github.com/childoftheuniverse/filesystem-rados"
> )

If Initialization is successful, this will register a handler for rados:// URLs
which can then be accessed using the filesytem API. If Initialization fails,
an error will be logged to the console.

If Initialization is supposed to happen in a more controlled and reliable way,
explicit registration must be used. The RegisterRadosConfig(),
RegisterRadosConfigWithUser() and RegisterRadosConfigWithClusterAndUser()
functions can be used towards this goal; they will indicate success or failure
of the Rados setup more cleanly.

Bugs
----

The Rados implementation currently has the following known shortcomings:

 - Context is not supported (timeouts and cancellations are not honored)
