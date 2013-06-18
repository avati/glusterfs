# File Snapshots and clones #

  This translator implements file snapshotting feature. It provides the
ability to take read-only point-in-time snapshots of a given file, and
also the ability to create a new read/writeable file using such a
snapshot as the backing file or base (i.e, create a CoW clone).

  This translator can be enabled on a pre-existing volume. Once enabled
and snapshots are taken, those files (and clones) may NOT be accessible
normally if file snapshotting feature is disabled in the future. Steps
for reverting the snapshot feature will be covered later.


## Terminology ##

### Normal File ###

A regular file which was created in a normal way. No snapshots were ever
taken on this so far. This file exists in the user visible namespace.

### Snapshotted File ###

A file which has at least one snapshot of it taken in the past. It can
also be a CoW clone created from a snapshot. This is a user visible file.

### Snapshot Root ###

The first snapshot taken on a normal file, thereby converting it into
a snapshotted file.

### Snapshot Image ###

A point-in-time, read-only image of a file. Taking a snapshot results in
the creation of such an image. Snapshots can be taken on both normal
files as well as snapshotted files. A snapshot image is a logical concept,
which comprises of a backing image and a delta file. The backing image
can either be a snapshot root image or in turn another snapshot image.

### Head File ###

The read-write file which absorbs writes and modifications to a
snapshotted file. Every user-visible snapshotted file has an associated
head file at a given time, which holds the changes being performed on
the file until the next snapshot is taken.

### Delta File ###

The difference between a snapshotted file (or any snapshot image) and
its corresponding backing image is stored in the delta file. In the
context of a snapshotted file, the read-write head file is the delta
file. For a snapshotted image the delta file is read only. When a
new snapshot is taken on an already snapshotted file, the current
head file becomes the next delta file, and a new empty head file is
created to start absorbing further writes and modifications.

Effectively, the delta file (or the head file) stores all the dirty
blocks (i.e blocks which underwent modification while the delta
file was (or is) the head file) at the offsets which match the true
offsets in the logical file.

### Snapshot Family ###

Every snapshot image has an associated delta file and a backing snapshot
image. Every user-visible snapshotted file has an associated head file
and a backing image. The backing image can be either a snapshot root or
yet another snapshot image. The set of all such delta files, head files
originating from a common snapshot root ancestor, all belong to the same
snapshot family.

A snapshot family is identified by the GFID of the normal file from which
the first snapshot was taken.

## File Components ##

### File size ###

The size of a snapshotted file or a snapshot image is the size of the
corresponding head file or delta file. When a snapshot is taken, a new
head file is created and truncate(2)ed to the size of the previous head
file (i.e, taking a snapshot does not change the size of the file). All
further extending writes and truncate(2)s are performed on the head file
which in turn result in the proper modification of EOF / file size.

### Data blocks ###

A filesystem (like XFS, EXT3/4 etc.) store file data in blocks. The size
of this allocation block can be queried at run-time in a filesystem
specific manner. Blocks are typically sized as a power of two bytes, and
the logical file is split at these block boundaries and mapped to physical
blocks on the disk.

As a simplistic view, the handling of data request in a logical (snapshotted)
file can be seen as the handling a data block, and extend it to any arbitrary
range with special handling of possible partial blocks at the start and end
of the request.

A given data block range in a logical file can be in any of the following
states  -

#### Hole ####

This region was neither written to nor allocated in the past. The logical
region of the file is not consume physical disk space.

* Reading a hole fills the buffer with zeros.

* Writing to a hole region will result in allocation of disk space from the
  free space pool (and can potentially fail if there is not enough space.)

#### Normal ####

This region has been written to in the past and has a specific location
assigned to it on the disk which is holding that data.

* Reading a normal block fills the buffer with the data which has been written
  in it.

* Writing to a normal block will over write the existing data on the same
  physical location on the disk. Writing to a normal region is guaranteed not
  to fail because of lack of free space.

#### Unwritten ####

This region was allocated with fallocate(), but has not been written to.

* Reading an unwritten block will fill the buffer with zeros.

* Writing to an unwritten block is guaranteed not to fail because of lack
  of free space.

#### Querying state of a data block ####

Linux allows userspace applications to query the state of a logical block
of a file using the FS_IOC_FIEMAP ioctl. The ioctl returns information
of extents (multiple adjacent blocks represented as a range). The state
of the extent which contains the block is the state of the block.

## Operations ##

The broad design of how snapshotting is achieved using sparse files
and FIEMAP is this way: Each snapshot has a sparse file associated with
it, called the delta file (or the head file.) The delta file holds
the "modifications" performed which are part of the snapshot image
since the previous snapshot was taken. Writes are performed on the
head file as-is (at the same offset, of the same size, with the
same data).

This "dirtying" is implicitly now represented in the FIEMAP making this
block a NORMAL block. This means reads requests against this snapshot
for this block must be served by reading the block from the head file.

However, if the read request lands on a block which is a HOLE in the
head or delta file, it only means that the block was not modified
in the snapshot (relative to the backing image) -- and NOT that the
logical file has a hole in that region. In such cases, a recursive
logical read must be performed on the backing image.


### Snapshot create ###

Snapshot creation works differently based on whether the file is a normal
file or a snapshotted file.

#### Snapshot on a normal file ####

If the file is normal, a new snapshot family is first created. A snapshot
family is identified by the GFID of the regular file, and represented
as a sub-directory named by the snapshot within the hidden directory
/.snap-family in the root of the volume.

e.g. /.snap-family/77c18542-b21e-4739-be57-29a543547310.

The root snapshot image is created as a hardlink to the normal file
within the family directory, by the name "root".

A new head file is created as a sparse file matching the size of the
normal file using the truncate(2) system call. The name of the head file
is the GFID of the user visible snapshot file it represents, prefixed
by the string "head-". So the first head file created will have the
same name as the snapshot family.

The snapshot family is stored in the xattr of the user visible file
to indicate that it is now a snapshotted file. The key of the xattr
to store the xattr is "snap-family"

The size of the normal file is recorded in the xattr of the head file
with the key "backing-limit". The backing-limit in a delta file or
head file represents the offset after which the state of the head
or delta file represents the state of the logical file (i.e. backing
files will not be referred in any way to serve read requests on
the logical file for offsets greater than this limit)

#### Snapshot on a snapshotted file ####

If the file is already a snapshotted file, then it already has a
snapshot family associted with it, along with root and head files.

The head file of the snapshotted file (which currently has the name
as the GFID of the user visible file) is renamed as the name of the
snapshot (with the string "snap-" prefixed to the snapshot name.)

A new head file is created as a sparse file matching the size of the
previous head file using the truncate(2) system call.

#### Snapshot Create Pseudocode ####

    SNAPSHOT_CREATE (Filename, Snapname) {
        Gfid = GET_XATTR (Filename, "GFID")
        SnapFamily = GET_XATTR (Filename, "snap-family");

        if (SnapFamily == NULL) {
            //First time snapshot on normal file

            SnapFamily = GFID

            MKDIR ("/.snap-family/" + SnapFamily)

            HARDLINK (Filename, "/.snap-family/" + SnapFamily + "/root")

            Size = GET_SIZE (Filename)

            HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

            CREATE (HeadFile)

            TRUNCATE (HeadFile, Size)

            SET_XATTR (HeadFile, "backing-limit", Size)

            SET_XATTR (HeadFile, "backing-image", "root")

            SET_XATTR (Filename, "snap-family", SnapFamily)

        } else {
            //Snapshotted file

            HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

            DeltaFile = "/.snap-family/" + SnapFamily + "/snap-" + SnapName

            Size = GET_SIZE (HeadFile)

            RENAME (HeadFile, SnapName)

            CREATE (HeadFile) // New

            TRUNCATE (HeadFile, Size)

            SET_XATTR (HeadFile, "backing-limit", Size)

            SET_XATTR (HeadFile, "backing-image", SnapName)
        }
    }

### Truncate ###

Truncate on a snapshotted file is accomplished by performing the same
truncate operation (with the same offset) on the head file. If the
truncated size is smaller than the "backing-limit" recorded in the
headfile, then the xattr is updated to reflect the new offset.

This is because the truncate operation effectively creates a hole
from the offset till infinity in the logical file, effectively
making that region "dirty".

#### Truncate Pseudocode ####

    TRUNCATE (Filename, Offset) {
        SnapFamily = GET_XATTR (Filename, "snap-family")

        if (SnapFamily == NULL) {

            // Normal file, pass through
            TRUNCATE (Filename, Offset)

        } else {

            Gfid = GET_XATTR (Filename, "GFID")

            HeadFile = "/.snap-family/" + SnapFamily + "/head-" Gfid

            BackingLimit = GET_XATTR (HeadFile, "backing-limit")

            TRUNCATE (HeadFile, Offset)

            if (Offset < BackingLimit) {

                BackingLimit = Offset

                SET_XATTR (HeadFile, "backing-limit", Offset)
            }
        }
    }

### Read ###

A read request is conceptually broken down into multiple requests for each
of the block the request region overlaps with. Either the starting block or
last block may be partially requested if the offset and/or size are unaligned.
In this case we re-align the request offset and size to match block boundary
and ignore the extra regions while completing the request.

A read request on a given logical block in the snapshotted file is handled
according to the state of the block in the head file and all backing files.

#### Read Pseudocode ####

    READ_BLOCK (File, Offset, Buf) {
        SnapFamily = GET_XATTR (File, "snap-family")

        if (SnapFamily == NULL) {
            return SYS_READ (File, Offset, Buf)
        }

        HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

        BackingLimit = GET_XATTR (HeadFile, "backing-limit")

        if (Offset >= BackingLimit) {
            return SYS_READ (HeadFile, Offset, Buf)
        }

        BlockType = QueryBlock (HeadFile, Offset)

        if (BlockType == NORMAL || BlockType == HOLE)
            return SYS_READ (HeadFile, Offset, Buf)

        if (BlockType == UNWRITTEN) {

            BackingFile = GET_XATTR (HeadFile, "backing-image")

            return READ_BLOCK (BackingFile, Offset, Buf)
        }
    }


### Write ###

The write call is mostly straight forward application of the call
on the head file, except in the cases of unaligned write requests.

For unaligned write requests, we perform a READ_BLOCK(), <modify>,
WRITE_BLOCK() sequence.

#### Write Pseudocode ####

    WRITE_BLOCK (File, Offset, Buf) {
        SnapFamily = GET_XATTR (File, "snap-family")

        if (SnapFamily == NULL) {
            // Normal file
            return SYS_WRITE (File, Offset, Buf)
        }

        HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

        return SYS_WRITE (File, Offset, Buf)
    }


### Fallocate ###

An fallocate() call MUST NOT have any effect on the readability of the
region/block. I.e, no matter what the status of a block was before
calling fallocate() on it (HOLE, NORMAL or UNWRITTEN), the data which
was obtained from read() must be the same data obtained from read()
after fallocate() is performed.

This is the reason why, in the READ_BLOCK() code, it is important to
pass-through the read request to the backing image when UNWRITTEN is
encountered.

#### Fallocate Pseudocode ####

    FALLOCATE_BLOCK (File, Offset, Buf) {
        SnapFamily = GET_XATTR (File, "snap-family")

        if (SnapFamily == NULL) {
            // Normal file
            return SYS_FALLOCATE (File, Offset, Buf)
        }

        HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

        return SYS_FALLOCATE (File, Offset, Buf)
    }

### Discard ###

It is important to note that DISCARD semantics (i.e fallocate(PUNCH_HOLE))
is different on snapshotted files compared to normal files. In a normal
file a DISCARD request deallocates the space, loses data in the discarded
region and future READs on the region return zeros.

However a DISCARD on a snapshotted file deallocates the space, loses data
in the discarded region, and future READs on the region returns data from
the *previous snapshot*.

It is however possible to provide DISCARD semantics like a normal file
with a minor extension - of recording DISCARD ranges in the xattr. Such
recorded xattr ranges must be referred when a HOLE is encountered, and
if the region was a DISCARDed region, then instead of passing through
to the backing image, we must return zeroes. Since this semantics is
NOT REQUIRED for a virtual machine use case (because drives can choose
to implement Non Deterministic TRIM - where there is no guarantee what
data is returned when READ is done after TRIM), the initial implementation
will not provide this extension.


#### Discard Pseudocode ####

    DISCARD_BLOCK (File, Offset, Buf) {
        SnapFamily = GET_XATTR (File, "snap-family")

        if (SnapFamily == NULL) {
            // Normal file
            return SYS_DISCARD (File, Offset, Buf)
        }

        HeadFile = "/.snap-family/" + SnapFamily + "/head-" + GFID

        return SYS_DISCARD (File, Offset, Buf)
    }

## Footnotes ##

* XFS speculative EOF preallocation which shows UNWRITTEN extents as
  NORMAL (because it actually writes zeroes instead of placing UNWRITTEN
  extent) is not a problem for snapshots because such preallocation
  only happens on extending writes, which are always beyond the "backing-limit"
