package database

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gogs.io/gogs/internal/errx"
	"gogs.io/gogs/internal/lfsx"
)

func TestLFS(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	ctx := context.Background()
	s := &LFSStore{
		db: newTestDB(t, "LFSStore"),
	}

	for _, tc := range []struct {
		name string
		test func(t *testing.T, ctx context.Context, s *LFSStore)
	}{
		{"CreateObject", lfsCreateObject},
		{"GetObjectByOID", lfsGetObjectByOID},
		{"GetObjectsByOIDs", lfsGetObjectsByOIDs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				err := clearTables(t, s.db)
				require.NoError(t, err)
			})
			tc.test(t, ctx, s)
		})
		if t.Failed() {
			break
		}
	}
}

func lfsCreateObject(t *testing.T, ctx context.Context, s *LFSStore) {
	// Create first LFS object
	repoID := int64(1)
	oid := lfsx.OID("ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64f")
	err := s.CreateObject(ctx, repoID, oid, 12, lfsx.StorageLocal)
	require.NoError(t, err)

	// Get it back and check the CreatedAt field
	object, err := s.GetObjectByOID(ctx, repoID, oid)
	require.NoError(t, err)
	assert.Equal(t, s.db.NowFunc().Format(time.RFC3339), object.CreatedAt.UTC().Format(time.RFC3339))

	// Try to create second LFS object with same oid should fail
	err = s.CreateObject(ctx, repoID, oid, 12, lfsx.StorageLocal)
	assert.Error(t, err)
}

func lfsGetObjectByOID(t *testing.T, ctx context.Context, s *LFSStore) {
	// Create a LFS object
	repoID := int64(1)
	oid := lfsx.OID("ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64f")
	err := s.CreateObject(ctx, repoID, oid, 12, lfsx.StorageLocal)
	require.NoError(t, err)

	// We should be able to get it back
	_, err = s.GetObjectByOID(ctx, repoID, oid)
	require.NoError(t, err)

	// Try to get a non-existent object
	_, err = s.GetObjectByOID(ctx, repoID, "bad_oid")
	expErr := ErrLFSObjectNotExist{args: errx.Args{"repoID": repoID, "oid": lfsx.OID("bad_oid")}}
	assert.Equal(t, expErr, err)
}

func lfsGetObjectsByOIDs(t *testing.T, ctx context.Context, s *LFSStore) {
	// Create two LFS objects
	repoID := int64(1)
	oid1 := lfsx.OID("ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64f")
	oid2 := lfsx.OID("ef797c8118f02dfb649607dd5d3f8c7623048c9c063d532cc95c5ed7a898a64g")
	err := s.CreateObject(ctx, repoID, oid1, 12, lfsx.StorageLocal)
	require.NoError(t, err)
	err = s.CreateObject(ctx, repoID, oid2, 12, lfsx.StorageLocal)
	require.NoError(t, err)

	// We should be able to get them back and ignore non-existent ones
	objects, err := s.GetObjectsByOIDs(ctx, repoID, oid1, oid2, "bad_oid")
	require.NoError(t, err)
	assert.Equal(t, 2, len(objects), "number of objects")

	assert.Equal(t, repoID, objects[0].RepoID)
	assert.Equal(t, oid1, objects[0].OID)

	assert.Equal(t, repoID, objects[1].RepoID)
	assert.Equal(t, oid2, objects[1].OID)
}

func TestLFSLock(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	ctx := context.Background()
	s := &LFSLockStore{
		db: newTestDB(t, "LFSLockStore"),
	}

	for _, tc := range []struct {
		name string
		test func(t *testing.T, ctx context.Context, s *LFSLockStore)
	}{
		{"CreateLock", lfsLockCreate},
		{"GetLockByID", lfsLockGetByID},
		{"ListLocks", lfsLockList},
		{"DeleteLockByID", lfsLockDeleteByID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				err := clearTables(t, s.db)
				require.NoError(t, err)
			})
			tc.test(t, ctx, s)
		})
		if t.Failed() {
			break
		}
	}
}

func lfsLockCreate(t *testing.T, ctx context.Context, s *LFSLockStore) {
	repoID := int64(1)
	userID := int64(1)

	lock, err := s.CreateLock(ctx, repoID, "path/to/file.bin", userID)
	require.NoError(t, err)
	assert.NotEmpty(t, lock.ID)
	assert.Equal(t, repoID, lock.RepoID)
	assert.Equal(t, "path/to/file.bin", lock.Path)
	assert.Equal(t, userID, lock.UserID)
	assert.False(t, lock.CreatedAt.IsZero())

	// Creating a lock for the same path should return ErrLFSLockAlreadyExist
	// with the existing lock attached.
	_, err = s.CreateLock(ctx, repoID, "path/to/file.bin", userID)
	require.True(t, IsErrLFSLockAlreadyExist(err))
	var conflictErr ErrLFSLockAlreadyExist
	require.ErrorAs(t, err, &conflictErr)
	assert.Equal(t, lock.ID, conflictErr.Lock.ID)
}

func lfsLockGetByID(t *testing.T, ctx context.Context, s *LFSLockStore) {
	repoID := int64(1)
	userID := int64(1)

	lock, err := s.CreateLock(ctx, repoID, "path/to/file.bin", userID)
	require.NoError(t, err)

	got, err := s.GetLockByID(ctx, repoID, lock.ID)
	require.NoError(t, err)
	assert.Equal(t, lock.ID, got.ID)
	assert.Equal(t, lock.Path, got.Path)

	// Non-existent lock
	_, err = s.GetLockByID(ctx, repoID, "nonexistent")
	assert.True(t, IsErrLFSLockNotExist(err))
}

func lfsLockList(t *testing.T, ctx context.Context, s *LFSLockStore) {
	repoID := int64(1)
	userID := int64(1)

	_, err := s.CreateLock(ctx, repoID, "a.bin", userID)
	require.NoError(t, err)
	_, err = s.CreateLock(ctx, repoID, "b.bin", userID)
	require.NoError(t, err)
	_, err = s.CreateLock(ctx, repoID, "c.bin", userID)
	require.NoError(t, err)

	// List all locks
	locks, nextCursor, err := s.ListLocks(ctx, repoID, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, locks, 3)
	assert.Empty(t, nextCursor)

	// Filter by path
	locks, _, err = s.ListLocks(ctx, repoID, "b.bin", "", 0)
	require.NoError(t, err)
	assert.Len(t, locks, 1)
	assert.Equal(t, "b.bin", locks[0].Path)

	// Pagination
	locks, nextCursor, err = s.ListLocks(ctx, repoID, "", "", 2)
	require.NoError(t, err)
	assert.Len(t, locks, 2)
	assert.NotEmpty(t, nextCursor)

	locks, nextCursor, err = s.ListLocks(ctx, repoID, "", nextCursor, 2)
	require.NoError(t, err)
	assert.Len(t, locks, 1)
	assert.Empty(t, nextCursor)
}

func lfsLockDeleteByID(t *testing.T, ctx context.Context, s *LFSLockStore) {
	repoID := int64(1)
	ownerID := int64(1)
	otherID := int64(2)

	lock, err := s.CreateLock(ctx, repoID, "path/to/file.bin", ownerID)
	require.NoError(t, err)

	// Non-owner without force should fail
	_, err = s.DeleteLockByID(ctx, repoID, lock.ID, otherID, false)
	assert.True(t, IsErrLFSLockUnauthorized(err))

	// Owner can delete
	deleted, err := s.DeleteLockByID(ctx, repoID, lock.ID, ownerID, false)
	require.NoError(t, err)
	assert.Equal(t, lock.ID, deleted.ID)

	// Lock should be gone
	_, err = s.GetLockByID(ctx, repoID, lock.ID)
	assert.True(t, IsErrLFSLockNotExist(err))

	// Force delete by non-owner
	lock2, err := s.CreateLock(ctx, repoID, "other.bin", ownerID)
	require.NoError(t, err)
	deleted, err = s.DeleteLockByID(ctx, repoID, lock2.ID, otherID, true)
	require.NoError(t, err)
	assert.Equal(t, lock2.ID, deleted.ID)
}
