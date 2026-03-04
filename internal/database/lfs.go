package database

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/oklog/ulid/v2"
	"gorm.io/gorm"

	"gogs.io/gogs/internal/errx"
	"gogs.io/gogs/internal/lfsx"
)

// LFSObject is the relation between an LFS object and a repository.
type LFSObject struct {
	RepoID    int64        `gorm:"primaryKey;auto_increment:false"`
	OID       lfsx.OID     `gorm:"primaryKey;column:oid"`
	Size      int64        `gorm:"not null"`
	Storage   lfsx.Storage `gorm:"not null"`
	CreatedAt time.Time    `gorm:"not null"`
}

// LFSStore is the storage layer for LFS objects.
type LFSStore struct {
	db *gorm.DB
}

func newLFSStore(db *gorm.DB) *LFSStore {
	return &LFSStore{db: db}
}

// CreateObject creates an LFS object record in database.
func (s *LFSStore) CreateObject(ctx context.Context, repoID int64, oid lfsx.OID, size int64, storage lfsx.Storage) error {
	object := &LFSObject{
		RepoID:  repoID,
		OID:     oid,
		Size:    size,
		Storage: storage,
	}
	return s.db.WithContext(ctx).Create(object).Error
}

type ErrLFSObjectNotExist struct {
	args errx.Args
}

func IsErrLFSObjectNotExist(err error) bool {
	return errors.As(err, &ErrLFSObjectNotExist{})
}

func (err ErrLFSObjectNotExist) Error() string {
	return fmt.Sprintf("LFS object does not exist: %v", err.args)
}

func (ErrLFSObjectNotExist) NotFound() bool {
	return true
}

// GetObjectByOID returns the LFS object with given OID. It returns
// ErrLFSObjectNotExist when not found.
func (s *LFSStore) GetObjectByOID(ctx context.Context, repoID int64, oid lfsx.OID) (*LFSObject, error) {
	object := new(LFSObject)
	err := s.db.WithContext(ctx).Where("repo_id = ? AND oid = ?", repoID, oid).First(object).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrLFSObjectNotExist{args: errx.Args{"repoID": repoID, "oid": oid}}
		}
		return nil, err
	}
	return object, err
}

// GetObjectsByOIDs returns LFS objects found within "oids". The returned list
// could have fewer elements if some oids were not found.
func (s *LFSStore) GetObjectsByOIDs(ctx context.Context, repoID int64, oids ...lfsx.OID) ([]*LFSObject, error) {
	if len(oids) == 0 {
		return []*LFSObject{}, nil
	}

	objects := make([]*LFSObject, 0, len(oids))
	err := s.db.WithContext(ctx).Where("repo_id = ? AND oid IN (?)", repoID, oids).Find(&objects).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	return objects, nil
}

// LFSLock represents a file lock in a repository.
type LFSLock struct {
	ID        string    `gorm:"primaryKey;size:26"`
	RepoID    int64     `gorm:"uniqueIndex:idx_lfs_lock_repo_path;not null"`
	Path      string    `gorm:"uniqueIndex:idx_lfs_lock_repo_path;not null"`
	UserID    int64     `gorm:"not null"`
	CreatedAt time.Time `gorm:"not null"`
}

// ErrLFSLockAlreadyExist is returned when a lock already exists for the given
// path in the repository.
type ErrLFSLockAlreadyExist struct {
	args errx.Args
	Lock *LFSLock
}

// IsErrLFSLockAlreadyExist returns true if the error indicates a lock already
// exists.
func IsErrLFSLockAlreadyExist(err error) bool {
	return errors.As(err, &ErrLFSLockAlreadyExist{})
}

// Error returns the error message.
func (err ErrLFSLockAlreadyExist) Error() string {
	return fmt.Sprintf("LFS lock already exists: %v", err.args)
}

// ErrLFSLockNotExist is returned when a lock does not exist.
type ErrLFSLockNotExist struct {
	args errx.Args
}

// IsErrLFSLockNotExist returns true if the error indicates a lock does not
// exist.
func IsErrLFSLockNotExist(err error) bool {
	return errors.As(err, &ErrLFSLockNotExist{})
}

// Error returns the error message.
func (err ErrLFSLockNotExist) Error() string {
	return fmt.Sprintf("LFS lock does not exist: %v", err.args)
}

// NotFound returns true to indicate this is a not-found error.
func (ErrLFSLockNotExist) NotFound() bool {
	return true
}

// ErrLFSLockUnauthorized is returned when a user tries to unlock a lock owned
// by another user without force.
type ErrLFSLockUnauthorized struct {
	args errx.Args
}

// IsErrLFSLockUnauthorized returns true if the error indicates the user is not
// authorized to unlock the lock.
func IsErrLFSLockUnauthorized(err error) bool {
	return errors.As(err, &ErrLFSLockUnauthorized{})
}

// Error returns the error message.
func (err ErrLFSLockUnauthorized) Error() string {
	return fmt.Sprintf("LFS lock unauthorized: %v", err.args)
}

// LFSLockStore is the storage layer for LFS file locks.
type LFSLockStore struct {
	db *gorm.DB
}

func newLFSLockStore(db *gorm.DB) *LFSLockStore {
	return &LFSLockStore{db: db}
}

// CreateLock creates a file lock for the given path in a repository. It returns
// ErrLFSLockAlreadyExist if a lock already exists for the path.
func (s *LFSLockStore) CreateLock(ctx context.Context, repoID int64, path string, userID int64) (*LFSLock, error) {
	lock := &LFSLock{
		ID:     ulid.MustNew(ulid.Timestamp(time.Now()), cryptorand.Reader).String(),
		RepoID: repoID,
		Path:   path,
		UserID: userID,
	}

	err := s.db.WithContext(ctx).Create(lock).Error
	if err != nil {
		// On unique constraint violation, look up the existing lock to return
		// it in the error. This avoids the race window of a check-then-insert.
		existing := new(LFSLock)
		if dbErr := s.db.WithContext(ctx).Where("repo_id = ? AND path = ?", repoID, path).First(existing).Error; dbErr == nil {
			return nil, ErrLFSLockAlreadyExist{
				args: errx.Args{"repoID": repoID, "path": path},
				Lock: existing,
			}
		}
		return nil, err
	}
	return lock, nil
}

// GetLockByID returns the lock with the given ID in the repository. It returns
// ErrLFSLockNotExist when not found.
func (s *LFSLockStore) GetLockByID(ctx context.Context, repoID int64, lockID string) (*LFSLock, error) {
	lock := new(LFSLock)
	err := s.db.WithContext(ctx).Where("repo_id = ? AND id = ?", repoID, lockID).First(lock).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrLFSLockNotExist{args: errx.Args{"repoID": repoID, "id": lockID}}
		}
		return nil, err
	}
	return lock, nil
}

// ListLocks returns locks for the given repository with optional path filter
// and cursor-based pagination. It returns the locks and the next cursor value
// (empty string if no more results).
func (s *LFSLockStore) ListLocks(ctx context.Context, repoID int64, path, cursor string, limit int) ([]*LFSLock, string, error) {
	if limit <= 0 {
		limit = 100
	}

	query := s.db.WithContext(ctx).Where("repo_id = ?", repoID)
	if path != "" {
		query = query.Where("path = ?", path)
	}
	if cursor != "" {
		query = query.Where("id > ?", cursor)
	}

	// Fetch one extra to determine if there's a next page.
	locks := make([]*LFSLock, 0, limit+1)
	err := query.Order("id ASC").Limit(limit + 1).Find(&locks).Error
	if err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(locks) > limit {
		nextCursor = locks[limit-1].ID
		locks = locks[:limit]
	}
	return locks, nextCursor, nil
}

// DeleteLockByID deletes a lock by ID. When force is false, only the lock owner
// can delete it, otherwise ErrLFSLockUnauthorized is returned. It returns
// ErrLFSLockNotExist when the lock is not found.
func (s *LFSLockStore) DeleteLockByID(ctx context.Context, repoID int64, lockID string, userID int64, force bool) (*LFSLock, error) {
	lock, err := s.GetLockByID(ctx, repoID, lockID)
	if err != nil {
		return nil, err
	}

	if !force && lock.UserID != userID {
		return nil, ErrLFSLockUnauthorized{args: errx.Args{"lockID": lockID, "ownerID": lock.UserID, "actorID": userID}}
	}

	err = s.db.WithContext(ctx).Delete(lock).Error
	if err != nil {
		return nil, err
	}
	return lock, nil
}
