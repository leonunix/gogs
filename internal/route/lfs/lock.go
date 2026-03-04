package lfs

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"gopkg.in/macaron.v1"
	log "unknwon.dev/clog/v2"

	"gogs.io/gogs/internal/database"
	"gogs.io/gogs/internal/strx"
)

// lockResponse is the JSON representation of a lock returned by the API.
type lockResponse struct {
	ID       string        `json:"id"`
	Path     string        `json:"path"`
	LockedAt time.Time     `json:"locked_at"`
	Owner    lockOwnerResp `json:"owner"`
}

type lockOwnerResp struct {
	Name string `json:"name"`
}

// serveLockCreate handles POST /locks to create a new file lock.
func serveLockCreate(store Store) macaron.Handler {
	return func(c *macaron.Context, repo *database.Repository) {
		actor := authenticatedUser(c)

		var request struct {
			Path string `json:"path"`
			Ref  *struct {
				Name string `json:"name"`
			} `json:"ref,omitempty"`
		}
		defer func() { _ = c.Req.Request.Body.Close() }()
		err := json.NewDecoder(c.Req.Request.Body).Decode(&request)
		if err != nil {
			responseJSON(c.Resp, http.StatusBadRequest, responseError{
				Message: strx.ToUpperFirst(err.Error()),
			})
			return
		}

		if request.Path == "" {
			responseJSON(c.Resp, http.StatusBadRequest, responseError{
				Message: "Path is required",
			})
			return
		}

		lock, err := store.CreateLFSLock(c.Req.Context(), repo.ID, request.Path, actor.ID)
		if err != nil {
			var conflictErr database.ErrLFSLockAlreadyExist
			if errors.As(err, &conflictErr) {
				ownerName := resolveUserName(store, c, conflictErr.Lock.UserID)
				responseJSON(c.Resp, http.StatusConflict, lockCreateConflictResponse{
					Lock:    toLockResponse(conflictErr.Lock, ownerName),
					Message: "already created lock",
				})
				return
			}
			internalServerError(c.Resp)
			log.Error("Failed to create LFS lock [repo_id: %d, path: %s]: %v", repo.ID, request.Path, err)
			return
		}

		responseJSON(c.Resp, http.StatusCreated, lockCreateResponse{
			Lock: toLockResponse(lock, actor.Name),
		})
	}
}

type lockCreateResponse struct {
	Lock lockResponse `json:"lock"`
}

type lockCreateConflictResponse struct {
	Lock    lockResponse `json:"lock"`
	Message string       `json:"message"`
}

// serveLockList handles GET /locks to list file locks.
func serveLockList(store Store) macaron.Handler {
	return func(c *macaron.Context, repo *database.Repository) {
		path := c.Query("path")
		id := c.Query("id")
		cursor := c.Query("cursor")
		limit, _ := strconv.Atoi(c.Query("limit"))

		// If filtering by ID, return at most one lock.
		if id != "" {
			lock, err := store.GetLFSLockByID(c.Req.Context(), repo.ID, id)
			if err != nil {
				if database.IsErrLFSLockNotExist(err) {
					responseJSON(c.Resp, http.StatusOK, lockListResponse{
						Locks: []lockResponse{},
					})
					return
				}
				internalServerError(c.Resp)
				log.Error("Failed to get LFS lock [repo_id: %d, id: %s]: %v", repo.ID, id, err)
				return
			}
			ownerName := resolveUserName(store, c, lock.UserID)
			responseJSON(c.Resp, http.StatusOK, lockListResponse{
				Locks: []lockResponse{toLockResponse(lock, ownerName)},
			})
			return
		}

		locks, nextCursor, err := store.ListLFSLocks(c.Req.Context(), repo.ID, path, cursor, limit)
		if err != nil {
			internalServerError(c.Resp)
			log.Error("Failed to list LFS locks [repo_id: %d]: %v", repo.ID, err)
			return
		}

		resp := lockListResponse{
			Locks:      make([]lockResponse, 0, len(locks)),
			NextCursor: nextCursor,
		}
		for _, lock := range locks {
			ownerName := resolveUserName(store, c, lock.UserID)
			resp.Locks = append(resp.Locks, toLockResponse(lock, ownerName))
		}
		responseJSON(c.Resp, http.StatusOK, resp)
	}
}

type lockListResponse struct {
	Locks      []lockResponse `json:"locks"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// serveLockVerify handles POST /locks/verify to list locks partitioned by
// ownership.
func serveLockVerify(store Store) macaron.Handler {
	return func(c *macaron.Context, repo *database.Repository) {
		actor := authenticatedUser(c)
		var request struct {
			Ref *struct {
				Name string `json:"name"`
			} `json:"ref,omitempty"`
			Cursor string `json:"cursor,omitempty"`
			Limit  int    `json:"limit,omitempty"`
		}
		defer func() { _ = c.Req.Request.Body.Close() }()
		err := json.NewDecoder(c.Req.Request.Body).Decode(&request)
		if err != nil {
			responseJSON(c.Resp, http.StatusBadRequest, responseError{
				Message: strx.ToUpperFirst(err.Error()),
			})
			return
		}

		locks, nextCursor, err := store.ListLFSLocks(c.Req.Context(), repo.ID, "", request.Cursor, request.Limit)
		if err != nil {
			internalServerError(c.Resp)
			log.Error("Failed to list LFS locks for verification [repo_id: %d]: %v", repo.ID, err)
			return
		}

		resp := lockVerifyResponse{
			Ours:       make([]lockResponse, 0),
			Theirs:     make([]lockResponse, 0),
			NextCursor: nextCursor,
		}
		for _, lock := range locks {
			ownerName := resolveUserName(store, c, lock.UserID)
			lr := toLockResponse(lock, ownerName)
			if lock.UserID == actor.ID {
				resp.Ours = append(resp.Ours, lr)
			} else {
				resp.Theirs = append(resp.Theirs, lr)
			}
		}
		responseJSON(c.Resp, http.StatusOK, resp)
	}
}

type lockVerifyResponse struct {
	Ours       []lockResponse `json:"ours"`
	Theirs     []lockResponse `json:"theirs"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

// serveLockDelete handles POST /locks/:id/unlock to delete a file lock.
func serveLockDelete(store Store) macaron.Handler {
	return func(c *macaron.Context, repo *database.Repository) {
		actor := authenticatedUser(c)
		var request struct {
			Force bool `json:"force,omitempty"`
			Ref   *struct {
				Name string `json:"name"`
			} `json:"ref,omitempty"`
		}
		defer func() { _ = c.Req.Request.Body.Close() }()
		err := json.NewDecoder(c.Req.Request.Body).Decode(&request)
		if err != nil {
			responseJSON(c.Resp, http.StatusBadRequest, responseError{
				Message: strx.ToUpperFirst(err.Error()),
			})
			return
		}

		lockID := c.Params(":id")
		lock, err := store.DeleteLFSLockByID(c.Req.Context(), repo.ID, lockID, actor.ID, request.Force)
		if err != nil {
			if database.IsErrLFSLockNotExist(err) {
				responseJSON(c.Resp, http.StatusNotFound, responseError{
					Message: "Lock not found",
				})
				return
			}
			if database.IsErrLFSLockUnauthorized(err) {
				responseJSON(c.Resp, http.StatusForbidden, responseError{
					Message: "You must have push access to delete locks you don't own",
				})
				return
			}
			internalServerError(c.Resp)
			log.Error("Failed to delete LFS lock [repo_id: %d, lock_id: %s]: %v", repo.ID, lockID, err)
			return
		}

		ownerName := resolveUserName(store, c, lock.UserID)
		responseJSON(c.Resp, http.StatusOK, lockCreateResponse{
			Lock: toLockResponse(lock, ownerName),
		})
	}
}

func toLockResponse(lock *database.LFSLock, ownerName string) lockResponse {
	return lockResponse{
		ID:       lock.ID,
		Path:     lock.Path,
		LockedAt: lock.CreatedAt,
		Owner:    lockOwnerResp{Name: ownerName},
	}
}

func resolveUserName(store Store, c *macaron.Context, userID int64) string {
	user, err := store.GetUserByID(c.Req.Context(), userID)
	if err != nil {
		log.Error("Failed to get user [id: %d]: %v", userID, err)
		return ""
	}
	return user.Name
}
