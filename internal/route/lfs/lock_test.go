package lfs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/macaron.v1"

	"gogs.io/gogs/internal/database"
)

// mapAuthenticatedUser stores the user in the request context the same way the
// authenticate middleware does, so lock handlers can retrieve it via
// authenticatedUser(c).
func mapAuthenticatedUser(c *macaron.Context, user *database.User) {
	c.Req.Request = c.Req.WithContext(
		context.WithValue(c.Req.Context(), authenticatedUserKey, user),
	)
}

func TestServeLockCreate(t *testing.T) {
	actor := &database.User{ID: 1, Name: "alice"}
	repo := &database.Repository{ID: 10, Name: "repo"}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		body          string
		mockStore     func() *MockStore
		expStatusCode int
		expBody       string
	}{
		{
			name:          "empty path",
			body:          `{}`,
			mockStore:     NewMockStore,
			expStatusCode: http.StatusBadRequest,
			expBody:       `{"message":"Path is required"}` + "\n",
		},
		{
			name: "conflict",
			body: `{"path":"docs/a.psd"}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.CreateLFSLockFunc.SetDefaultReturn(nil, database.ErrLFSLockAlreadyExist{
					Lock: &database.LFSLock{ID: "existing-id", Path: "docs/a.psd", UserID: 1, CreatedAt: now},
				})
				s.GetUserByIDFunc.SetDefaultReturn(actor, nil)
				return s
			},
			expStatusCode: http.StatusConflict,
			expBody:       `{"lock":{"id":"existing-id","path":"docs/a.psd","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"alice"}},"message":"already created lock"}` + "\n",
		},
		{
			name: "success",
			body: `{"path":"docs/a.psd"}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.CreateLFSLockFunc.SetDefaultReturn(&database.LFSLock{
					ID: "new-lock-id", Path: "docs/a.psd", UserID: 1, CreatedAt: now,
				}, nil)
				return s
			},
			expStatusCode: http.StatusCreated,
			expBody:       `{"lock":{"id":"new-lock-id","path":"docs/a.psd","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"alice"}}}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockStore := test.mockStore()
			m := macaron.New()
			m.Use(macaron.Renderer())
			m.Use(func(c *macaron.Context) {
				mapAuthenticatedUser(c, actor)
				c.Map(repo)
			})
			m.Post("/", serveLockCreate(mockStore))

			r, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			m.ServeHTTP(rr, r)

			resp := rr.Result()
			assert.Equal(t, test.expStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, test.expBody, string(body))
		})
	}
}

func TestServeLockList(t *testing.T) {
	repo := &database.Repository{ID: 10, Name: "repo"}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	alice := &database.User{ID: 1, Name: "alice"}

	tests := []struct {
		name          string
		query         string
		mockStore     func() *MockStore
		expStatusCode int
		expBody       string
	}{
		{
			name:  "filter by id not found",
			query: "?id=nonexistent",
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.GetLFSLockByIDFunc.SetDefaultReturn(nil, database.ErrLFSLockNotExist{})
				return s
			},
			expStatusCode: http.StatusOK,
			expBody:       `{"locks":[]}` + "\n",
		},
		{
			name:  "filter by id found",
			query: "?id=lock-1",
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.GetLFSLockByIDFunc.SetDefaultReturn(&database.LFSLock{
					ID: "lock-1", Path: "a.bin", UserID: 1, CreatedAt: now,
				}, nil)
				s.GetUserByIDFunc.SetDefaultReturn(alice, nil)
				return s
			},
			expStatusCode: http.StatusOK,
			expBody:       `{"locks":[{"id":"lock-1","path":"a.bin","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"alice"}}]}` + "\n",
		},
		{
			name:  "empty list",
			query: "",
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.ListLFSLocksFunc.SetDefaultReturn([]*database.LFSLock{}, "", nil)
				return s
			},
			expStatusCode: http.StatusOK,
			expBody:       `{"locks":[]}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockStore := test.mockStore()
			m := macaron.New()
			m.Use(macaron.Renderer())
			m.Use(func(c *macaron.Context) {
				c.Map(repo)
			})
			m.Get("/", serveLockList(mockStore))

			r, err := http.NewRequest(http.MethodGet, "/"+test.query, nil)
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			m.ServeHTTP(rr, r)

			resp := rr.Result()
			assert.Equal(t, test.expStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, test.expBody, string(body))
		})
	}
}

func TestServeLockVerify(t *testing.T) {
	actor := &database.User{ID: 1, Name: "alice"}
	repo := &database.Repository{ID: 10, Name: "repo"}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	bob := &database.User{ID: 2, Name: "bob"}

	tests := []struct {
		name          string
		body          string
		mockStore     func() *MockStore
		expStatusCode int
		expBody       string
	}{
		{
			name: "partitions ours and theirs",
			body: `{}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.ListLFSLocksFunc.SetDefaultReturn([]*database.LFSLock{
					{ID: "lock-1", Path: "mine.bin", UserID: 1, CreatedAt: now},
					{ID: "lock-2", Path: "theirs.bin", UserID: 2, CreatedAt: now},
				}, "", nil)
				s.GetUserByIDFunc.PushReturn(actor, nil)
				s.GetUserByIDFunc.PushReturn(bob, nil)
				return s
			},
			expStatusCode: http.StatusOK,
			expBody:       `{"ours":[{"id":"lock-1","path":"mine.bin","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"alice"}}],"theirs":[{"id":"lock-2","path":"theirs.bin","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"bob"}}]}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockStore := test.mockStore()
			m := macaron.New()
			m.Use(macaron.Renderer())
			m.Use(func(c *macaron.Context) {
				mapAuthenticatedUser(c, actor)
				c.Map(repo)
			})
			m.Post("/", serveLockVerify(mockStore))

			r, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(test.body))
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			m.ServeHTTP(rr, r)

			resp := rr.Result()
			assert.Equal(t, test.expStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, test.expBody, string(body))
		})
	}
}

func TestServeLockDelete(t *testing.T) {
	actor := &database.User{ID: 1, Name: "alice"}
	repo := &database.Repository{ID: 10, Name: "repo"}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		body          string
		mockStore     func() *MockStore
		expStatusCode int
		expBody       string
	}{
		{
			name: "lock not found",
			body: `{}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.DeleteLFSLockByIDFunc.SetDefaultReturn(nil, database.ErrLFSLockNotExist{})
				return s
			},
			expStatusCode: http.StatusNotFound,
			expBody:       `{"message":"Lock not found"}` + "\n",
		},
		{
			name: "unauthorized",
			body: `{}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.DeleteLFSLockByIDFunc.SetDefaultReturn(nil, database.ErrLFSLockUnauthorized{})
				return s
			},
			expStatusCode: http.StatusForbidden,
			expBody:       `{"message":"You must have push access to delete locks you don't own"}` + "\n",
		},
		{
			name: "success",
			body: `{"force":true}`,
			mockStore: func() *MockStore {
				s := NewMockStore()
				s.DeleteLFSLockByIDFunc.SetDefaultReturn(&database.LFSLock{
					ID: "lock-1", Path: "a.bin", UserID: 1, CreatedAt: now,
				}, nil)
				s.GetUserByIDFunc.SetDefaultReturn(actor, nil)
				return s
			},
			expStatusCode: http.StatusOK,
			expBody:       `{"lock":{"id":"lock-1","path":"a.bin","locked_at":"2024-01-01T00:00:00Z","owner":{"name":"alice"}}}` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockStore := test.mockStore()
			m := macaron.New()
			m.Use(macaron.Renderer())
			m.Use(func(c *macaron.Context) {
				mapAuthenticatedUser(c, actor)
				c.Map(repo)
			})
			m.Post("/:id", serveLockDelete(mockStore))

			r, err := http.NewRequest(http.MethodPost, "/lock-1", strings.NewReader(test.body))
			require.NoError(t, err)

			rr := httptest.NewRecorder()
			m.ServeHTTP(rr, r)

			resp := rr.Result()
			assert.Equal(t, test.expStatusCode, resp.StatusCode)

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, test.expBody, string(body))
		})
	}
}
