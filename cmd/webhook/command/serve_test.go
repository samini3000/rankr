package command

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/gocasters/rankr/webhookapp"
	nc "github.com/nats-io/nats.go"
)

type fakeJetStreamManager struct {
	info      *nc.StreamInfo
	infoErr   error
	added     *nc.StreamConfig
	updated   *nc.StreamConfig
	addErr    error
	updateErr error
}

func (f *fakeJetStreamManager) StreamInfo(string, ...nc.JSOpt) (*nc.StreamInfo, error) {
	return f.info, f.infoErr
}

func (f *fakeJetStreamManager) AddStream(cfg *nc.StreamConfig, _ ...nc.JSOpt) (*nc.StreamInfo, error) {
	f.added = cfg
	if f.addErr != nil {
		return nil, f.addErr
	}
	return &nc.StreamInfo{Config: *cfg}, nil
}

func (f *fakeJetStreamManager) UpdateStream(cfg *nc.StreamConfig, _ ...nc.JSOpt) (*nc.StreamInfo, error) {
	f.updated = cfg
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return &nc.StreamInfo{Config: *cfg}, nil
}

func TestEnsureEventStream(t *testing.T) {
	cfg := webhookapp.NATSStreamConfig{
		Name:     "EVENTS_STREAM",
		Subjects: []string{"github.*", "rankr_raw_events"},
		MaxBytes: 1 << 30,
		MaxAge:   72 * time.Hour,
		MaxMsgs:  1_000_000,
	}

	t.Run("creates a missing stream with the configured limits", func(t *testing.T) {
		js := &fakeJetStreamManager{infoErr: nc.ErrStreamNotFound}

		if err := ensureEventStream(js, cfg); err != nil {
			t.Fatalf("ensureEventStream() error = %v", err)
		}
		if js.added == nil {
			t.Fatal("expected AddStream to be called")
		}
		if js.updated != nil {
			t.Fatal("expected UpdateStream not to be called")
		}
		assertStreamConfig(t, js.added, cfg, nc.FileStorage)
	})

	t.Run("updates an existing stream and keeps its storage type", func(t *testing.T) {
		js := &fakeJetStreamManager{
			info: &nc.StreamInfo{Config: nc.StreamConfig{Name: cfg.Name, Storage: nc.MemoryStorage}},
		}

		if err := ensureEventStream(js, cfg); err != nil {
			t.Fatalf("ensureEventStream() error = %v", err)
		}
		if js.updated == nil {
			t.Fatal("expected UpdateStream to be called")
		}
		if js.added != nil {
			t.Fatal("expected AddStream not to be called")
		}
		assertStreamConfig(t, js.updated, cfg, nc.MemoryStorage)
	})

	t.Run("returns StreamInfo errors without trying to create the stream", func(t *testing.T) {
		wantErr := errors.New("info failed")
		js := &fakeJetStreamManager{infoErr: wantErr}

		if err := ensureEventStream(js, cfg); !errors.Is(err, wantErr) {
			t.Fatalf("ensureEventStream() error = %v, want %v", err, wantErr)
		}
		if js.added != nil {
			t.Fatal("expected AddStream not to be called")
		}
	})

	t.Run("returns AddStream errors", func(t *testing.T) {
		wantErr := errors.New("add failed")
		js := &fakeJetStreamManager{infoErr: nc.ErrStreamNotFound, addErr: wantErr}

		if err := ensureEventStream(js, cfg); !errors.Is(err, wantErr) {
			t.Fatalf("ensureEventStream() error = %v, want %v", err, wantErr)
		}
	})

	t.Run("returns UpdateStream errors", func(t *testing.T) {
		wantErr := errors.New("update failed")
		js := &fakeJetStreamManager{info: &nc.StreamInfo{}, updateErr: wantErr}

		if err := ensureEventStream(js, cfg); !errors.Is(err, wantErr) {
			t.Fatalf("ensureEventStream() error = %v, want %v", err, wantErr)
		}
	})
}

func assertStreamConfig(t *testing.T, got *nc.StreamConfig, want webhookapp.NATSStreamConfig, wantStorage nc.StorageType) {
	t.Helper()

	if got.Name != want.Name {
		t.Errorf("Name = %q, want %q", got.Name, want.Name)
	}
	if !slices.Equal(got.Subjects, want.Subjects) {
		t.Errorf("Subjects = %v, want %v", got.Subjects, want.Subjects)
	}
	if got.MaxBytes != want.MaxBytes {
		t.Errorf("MaxBytes = %d, want %d", got.MaxBytes, want.MaxBytes)
	}
	if got.MaxAge != want.MaxAge {
		t.Errorf("MaxAge = %v, want %v", got.MaxAge, want.MaxAge)
	}
	if got.MaxMsgs != want.MaxMsgs {
		t.Errorf("MaxMsgs = %d, want %d", got.MaxMsgs, want.MaxMsgs)
	}
	if got.Storage != wantStorage {
		t.Errorf("Storage = %v, want %v", got.Storage, wantStorage)
	}
	if got.Retention != nc.LimitsPolicy {
		t.Errorf("Retention = %v, want %v", got.Retention, nc.LimitsPolicy)
	}
}
