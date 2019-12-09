/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

const testRef = "testRef"

// TestPreparation tests functionality to prepare snapshots using image's basic
// information.
func TestPreparation(t *testing.T) {
	tests := []struct {
		name       string
		preparable []bool
	}{
		{
			name:       "all_non_preparable",
			preparable: []bool{false, false, false},
		},
		{
			name:       "all_preparable",
			preparable: []bool{true, true, true},
		},
		{
			name:       "lower_layer_preparable",
			preparable: []bool{true, true, false},
		},
		{
			name:       "upper_layer_preparable",
			preparable: []bool{false, true, false},
		},
		{
			name:       "partially_preparable_1",
			preparable: []bool{true, false, true, false},
		},
		{
			name:       "partially_preparable_2",
			preparable: []bool{false, true, false, true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				preparable = make(map[string]bool)
				inDesc     []ocispec.Descriptor
				inChain    []digest.Digest
			)
			for i, e := range tt.preparable {
				desc := descriptor(i)
				inDesc = append(inDesc, desc)
				inChain = append(inChain, digest.Digest(fmt.Sprintf("%d-chain", i)))
				if e {
					preparable[desc.Digest.String()] = true
					t.Logf("  preparable: %v", desc.Digest.String())
				}
			}
			sn := &testSnapshotter{
				t:          t,
				preparable: preparable,
				active:     make(map[string]bool),
				committed:  make(map[string]bool),
			}
			gotI := doUntilFirstFalse(inDesc, inChain, prepareSnapshots(context.TODO(), sn, testRef))
			wantI := doUntilFirstFalse(inDesc, inChain, func(l ocispec.Descriptor, chain []digest.Digest) bool {
				return tt.preparable[len(chain)-1]
			})
			if gotI != wantI {
				t.Errorf("upper-most preparable = %d; want %d", gotI, wantI)
				return
			}
			if len(sn.active) != 0 {
				t.Errorf("remaining garbage active snapshot. must be clean")
				return
			}
			if len(sn.committed) != wantI {
				t.Errorf("num of the committed snapshot: %d; want: %d", len(sn.committed), wantI)
				return
			}
			for i := 0; i < wantI; i++ {
				chainID := identity.ChainID(inChain[:i+1]).String()
				if ok := sn.committed[chainID]; !ok {
					t.Errorf("chainID %q must be committed", chainID)
					return
				}
			}
		})
	}
}

func descriptor(id int) ocispec.Descriptor {
	return ocispec.Descriptor{
		Digest: digest.Digest(fmt.Sprintf("%d-desc", id)),
	}
}

type testSnapshotter struct {
	t          *testing.T
	preparable map[string]bool
	active     map[string]bool
	committed  map[string]bool
}

func (ts *testSnapshotter) Stat(ctx context.Context, key string) (snapshots.Info, error) {
	if ok := ts.active[key]; ok {
		return snapshots.Info{}, nil
	}
	if ok := ts.committed[key]; ok {
		return snapshots.Info{}, nil
	}
	return snapshots.Info{}, errors.Wrapf(errdefs.ErrNotFound, "resource %q does not exist", key)
}

func (ts *testSnapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	if parent != "" {
		if ok := ts.committed[parent]; !ok {
			return nil, errors.Wrapf(errdefs.ErrNotFound, "parent snapshot %q does not exist", parent)
		}
	}

	if err := ts.createSnapshot(opts...); err != nil {
		ts.t.Logf("failed to create snapshot: %q", err)
		ts.active[key] = true
		return nil, nil
	}

	return nil, errors.Wrapf(errdefs.ErrAlreadyExists, "created snapshot")
}

func (ts *testSnapshotter) createSnapshot(opts ...snapshots.Opt) error {
	var base snapshots.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return err
		}
	}

	chainID, ok := base.Labels[TargetSnapshotLabel]
	if !ok {
		return fmt.Errorf("chainID is not passed")
	}
	ref, ok := base.Labels[TargetRefLabel]
	if !ok {
		return fmt.Errorf("image reference is not passed")
	}
	digest, ok := base.Labels[TargetDigestLabel]
	if !ok {
		return fmt.Errorf("image digest are not passed")
	}
	if ref != testRef {
		return fmt.Errorf("unknown ref")
	}
	if ok := ts.preparable[digest]; !ok {
		return fmt.Errorf("unknown digest")
	}
	ts.committed[chainID] = true

	return nil
}

func (ts *testSnapshotter) Remove(ctx context.Context, key string) error {
	delete(ts.active, key)
	delete(ts.committed, key)
	return nil
}

func (ts *testSnapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	return snapshots.Info{}, nil
}

func (ts *testSnapshotter) Usage(ctx context.Context, key string) (snapshots.Usage, error) {
	return snapshots.Usage{}, nil
}

func (ts *testSnapshotter) Mounts(ctx context.Context, key string) ([]mount.Mount, error) {
	return nil, nil
}

func (ts *testSnapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return nil, nil
}

func (ts *testSnapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return nil
}

func (ts *testSnapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, filters ...string) error {
	return nil
}

func (ts *testSnapshotter) Close() error {
	return nil
}
