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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/remotes"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	// TargetSnapshotLabel is a label to tell the backend snapshotter the
	// chainID of this snapshot. The backend snapshotter can use this
	// information to search for the contents of the snapshot and to commit
	// it. The client can use the committed snapshot with the chainID later.
	TargetSnapshotLabel = "containerd.io/snapshot.ref"

	// TargetRefLabel is a label to tell the backend snapshotter the basic
	// information(image reference) of the image layer. The backend
	// snapshotter can use this information to search for the contents of
	// the snapshot.
	TargetRefLabel = "containerd.io/snapshot/target.reference"

	// TargetDigestLabel is a label to tell the backend snapshotter the
	// basic information(layer digest) of the image layer. The backend
	// snapshotter can use this information to search for the contents of
	// the snapshot.
	TargetDigestLabel = "containerd.io/snapshot/target.digest"
)

// PrepareSnapshotterHandlerWrapper prepares snapshots using image's basic
// information. Snapshotters can prepare/commit them internally. These internal
// snapshots will be recognized and synchronized by unpacker.
func PrepareSnapshotsByImage(sn snapshots.Snapshotter, store content.Store, fetcher remotes.Fetcher, ref string) images.HandlerFunc {
	var (
		mu     sync.Mutex
		layers = map[digest.Digest][]ocispec.Descriptor{}
	)
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch desc.MediaType {
		case ocispec.MediaTypeImageManifest, images.MediaTypeDockerSchema2Manifest:
			p, err := content.ReadBlob(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			var manifest ocispec.Manifest
			if err := json.Unmarshal(p, &manifest); err != nil {
				return nil, err
			}
			mu.Lock()
			layers[manifest.Config.Digest] = manifest.Layers
			mu.Unlock()
		case images.MediaTypeDockerSchema2Config, ocispec.MediaTypeImageConfig:
			mu.Lock()
			l := layers[desc.Digest]
			mu.Unlock()
			chain, err := images.RootFS(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			doUntilFirstFalse(l, chain, prepareSnapshots(ctx, sn, ref))
		}

		return nil, nil
	}
}

func prepareSnapshots(ctx context.Context, sn snapshots.Snapshotter, ref string) func(ocispec.Descriptor, []digest.Digest) bool {

	// Attempts to prepare snapshots using image's basic information. Using chain,
	// this function generates chainID to prepare/commit the snapshot so that the
	// snapshot can be Prepare()ed later using the chainID as a normal way.
	return func(layer ocispec.Descriptor, chain []digest.Digest) bool {
		var (
			parent      = identity.ChainID(chain[:len(chain)-1]).String()
			chainID     = identity.ChainID(chain).String()
			layerDigest = layer.Digest.String()
		)

		// If this layer has been successfully prepared, the snapshotter gives us
		// ErrAlreadyExists.
		for {
			pKey := getUniqueKey(ctx, sn, chainID)
			if _, err := sn.Prepare(ctx, pKey, parent, snapshots.WithLabels(map[string]string{
				TargetSnapshotLabel: chainID,
				TargetRefLabel:      ref,
				TargetDigestLabel:   layerDigest,
			})); !errdefs.IsAlreadyExists(err) {
				sn.Remove(ctx, pKey)
				return false
			}
			if _, err := sn.Stat(ctx, pKey); err == nil {

				// We got ErrAlreadyExist and the ActiveSnapshot already exists. This
				// doesn't indicate that we prepared the snapshot but key confliction.
				// Try again with another key.
				continue
			}

			break
		}

		return true
	}
}

func getUniqueKey(ctx context.Context, sn snapshots.Snapshotter, chainID string) (key string) {
	for {
		t := time.Now()
		var b [3]byte
		// Ignore read failures, just decreases uniqueness
		rand.Read(b[:])
		uniquePart := fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding.EncodeToString(b[:]))
		key = fmt.Sprintf("target-%s %s", uniquePart, chainID)
		if _, err := sn.Stat(ctx, key); err == nil {
			continue
		}
		break
	}
	return
}

func doUntilFirstFalse(layers []ocispec.Descriptor, chain []digest.Digest, f func(ocispec.Descriptor, []digest.Digest) bool) int {
	for i, l := range layers {
		if !f(l, chain[:i+1]) {
			return i
		}
	}
	return len(layers)
}
