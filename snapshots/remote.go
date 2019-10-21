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

package snapshots

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/remotes"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const (
	RemoteSnapshotLabel = "containerd.io/snapshot.ref"
	RemoteRefLabel      = "containerd.io/snapshot/remote_snapshot/ref"
	RemoteDigestLabel   = "containerd.io/snapshot/remote_snapshot/digest"
)

// FilterLayerBySnapshotter filters out layers from download candidates if we
// can make a snapshot without downloading the actual contents of the layer.
func FilterLayerBySnapshotter(f images.HandlerFunc, sn Snapshotter, store content.Store, fetcher remotes.Fetcher, ref string) images.HandlerFunc {
	return func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := f(ctx, desc)
		if err != nil {
			return nil, err
		}

		if desc.MediaType == ocispec.MediaTypeImageManifest ||
			desc.MediaType == images.MediaTypeDockerSchema2Manifest {
			p, err := content.ReadBlob(ctx, store, desc)
			if err != nil {
				return nil, err
			}
			var manifest ocispec.Manifest
			if err := json.Unmarshal(p, &manifest); err != nil {
				return nil, err
			}

			configDesc := manifest.Config
			if _, err := remotes.FetchHandler(store, fetcher)(ctx, configDesc); err != nil {
				return nil, err
			}
			configLayers, err := images.RootFS(ctx, store, configDesc)
			if err != nil {
				return nil, err
			}

			necessary, _, _ := createRemoteChain(ctx, manifest.Layers, configLayers, sn, ref)
			unnecessary := exclude(manifest.Layers, necessary)
			children = exclude(children, unnecessary)
		}

		return children, nil
	}
}

// createRemoteChain checks if each layer and the lower layers are "remote
// layer" with which the remote snapshotter can make a snapshot without
// downloading the actual layer contents.
// If so, it filters out the layer from download candidates and make the
// snapshot(we call "remote snapshot" here) NOW to avoid unpacking the
// layer contents.
func createRemoteChain(ctx context.Context, layers []ocispec.Descriptor, diffIDs []digest.Digest, sn Snapshotter, ref string) ([]ocispec.Descriptor, string, bool) {
	if len(layers) <= 0 {
		return nil, "", true
	}
	chainID := identity.ChainID(diffIDs).String()

	// Make sure that all lower chains are remote snapshots.
	necessary, parentID, ok := createRemoteChain(ctx, layers[:len(layers)-1], diffIDs[:len(diffIDs)-1], sn, ref)
	if !ok {

		// Some of lower chains aren't remote snapshots.
		// We need to fetch all layers above.
		return append(necessary, layers[len(layers)-1]), chainID, false
	}

	// Prepare()ing a snapshot with a special label RemoteSnapshotLabel and
	// some basic information of the layer (ref and layer digest). Remote
	// snapshotters MUST recognise these labels and MUST check if the layer is
	// a remote layer. If the remote snapshot exists, remote snapshotter MUST
	// prepare and commit(internally) the remote snapshot WITH "RemoteSnapshotLabel"
	// then MUST return ErrAlreadyExists. Metadata snapshotter recognizes this label
	// and this error and can deal with this automatically-committed remote snapshot
	// correctly as a committed snapshot.
	remoteOpt := WithLabels(map[string]string{
		RemoteSnapshotLabel: chainID,
		RemoteRefLabel:      ref,
		RemoteDigestLabel:   layers[len(layers)-1].Digest.String(),
	})
	key := getUniqueKey(ctx, sn, chainID)
	if _, err := sn.Prepare(ctx, key, parentID, remoteOpt); errdefs.IsAlreadyExists(err) {

		// Check if the snapshot is a remote snapshot. If the snapshot has a RemoteSnapshotLabel,
		// this is a remote snapshot and we can skip downloading this layer.
		if info, err := sn.Stat(ctx, chainID); err == nil {
			if target, ok := info.Labels[RemoteSnapshotLabel]; ok && target == chainID {
				return necessary, chainID, true
			}
		}
	}

	// Failed to prepare the remote snapshotter. So we treat this layer as a normal way.
	sn.Remove(ctx, key)
	return append(necessary, layers[len(layers)-1]), chainID, false
}

func getUniqueKey(ctx context.Context, sn Snapshotter, chainID string) (key string) {
	for {
		t := time.Now()
		var b [3]byte
		// Ignore read failures, just decreases uniqueness
		rand.Read(b[:])
		uniquePart := fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding.EncodeToString(b[:]))
		key = fmt.Sprintf("remote-%s %s", uniquePart, chainID)
		if _, err := sn.Stat(ctx, key); err == nil {
			continue
		}
		break
	}
	return
}

func exclude(a []ocispec.Descriptor, b []ocispec.Descriptor) []ocispec.Descriptor {
	amap := map[string]ocispec.Descriptor{}
	for _, va := range a {
		amap[va.Digest.String()] = va
	}
	for _, vb := range b {
		delete(amap, vb.Digest.String())
	}
	var res []ocispec.Descriptor
	for _, va := range amap {
		res = append(res, va)
	}
	return res
}
